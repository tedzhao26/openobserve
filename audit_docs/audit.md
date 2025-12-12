# OpenObserve Trace Ingestion Audit

## Executive Summary

This audit examines the trace ingestion pipeline in OpenObserve, identifying architectural strengths, potential bottlenecks, and comparing the approach against Elasticsearch's traditional ingestion patterns. OpenObserve's architecture provides significant advantages in storage cost (claimed 140x reduction) and ingestion throughput through its columnar storage approach and modern Rust-based implementation.

---

## Table of Contents

1. [Trace Ingestion Architecture](#trace-ingestion-architecture)
2. [Physical Data Swap Architecture](#physical-data-swap-architecture)
3. [S3 Capacity Scaling Architecture](#s3-capacity-scaling-architecture)
4. [Data Flow Analysis](#data-flow-analysis)
5. [Bottleneck Analysis](#bottleneck-analysis)
6. [Caveats and Limitations](#caveats-and-limitations)
7. [Comparison with Elasticsearch](#comparison-with-elasticsearch)
8. [Configuration Tuning Guide](#configuration-tuning-guide)
9. [Recommendations](#recommendations)

---

## Trace Ingestion Architecture

### Overview

OpenObserve's trace ingestion follows a multi-stage pipeline:

```mermaid
flowchart LR
    subgraph Input["🌐 Input Layer"]
        HTTP["HTTP/gRPC Request"]
        Proto["Protobuf"]
        JSON["JSON"]
    end
    
    subgraph Processing["⚙️ Processing Layer"]
        Parse["Protocol Parsing"]
        Validate["Validation"]
        Pipeline["Pipeline Processing"]
    end
    
    subgraph Storage["💾 Storage Layer"]
        WAL["WAL Write"]
        Mem["Memtable"]
        Immut["Immutable"]
        Parquet["Parquet Files"]
        ObjStore["Object Storage\n(S3/GCS/Azure)"]
    end
    
    HTTP --> Proto & JSON
    Proto & JSON --> Parse
    Parse --> Validate
    Validate --> Pipeline
    Pipeline --> WAL
    WAL --> Mem
    Mem --> Immut
    Immut --> Parquet
    Parquet --> ObjStore
    
    style Input fill:#e1f5fe
    style Processing fill:#fff3e0
    style Storage fill:#e8f5e9
```

### Key Components

#### 1. HTTP Handlers (`src/handler/http/request/traces/mod.rs`)

- **Endpoints**: `POST /{org_id}/traces` and `POST /{org_id}/v1/traces` (OTLP)
- **Protocols**: Supports both Protobuf (`application/x-protobuf`) and JSON (`application/json`)
- **Rate limiting**: Cloud feature with `check_ingestion_allowed()` pre-flight check

#### 2. Trace Service (`src/service/traces/mod.rs`)

- **OTLP Processing**: Parses `ExportTraceServiceRequest` from OpenTelemetry format
- **Span validation**: Validates trace_id (16 bytes) and span_id (8 bytes) lengths
- **Timestamp validation**: Rejects spans outside configurable retention windows
- **Pipeline integration**: Supports pre-processing via configurable pipelines
- **Flattening**: JSON data is flattened for columnar storage

#### 3. Ingester Module (`src/ingester/`)

```mermaid
flowchart TB
    subgraph Ingester["Ingester Module"]
        direction TB
        
        subgraph Writer["Writer Component"]
            WQ["Write Queue\n(async channel)"]
            WL["WAL Lock\n(per bucket)"]
        end
        
        subgraph WAL["WAL Component"]
            WF["WAL Files\n(append-only)"]
            WB["Write Buffer\n(configurable size)"]
        end
        
        subgraph Memory["Memory Components"]
            MT["Memtable\n(Arrow RecordBatch)"]
            IM["Immutable\n(frozen batches)"]
        end
        
        subgraph Persist["Persistence"]
            PQ["Parquet Writer"]
            UP["Upload Worker"]
        end
        
        WQ --> WL
        WL --> WB
        WB --> WF
        WL --> MT
        MT -->|"size/time\nlimit"| IM
        IM --> PQ
        PQ --> UP
    end
    
    style Writer fill:#e3f2fd
    style WAL fill:#fce4ec
    style Memory fill:#e8f5e9
    style Persist fill:#fff3e0
```

- **Writer**: Manages WAL files and memtables per stream bucket
- **WAL (Write-Ahead Log)**: Ensures durability before acknowledgment
- **Memtable**: In-memory buffer using Arrow RecordBatch format
- **Immutable**: Frozen memtables awaiting persistence

#### 4. Storage Layer (`src/infra/src/storage/`)

- **Parquet format**: Columnar storage with high compression
- **Multi-backend**: Local disk, S3, MinIO, GCS, Azure Blob
- **Caching**: Disk and memory caches for query acceleration

---

## Physical Data Swap Architecture

### Memory → Disk → Object Storage Data Flow

```mermaid
flowchart TB
    subgraph Ingestion["📥 Hot Path (Synchronous)"]
        direction LR
        REQ["Incoming\nRequest"]
        WAL_BUF["WAL Buffer\n(8KB-64KB)"]
        WAL_FILE["WAL File\n(Local Disk)"]
        MEM["Memtable\n(Arrow RecordBatch)"]
        
        REQ -->|"1. Append"| WAL_BUF
        WAL_BUF -->|"2. Flush/fsync"| WAL_FILE
        REQ -->|"3. Write"| MEM
    end
    
    subgraph Background["⚙️ Background Path (Async)"]
        direction TB
        IMM["Immutable\nMemtable"]
        PARQ_LOCAL["Local Parquet\n(/data/wal/files/)"]
        MERGE["File Merger\n(Multiple threads)"]
        
        MEM -->|"4. Freeze\n(size/time)"| IMM
        IMM -->|"5. Convert\nto Parquet"| PARQ_LOCAL
        PARQ_LOCAL -->|"6. Batch &\nMerge"| MERGE
    end
    
    subgraph Remote["☁️ Object Storage (S3/GCS/Azure)"]
        direction TB
        S3_PUT["PUT Object\n(multipart if >5MB)"]
        S3_BUCKET["Object Storage\nBucket"]
        META_DB["File List DB\n(PostgreSQL/MySQL)"]
        
        MERGE -->|"7. Upload"| S3_PUT
        S3_PUT -->|"8. Store"| S3_BUCKET
        S3_PUT -->|"9. Record\nmetadata"| META_DB
    end
    
    subgraph Cleanup["🧹 Cleanup"]
        WAL_DEL["Delete WAL"]
        LOCAL_DEL["Delete Local\nParquet"]
        
        META_DB -->|"10. On success"| WAL_DEL
        META_DB -->|"10. On success"| LOCAL_DEL
    end
    
    style Ingestion fill:#e8f5e9
    style Background fill:#fff3e0
    style Remote fill:#e3f2fd
    style Cleanup fill:#ffebee
```

### Physical Storage Tiers

```mermaid
flowchart LR
    subgraph Tier1["🔥 Tier 1: Memory (Fastest)"]
        direction TB
        T1_WAL["WAL Buffer\n~64KB per bucket"]
        T1_MEM["Memtable\nArrow RecordBatch\n(configurable max)"]
        T1_CACHE["Memory Cache\n(LRU eviction)"]
    end
    
    subgraph Tier2["💾 Tier 2: Local Disk (Fast)"]
        direction TB
        T2_WAL["WAL Files\n/data/wal/logs/"]
        T2_PARQ["Local Parquet\n/data/wal/files/"]
        T2_CACHE["Disk Cache\n(FIFO/LRU/TimeLRU)"]
    end
    
    subgraph Tier3["☁️ Tier 3: Object Storage (Infinite)"]
        direction TB
        T3_S3["S3/GCS/Azure\nfiles/{org}/{type}/{stream}/"]
        T3_META["Metadata DB\n(file_list table)"]
    end
    
    T1_WAL -->|"fsync"| T2_WAL
    T1_MEM -->|"freeze & convert"| T2_PARQ
    T2_PARQ -->|"upload & delete local"| T3_S3
    T2_WAL -->|"cleanup after upload"| T2_WAL
    
    T3_S3 -.->|"cache on read"| T2_CACHE
    T2_CACHE -.->|"promote hot data"| T1_CACHE
    
    style Tier1 fill:#ffebee
    style Tier2 fill:#fff3e0
    style Tier3 fill:#e3f2fd
```

### File Path Convention

```
Object Storage Layout:
files/{org_id}/{stream_type}/{stream_name}/{year}/{month}/{day}/{hour}/{unique_id}.parquet

Example:
files/default/traces/default/2024/12/12/14/7f3a2b1c9d8e4f5a.parquet
     │         │       │       │    │   │   │   └── Unique file ID
     │         │       │       │    │   │   └── Hour partition (00-23)
     │         │       │       │    │   └── Day partition
     │         │       │       │    └── Month partition
     │         │       │       └── Year partition
     │         │       └── Stream name
     │         └── Stream type (logs/traces/metrics)
     └── Organization ID
```

---

## S3 Capacity Scaling Architecture

### Horizontal Scaling Model

```mermaid
flowchart TB
    subgraph Ingesters["📥 Ingester Nodes (Stateless)"]
        I1["Ingester 1\n(WAL + Memtable)"]
        I2["Ingester 2\n(WAL + Memtable)"]
        I3["Ingester N\n(WAL + Memtable)"]
    end
    
    subgraph Queriers["🔍 Querier Nodes (Stateless)"]
        Q1["Querier 1\n(Disk Cache)"]
        Q2["Querier 2\n(Disk Cache)"]
        Q3["Querier N\n(Disk Cache)"]
    end
    
    subgraph Compactors["🔧 Compactor Nodes"]
        C1["Compactor 1"]
        C2["Compactor N"]
    end
    
    subgraph SharedStorage["☁️ Shared Object Storage (Infinite Scale)"]
        S3["S3 / GCS / Azure Blob\n• No capacity limits\n• Pay per GB stored\n• 11 nines durability"]
    end
    
    subgraph MetaDB["📊 Metadata Store"]
        DB["PostgreSQL / MySQL\n(file_list table)"]
    end
    
    I1 & I2 & I3 -->|"Upload Parquet"| S3
    I1 & I2 & I3 -->|"Register files"| DB
    
    Q1 & Q2 & Q3 <-->|"Read Parquet"| S3
    Q1 & Q2 & Q3 <-->|"Query file list"| DB
    
    C1 & C2 -->|"Read small files"| S3
    C1 & C2 -->|"Write merged files"| S3
    C1 & C2 <-->|"Update file list"| DB
    
    style Ingesters fill:#e8f5e9
    style Queriers fill:#e3f2fd
    style Compactors fill:#fff3e0
    style SharedStorage fill:#f3e5f5
    style MetaDB fill:#fce4ec
```

### Why S3 Enables Infinite Scaling

```mermaid
flowchart LR
    subgraph Traditional["❌ Traditional (Elasticsearch)"]
        direction TB
        ES_N1["Node 1\n💾 Local Storage\n1TB limit"]
        ES_N2["Node 2\n💾 Local Storage\n1TB limit"]
        ES_N3["Node 3\n💾 Local Storage\n1TB limit"]
        ES_REP["Replicas needed\nfor durability"]
        
        ES_N1 <--> ES_N2 <--> ES_N3
        ES_N1 & ES_N2 & ES_N3 --> ES_REP
    end
    
    subgraph Modern["✅ OpenObserve (S3-Native)"]
        direction TB
        OO_N1["Ingester 1\n🔥 Hot data only\n~10GB local"]
        OO_N2["Ingester 2\n🔥 Hot data only\n~10GB local"]
        OO_S3["S3 Bucket\n☁️ Unlimited\n• Auto scales\n• Built-in redundancy\n• ~$0.023/GB/month"]
        
        OO_N1 --> OO_S3
        OO_N2 --> OO_S3
    end
    
    style Traditional fill:#ffebee
    style Modern fill:#e8f5e9
```

### Data Compaction Flow

```mermaid
sequenceDiagram
    participant Scheduler as Compaction Scheduler
    participant Worker as Compactor Worker
    participant DB as File List DB
    participant S3 as Object Storage
    participant Cache as Local Cache

    Note over Scheduler: Runs every 10 seconds
    
    Scheduler->>DB: Query files older than 1 hour
    DB-->>Scheduler: List of small files per partition
    
    loop For each partition with multiple small files
        Scheduler->>Worker: Submit merge job
        
        Worker->>S3: Download small parquet files
        S3-->>Worker: File contents
        
        Worker->>Worker: Read with DataFusion
        Worker->>Worker: Sort by _timestamp
        Worker->>Worker: Apply bloom filters
        Worker->>Worker: Write merged parquet
        
        Note over Worker: Target size: 256MB (configurable)
        
        Worker->>S3: Upload merged file
        S3-->>Worker: Success
        
        Worker->>DB: Transaction: Add new file, mark old as deleted
        DB-->>Worker: Committed
        
        Worker->>S3: Delete old small files
        Worker->>Cache: Invalidate cached files
    end
```

### Multi-Account S3 Support

```mermaid
flowchart TB
    subgraph Accounts["🗂️ Multiple S3 Accounts"]
        A1["Account: default\nBucket: prod-data\nRegion: us-east-1"]
        A2["Account: archive\nBucket: archive-data\nRegion: us-west-2"]
        A3["Account: hot\nBucket: hot-data\nRegion: eu-west-1"]
    end
    
    subgraph Router["🔀 Storage Router"]
        R["Account Selection\nbased on file path"]
    end
    
    subgraph FileTypes["📁 File Routing"]
        F1["Recent data\n(< 7 days)"] -->|"hot account"| A3
        F2["Standard data\n(7-90 days)"] -->|"default account"| A1
        F3["Archive data\n(> 90 days)"] -->|"archive account"| A2
    end
    
    Router --> Accounts
    
    style Accounts fill:#e3f2fd
    style Router fill:#fff3e0
    style FileTypes fill:#e8f5e9
```

### Cache Hierarchy for Query Performance

```mermaid
flowchart TB
    subgraph Query["🔍 Query Request"]
        REQ["SELECT * FROM traces\nWHERE service='api'"]
    end
    
    subgraph CacheCheck["Cache Lookup (Fast → Slow)"]
        direction TB
        
        MC["1️⃣ Memory Cache\n• LRU eviction\n• ~μs latency\n• Size: ZO_MEMORY_CACHE_SIZE"]
        
        DC["2️⃣ Disk Cache\n• FIFO/LRU/TimeLRU\n• ~ms latency\n• Size: ZO_DISK_CACHE_SIZE"]
        
        S3["3️⃣ Object Storage\n• ~100ms latency\n• Unlimited"]
        
        MC -->|"Miss"| DC
        DC -->|"Miss"| S3
    end
    
    subgraph Populate["Cache Population"]
        POP["Download & Cache\n(async background)"]
    end
    
    Query --> MC
    S3 -->|"On read"| POP
    POP -->|"Store for future"| DC
    POP -.->|"Hot data"| MC
    
    style Query fill:#fff3e0
    style CacheCheck fill:#e3f2fd
    style Populate fill:#e8f5e9
```

### Storage Cost Comparison

```mermaid
pie title Storage Cost Distribution (100TB data, 30 days)
    "Elasticsearch (3x replication, SSD)" : 45000
    "OpenObserve (S3 Standard)" : 2300
    "OpenObserve (S3 IA)" : 1250
    "OpenObserve (S3 Glacier)" : 400
```

| Storage Option | Cost/TB/Month | 100TB/30 days | Notes |
|----------------|---------------|---------------|-------|
| Elasticsearch (SSD, 3x) | ~$150 | ~$45,000 | Requires 3x for durability |
| OpenObserve + S3 Standard | ~$23 | ~$2,300 | Built-in 11-nines durability |
| OpenObserve + S3 IA | ~$12.50 | ~$1,250 | Infrequent access tier |
| OpenObserve + S3 Glacier | ~$4 | ~$400 | Archive tier |

---

## Data Flow Analysis

### Ingestion Flow Detail

```mermaid
sequenceDiagram
    participant Client
    participant Handler as HTTP/gRPC Handler
    participant Service as Trace Service
    participant Pipeline as Pipeline Processor
    participant Writer as Ingester Writer
    participant WAL
    participant Memtable
    participant Background as Background Worker
    participant Storage as Object Storage

    Client->>Handler: POST /traces (Proto/JSON)
    Handler->>Handler: Parse Content-Type
    Handler->>Service: Decode ExportTraceServiceRequest
    
    Note over Service: Cloud checks (if enabled)
    
    loop For each ResourceSpan
        Service->>Service: Extract service attributes
        loop For each Span
            Service->>Service: Validate trace_id (16 bytes)
            Service->>Service: Validate span_id (8 bytes)
            Service->>Service: Validate timestamp bounds
            Service->>Pipeline: Apply transformations
            Pipeline-->>Service: Transformed span
            Service->>Service: Flatten JSON structure
        end
    end
    
    Service->>Writer: Write batch
    Writer->>WAL: Persist to WAL
    Writer->>Memtable: Write to memory
    Writer-->>Service: Acknowledgment
    Service-->>Handler: Success response
    Handler-->>Client: HTTP 200 OK
    
    Note over Background: Async processing
    Background->>Memtable: Check size/time limits
    Background->>Background: Freeze to Immutable
    Background->>Background: Convert to Parquet
    Background->>Storage: Upload to S3/GCS/Azure
    Background->>WAL: Cleanup processed entries
```

### Detailed Flow Steps

```
1. Request arrives → Content-Type parsing
2. Proto/JSON decode → ExportTraceServiceRequest
3. Cloud checks (if enabled) → Trial period, quotas
4. Resource spans iteration:
   - Extract service attributes
   - Process each span:
     - Validate trace_id/span_id length
     - Validate timestamp bounds
     - Build Span struct with events/links
     - Apply pipeline transformations (if configured)
     - Flatten JSON structure
5. Write to WAL + Memtable simultaneously
6. Return success response
7. Background: Memtable → Immutable → Parquet → Object Storage
```

### Critical Path Timing

| Stage | Blocking? | Typical Latency |
|-------|-----------|-----------------|
| Protocol decode | Yes | <1ms |
| Span validation | Yes | <1ms per span |
| Pipeline processing | Yes | Variable (VRL execution) |
| WAL write | Yes | 1-10ms (depends on fsync) |
| Memtable write | Yes | <1ms |
| Parquet persistence | No | Background (seconds) |

---

## Bottleneck Analysis

### Bottleneck Overview

```mermaid
flowchart LR
    subgraph Critical["🔴 Critical Path Bottlenecks"]
        B1["WAL Write\nContention"]
        B2["Memtable\nSize Limits"]
        B3["Schema\nEvolution"]
    end
    
    subgraph Resource["🟡 Resource Bottlenecks"]
        B4["Memory\nCircuit Breaker"]
        B5["Disk\nCircuit Breaker"]
    end
    
    subgraph Processing["🟠 Processing Bottlenecks"]
        B6["Pipeline\nProcessing"]
        B7["Real-time\nAlerts"]
    end
    
    Input["Incoming\nTraces"] --> B1
    B1 --> B2
    B2 --> B3
    B3 --> Output["Storage"]
    
    B4 -.->|"Rejects\nWrites"| B1
    B5 -.->|"Rejects\nWrites"| B1
    B6 -.->|"Adds\nLatency"| B3
    B7 -.->|"Adds\nLatency"| B3
    
    style Critical fill:#ffebee
    style Resource fill:#fff8e1
    style Processing fill:#fff3e0
```

### 1. WAL Write Contention

**Location**: `src/ingester/src/writer.rs`

**Issue**: WAL writes are serialized through an async queue system.

```rust
// Line 310: Queue-based write system
let (tx, rx) = mpsc::channel(cfg.limit.wal_write_queue_size);
```

**Impact**: 
- Under high load, the queue can fill up (`wal_write_queue_size` default: 10,000)
- When `wal_write_queue_full_reject` is enabled, writes are rejected
- WAL lock contention metrics tracked via `INGEST_WAL_LOCK_TIME`

**Mitigation**:
- Increase `ZO_WAL_WRITE_QUEUE_SIZE` for higher throughput
- Enable dedicated WAL runtime (`ZO_WAL_DEDICATED_RUNTIME_ENABLED`)
- Consider `ZO_WAL_FSYNC_DISABLED=true` for higher throughput (at durability risk)

### 2. Memtable Size Limits

**Location**: `src/ingester/src/writer.rs:87-95`

**Issue**: Global memtable size limit can reject ingestion.

```rust
pub fn check_memtable_size() -> Result<()> {
    let cur_mem = metrics::INGEST_MEMTABLE_ARROW_BYTES...
    if cur_mem >= get_config().limit.mem_table_max_size as i64 {
        Err(Error::MemoryTableOverflowError {})
    }
}
```

**Impact**: When memtable reaches `ZO_MEM_TABLE_MAX_SIZE`, all ingestion fails.

**Mitigation**:
- Increase `ZO_MEM_TABLE_MAX_SIZE` (consider available RAM)
- Reduce `ZO_MAX_FILE_RETENTION_TIME` for faster rotation
- Increase `ZO_MEM_DUMP_THREAD_NUM` for faster persistence

### 3. Memory Circuit Breaker

**Location**: `src/ingester/src/writer.rs:99-112`

**Issue**: System-wide memory usage triggers ingestion rejection.

**Impact**: High query load can starve ingestion.

**Configuration**:
- `ZO_MEMORY_CIRCUIT_BREAKER_ENABLED` (default: varies)
- `ZO_MEMORY_CIRCUIT_BREAKER_RATIO` (percentage threshold)

### 4. Disk Circuit Breaker

**Location**: `src/ingester/src/writer.rs:119-148`

**Issue**: Low disk space triggers ingestion rejection.

**Configuration**:
- `ZO_DISK_CIRCUIT_BREAKER_ENABLED`
- `ZO_DISK_CIRCUIT_BREAKER_THRESHOLD` (<100 = percentage, ≥100 = MB remaining)

### 5. Schema Evolution Overhead

**Location**: `src/service/traces/mod.rs:887-908`

**Issue**: Each batch triggers schema check/evolution.

```rust
let (_schema_evolution, _infer_schema) = check_for_schema(
    org_id,
    stream_name,
    StreamType::Traces,
    &mut traces_schema_map,
    json_data.iter().map(|(_, v)| v).collect(),
    *min_timestamp,
    false,
).await
```

**Impact**: 
- Dynamic schema adds latency
- High cardinality spans can cause schema explosion
- `ZO_SCHEMA_MAX_FIELDS_TO_ENABLE_UDS` (default: 1000) triggers user-defined schema mode

### 6. Pipeline Processing Latency

**Location**: `src/service/traces/mod.rs:447-526`

**Issue**: Configured pipelines execute synchronously during ingestion.

**Impact**: Complex VRL functions add per-record latency.

**Mitigation**: Keep pipeline functions simple, avoid expensive operations.

### 7. Real-time Alert Evaluation

**Location**: `src/service/traces/mod.rs:954-982`

**Issue**: Alerts are evaluated inline during ingestion.

**Impact**: Multiple real-time alerts per stream multiply latency.

---

## Caveats and Limitations

### Data Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> Incoming: Trace Request
    Incoming --> Validated: Pass validation
    Incoming --> Rejected: Invalid trace_id/span_id
    Incoming --> Rejected: Timestamp out of bounds
    
    Validated --> WAL: Write to disk
    WAL --> Memtable: Buffer in memory
    
    Memtable --> Memtable: Accumulate spans
    Memtable --> Immutable: Size/time limit reached
    
    Immutable --> Parquet: Convert format
    Parquet --> ObjectStorage: Upload complete
    
    ObjectStorage --> [*]: Query ready
    
    Rejected --> [*]: Error response
    
    note right of Memtable
        Triggers freeze when:
        - Size > ZO_MAX_FILE_SIZE_IN_MEMORY
        - Time > ZO_MAX_FILE_RETENTION_TIME
    end note
    
    note right of ObjectStorage
        Supports:
        - S3
        - GCS
        - Azure Blob
        - Local disk
    end note
```

### 1. Timestamp Restrictions

- **Past limit**: `ZO_INGEST_ALLOWED_UPTO` hours (default: 5 hours)
- **Future limit**: `ZO_INGEST_ALLOWED_IN_FUTURE` hours (default: 24 hours)
- Spans outside these windows are rejected with partial success response

### 2. Span Validation

- Trace IDs must be exactly 16 bytes
- Span IDs must be exactly 8 bytes
- Invalid IDs result in span rejection, not request failure

### 3. Service Graph Processing

- Service graph data is processed by a periodic daemon
- Not inline during trace ingestion (good for latency)
- May have slight delay in topology updates

### 4. Blocking Field Names

Certain field names are blocked and get prefixed:
- `_timestamp` → `attr__timestamp`
- `duration` → `attr_duration`
- `start_time` → `attr_start_time`
- `end_time` → `attr_end_time`

### 5. Stream Deletion Lock

Ingestion fails if stream is being deleted:
```rust
if db::compact::retention::is_deleting_stream(org_id, stream_type, stream_name, None) {
    return Err(Error::IngestionError("stream is being deleted"))
}
```

### 6. Single-Node Ingestion Constraint

Ingestion only works on nodes with `ingester` role:
```rust
if !LOCAL_NODE.is_ingester() {
    return Err(Error::IngestionError("not an ingester"))
}
```

---

## Comparison with Elasticsearch

### Architecture Comparison

```mermaid
flowchart TB
    subgraph OO["OpenObserve Architecture"]
        direction TB
        OO_In["HTTP/gRPC Input"]
        OO_WAL["WAL\n(Write-Ahead Log)"]
        OO_Mem["Arrow Memtable\n(Columnar)"]
        OO_Parq["Parquet Files\n(Columnar + ZSTD)"]
        OO_S3["Object Storage\n(S3/GCS/Azure)"]
        
        OO_In --> OO_WAL
        OO_WAL --> OO_Mem
        OO_Mem --> OO_Parq
        OO_Parq --> OO_S3
    end
    
    subgraph ES["Elasticsearch Architecture"]
        direction TB
        ES_In["HTTP Input"]
        ES_Trans["Translog\n(Write-Ahead Log)"]
        ES_Buf["JVM Buffer\n(Row-based)"]
        ES_Seg["Lucene Segments\n(Inverted Index)"]
        ES_Disk["Local Disk\n(Replicated)"]
        
        ES_In --> ES_Trans
        ES_Trans --> ES_Buf
        ES_Buf --> ES_Seg
        ES_Seg --> ES_Disk
    end
    
    style OO fill:#e8f5e9
    style ES fill:#fff3e0
```

### Storage Architecture

| Aspect | OpenObserve | Elasticsearch |
|--------|-------------|---------------|
| **Format** | Parquet (columnar) | Lucene (row-based) |
| **Compression** | High (columnar + ZSTD) | Moderate (LZ4/deflate) |
| **Storage Cost** | ~140x lower (claimed) | Baseline |
| **Index Structure** | No inverted index by default | Full inverted index |

### Ingestion Model

| Aspect | OpenObserve | Elasticsearch |
|--------|-------------|---------------|
| **Write Path** | WAL → Memtable → Parquet | Translog → Segment |
| **Durability** | WAL with configurable fsync | Translog (always fsync) |
| **Refresh Model** | Background persist | Near-real-time refresh |
| **Buffer Management** | In-memory Arrow batches | JVM heap segments |

### Key Advantages of OpenObserve

1. **No JVM Overhead**: Rust implementation avoids GC pauses
2. **Columnar Compression**: Significantly better for sparse trace attributes
3. **S3-Native**: Cheap long-term storage, no shard management
4. **Simpler Operations**: No cluster state, replica management
5. **Lower Memory Footprint**: Arrow-based in-memory format

### Key Advantages of Elasticsearch

1. **Mature Full-Text Search**: Superior text analysis
2. **Near-Real-Time**: Faster search visibility after ingestion
3. **Distributed Coordination**: Built-in cluster consensus
4. **Richer Aggregations**: More aggregation types for analytics

### Ingestion Throughput Comparison

| Factor | OpenObserve | Elasticsearch |
|--------|-------------|---------------|
| **Bulk API** | Native batch processing | Bulk API with coordination overhead |
| **Index Creation** | Dynamic, low cost | Index creation is expensive |
| **Schema Changes** | Flexible, append-only | Requires reindex for mapping changes |
| **Backpressure** | Circuit breakers | Thread pool queues |

---

## Configuration Tuning Guide

### High-Throughput Ingestion

```bash
# Increase WAL queue for burst handling
ZO_WAL_WRITE_QUEUE_SIZE=50000

# Larger memtable for batching efficiency
ZO_MEM_TABLE_MAX_SIZE=4096  # 4GB

# More dump threads for faster persistence
ZO_MEM_DUMP_THREAD_NUM=8

# Faster file rotation (MB)
ZO_MAX_FILE_SIZE_IN_MEMORY=512

# Dedicated WAL runtime (isolates IO)
ZO_WAL_DEDICATED_RUNTIME_ENABLED=true

# Consider disabling fsync for highest throughput (risk: data loss on crash)
# ZO_WAL_FSYNC_DISABLED=true
```

### Low-Latency Ingestion

```bash
# Smaller batches for faster acknowledgment
ZO_MAX_FILE_SIZE_ON_DISK=64

# More buckets to reduce contention
ZO_MEM_TABLE_BUCKET_NUM=16

# Smaller WAL buffer
ZO_WAL_WRITE_BUFFER_SIZE=8192

# Lower retention for faster rotation
ZO_MAX_FILE_RETENTION_TIME=300
```

### Resource-Constrained Environment

```bash
# Conservative memtable size
ZO_MEM_TABLE_MAX_SIZE=512

# Enable circuit breakers
ZO_MEMORY_CIRCUIT_BREAKER_ENABLED=true
ZO_MEMORY_CIRCUIT_BREAKER_RATIO=80
ZO_DISK_CIRCUIT_BREAKER_ENABLED=true
ZO_DISK_CIRCUIT_BREAKER_THRESHOLD=1024  # 1GB free

# Fewer threads
ZO_MEM_DUMP_THREAD_NUM=2
ZO_FILE_MOVE_THREAD_NUM=2
```

---

## Recommendations

### Short-Term Improvements

1. **Batch Coalescing**: Consider coalescing small batches before WAL write
2. **Adaptive Backpressure**: Implement gradual throttling instead of hard rejection
3. **Schema Caching**: Cache inferred schemas per stream to reduce overhead
4. **Async Alert Evaluation**: Move real-time alerts to separate worker pool

### Medium-Term Improvements

1. **Tiered Memtables**: Hot/cold memtable tiers for different access patterns
2. **Write-Behind WAL**: Option for eventual consistency with higher throughput
3. **Span Sampling**: Configurable sampling for high-volume traces
4. **Adaptive Compression**: Use different compression levels based on data patterns

### Monitoring Recommendations

```mermaid
flowchart TB
    subgraph Metrics["📊 Key Metrics to Monitor"]
        direction TB
        
        subgraph Throughput["Throughput Metrics"]
            T1["INGEST_MEMTABLE_ARROW_BYTES"]
            T2["INGEST_MEMTABLE_BYTES"]
            T3["INGEST_MEMTABLE_FILES"]
        end
        
        subgraph Latency["Latency Indicators"]
            L1["INGEST_WAL_LOCK_TIME"]
            L2["INGEST_MEMTABLE_LOCK_TIME"]
        end
        
        subgraph Saturation["Saturation Signals"]
            S1["INGEST_PARQUET_FILES\n(pending upload)"]
            S2["WAL write queue length"]
        end
        
        subgraph Errors["Error Indicators"]
            E1["INGEST_ERRORS\n(with labels)"]
            E2["Circuit breaker triggers"]
        end
    end
    
    subgraph Alerts["🚨 Alert Thresholds"]
        A1["Memtable > 80% limit"]
        A2["WAL queue > 8000"]
        A3["Lock time > 100ms"]
        A4["Error rate > 1%"]
    end
    
    Throughput --> A1
    Saturation --> A2
    Latency --> A3
    Errors --> A4
    
    style Throughput fill:#e8f5e9
    style Latency fill:#fff3e0
    style Saturation fill:#fff8e1
    style Errors fill:#ffebee
    style Alerts fill:#e3f2fd
```

Track these metrics for ingestion health:

```
# Ingestion throughput
INGEST_MEMTABLE_ARROW_BYTES
INGEST_MEMTABLE_BYTES
INGEST_MEMTABLE_FILES

# Latency indicators
INGEST_WAL_LOCK_TIME
INGEST_MEMTABLE_LOCK_TIME

# Saturation
INGEST_PARQUET_FILES (pending upload)
WAL write queue length

# Errors
INGEST_ERRORS (with labels)
Circuit breaker triggers
```

---

## Conclusion

OpenObserve's trace ingestion pipeline is well-architected for modern observability workloads. The Rust implementation, columnar storage, and S3-native design provide significant advantages over traditional solutions like Elasticsearch.

**Key Strengths**:
- Exceptional storage efficiency (Parquet + compression)
- No JVM overhead or GC pauses
- Simple operational model
- Flexible schema handling

**Areas for Attention**:
- WAL contention under extreme load
- Circuit breaker behavior during memory pressure
- Real-time alert overhead in hot paths
- Timestamp validation window configuration

The architecture successfully trades some Elasticsearch features (full-text search richness, near-real-time visibility) for dramatic cost reduction and operational simplicity, making it an excellent choice for high-volume trace storage and analysis.

---

*Audit conducted: December 2024*
*OpenObserve Version: Based on source code analysis*

