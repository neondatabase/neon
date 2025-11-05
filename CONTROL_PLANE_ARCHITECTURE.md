# Neon Control Plane Architecture - Comprehensive Guide

## Executive Summary

The Neon control plane manages the orchestration of all storage components in a Neon database cluster. The repository contains **two implementations**:

1. **`neon_local` (Test/Dev Control Plane)** - Located in `/control_plane/`
   - Simplified, single-machine orchestrator for local development
   - In-memory state, no database backend
   - All processes run locally as child processes
   - Purpose: Fast local testing and development

2. **Storage Controller (Production Control Plane)** - Located in `/storage_controller/`
   - Full-featured distributed orchestration system
   - PostgreSQL backend for durable state
   - Manages cluster of pageserver and safekeeper nodes
   - Production-grade HA, reconciliation, and scheduling

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONTROL PLANE LAYER                          │
│                                                                 │
│  ┌─────────────────────┐         ┌────────────────────────┐   │
│  │   neon_local        │         │  Storage Controller    │   │
│  │  (Test/Dev Only)    │         │  (Production)          │   │
│  │                     │         │                        │   │
│  │  - CLI commands     │         │  - HTTP APIs           │   │
│  │  - Local env setup  │         │  - Scheduling          │   │
│  │  - Process mgmt     │         │  - Reconciliation      │   │
│  │  - In-memory state  │         │  - PostgreSQL backend  │   │
│  └──────────┬──────────┘         └─────────┬──────────────┘   │
│             │                              │                   │
└─────────────┼──────────────────────────────┼───────────────────┘
              │                              │
              │ Manages local processes      │ Manages distributed cluster
              │                              │
    ┌─────────┴────────┐          ┌─────────┴──────────────────┐
    │                  │          │                            │
    ▼                  ▼          ▼                            ▼
┌─────────┐    ┌────────────┐  ┌──────────────┐      ┌──────────────┐
│Pageserver│   │ Safekeeper │  │ Pageserver   │      │ Safekeeper   │
│(local)   │   │ (local)    │  │ Node Pool    │      │ Node Pool    │
└─────────┘    └────────────┘  │ (distributed)│      │ (distributed)│
                               └──────┬───────┘      └──────┬───────┘
                                      │                     │
                                      ▼                     ▼
                                  ┌──────────────────────────────┐
                                  │   Remote Storage (S3)        │
                                  └──────────────────────────────┘
```

---

## Part 1: neon_local (Test Control Plane)

### Location
- **Source**: `/home/user/neon/control_plane/`
- **Binary**: `neon_local` (invoked via `cargo neon`)
- **Config**: `.neon/config` (TOML format)

### Purpose
A minimal control plane for local development and testing. Manages a single-machine Neon cluster.

### Key Components

#### 1. LocalEnv (`local_env.rs`)
**Lines**: 1,151
**Role**: Configuration and environment management
- Manages `.neon/` directory structure
- Loads/saves TOML configuration
- Generates JWT tokens for authentication
- Tracks tenant, timeline, and endpoint mappings
- Path management for all components

**Key State**:
```rust
pub struct LocalEnv {
    pub base_data_dir: PathBuf,
    pub pg_distrib_dir: PathBuf,
    pub private_key: Vec<u8>,
    pub public_key: String,
    pub broker: NeonBroker,
    pub pageservers: Vec<PageServerNode>,
    pub safekeepers: Vec<SafekeeperNode>,
    pub storage_controller: Option<StorageController>,
}
```

#### 2. ComputeControlPlane (`endpoint.rs`)
**Lines**: 1,299
**Role**: PostgreSQL compute endpoint management
- Creates and manages compute endpoints (PostgreSQL instances)
- Generates `postgresql.conf` configurations
- Manages `endpoint.json` metadata files
- Launches `compute_ctl` processes
- Handles endpoint start/stop/reconfiguration
- Provides connection strings to clients

**Workflow**:
```
1. Create endpoint → Generate configs → Start compute_ctl
2. compute_ctl → Starts PostgreSQL with Neon extension
3. PostgreSQL → Connects to pageserver for storage
4. Reconfigure on pageserver changes (live migration support)
```

#### 3. PageServerNode (`pageserver.rs`)
**Lines**: 668
**Role**: Pageserver process lifecycle management
- HTTP client for pageserver management API
- Creates tenants and timelines
- Manages TOML configuration files
- Communicates with storage controller

#### 4. SafekeeperNode (`safekeeper.rs`)
**Lines**: 284
**Role**: WAL safekeeper management
- HTTP client to safekeeper API
- Generates JWT tokens for inter-keeper auth
- Manages safekeeper cluster membership

#### 5. StorageController Interface (`storage_controller.rs`)
**Lines**: 1,045
**Role**: Interface to storage controller (when present)
- Starts storage controller process
- HTTP client for storage controller API
- Routes tenant/timeline operations through controller

### Limitations (Why it's "Test Only")
1. **Single Machine**: All processes on one host
2. **No Persistence**: State in memory, lost on restart
3. **No HA**: No failover or redundancy
4. **No Scaling**: Cannot add/remove nodes dynamically
5. **Simplified Scheduling**: No optimization or rebalancing
6. **Local Storage**: Uses local filesystem, not distributed S3
7. **Fast Heartbeats**: 1 second intervals (not production-safe)

### Configuration Example
```toml
# .neon/config
[[pageservers]]
id = 1
listen_pg_addr = '127.0.0.1:64000'
listen_http_addr = '127.0.0.1:9898'
pg_auth_type = 'Trust'
http_auth_type = 'Trust'

[[safekeepers]]
id = 1
pg_port = 5454
http_port = 7676

[broker]
listen_addr = '127.0.0.1:50051'

[storage_controller]
max_offline = "10s"
heartbeat_interval = "1s"
```

---

## Part 2: Storage Controller (Production Control Plane)

### Location
- **Source**: `/home/user/neon/storage_controller/`
- **Binary**: `storage_controller`
- **Database**: PostgreSQL (external, durable)
- **Migrations**: `migrations/` directory

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Storage Controller Service                   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              HTTP Server (http.rs)                   │  │
│  │  • /v1/...        - Pageserver-compatible API        │  │
│  │  • /control/v1/...- Management API                   │  │
│  │  • /debug/v1/...  - Debug/testing endpoints          │  │
│  │  • /upcall/v1/... - Pageserver callbacks            │  │
│  └───────────────────────┬──────────────────────────────┘  │
│                          │                                  │
│  ┌───────────────────────▼──────────────────────────────┐  │
│  │              Service (service.rs)                    │  │
│  │  Main orchestration logic with ServiceInner RwLock  │  │
│  │                                                       │  │
│  │  State:                                              │  │
│  │  • tenants: BTreeMap<TenantShardId, TenantShard>    │  │
│  │  • nodes: HashMap<NodeId, Node>                     │  │
│  │  • safekeepers: HashMap<NodeId, Safekeeper>         │  │
│  │  • scheduler: Scheduler                             │  │
│  │  • leadership_status: LeadershipStatus              │  │
│  └───────┬──────────────┬───────────────┬──────────────┘  │
│          │              │               │                  │
│  ┌───────▼─────┐ ┌─────▼──────┐ ┌──────▼────────┐        │
│  │  Scheduler  │ │ Reconciler │ │  Heartbeater  │        │
│  │  (scheduler)│ │ (reconciler)│ │ (heartbeater) │        │
│  │             │ │             │ │               │        │
│  │ • Placement │ │ • Attach/   │ │ • Health      │        │
│  │   decisions │ │   detach    │ │   monitoring  │        │
│  │ • AZ aware  │ │ • Live      │ │ • Failure     │        │
│  │ • Load      │ │   migration │ │   detection   │        │
│  │   balancing │ │ • Generation│ │ • Node state  │        │
│  └─────────────┘ └─────────────┘ └───────────────┘        │
│                          │                                  │
│  ┌───────────────────────▼──────────────────────────────┐  │
│  │         Persistence (persistence.rs)                 │  │
│  │  Database I/O layer (diesel ORM)                     │  │
│  │  • Tenant/shard mappings                             │  │
│  │  • Node registrations                                │  │
│  │  • Generation numbers (CRITICAL for data safety)    │  │
│  └──────────────────────┬───────────────────────────────┘  │
└─────────────────────────┼───────────────────────────────────┘
                          │
                 ┌────────▼────────┐
                 │   PostgreSQL    │
                 │   (External)    │
                 │                 │
                 │ ** MUST BE      │
                 │ ** DURABLE      │
                 └─────────────────┘
```

### Core Components

#### 1. Service (`service.rs`)
**Lines**: ~8,000+
**Role**: Central orchestration engine

**Key Responsibilities**:
- Tenant lifecycle (create, delete, configure)
- Timeline lifecycle (create, delete)
- Node registration and health tracking
- Scheduling decisions via `Scheduler`
- Triggering reconciliation via `Reconciler`
- Leadership coordination (multi-controller HA)
- Compute hook notifications

**Concurrency**:
- Uses `ServiceInner` wrapped in `RwLock`
- Single lock for all state (simplifies reasoning)
- Default: 128 normal-priority reconcilers
- Default: 256 high-priority reconcilers
- Default: 32 safekeeper reconcilers

**Configuration** (`Config` struct):
```rust
pub struct Config {
    pub max_offline_interval: Duration,        // Default: 30s
    pub max_warming_up_interval: Duration,     // Default: 300s
    pub heartbeat_interval: Duration,          // Default: 5s
    pub reconciler_concurrency: usize,         // Default: 128
    pub split_threshold: Option<u64>,          // Auto-split size
    pub database_url: String,
    pub jwt_token: Option<String>,
    pub control_plane_url: Option<String>,     // For compute hooks
}
```

#### 2. Scheduler (`scheduler.rs`)
**Role**: Intelligent placement decisions

**Capabilities**:
- **Initial placement**: Choose best pageserver for new tenant
- **AZ awareness**: Prefer nodes in correct availability zones
- **Load balancing**: Distribute tenants across nodes
- **Affinity rules**: Keep shards of same tenant separate
- **Capacity management**: Track node utilization

**Placement Algorithm**:
1. Filter nodes by scheduling policy (active/draining/paused)
2. Filter by AZ preference if configured
3. Score nodes by:
   - Current shard count
   - Active attachments
   - AZ affinity
4. Select best-scoring node
5. Update node state

#### 3. Reconciler (`reconciler.rs`)
**Role**: Make observed state match intent state

**Reconciliation Process**:
```
┌─────────────────────────────────────────────────────────┐
│  Intent State          Reconciliation         Observed  │
│  (What we want)        (Actions)              State     │
│                                               (Reality) │
│  ┌────────────┐       ┌────────────┐       ┌──────────┐│
│  │ Attached   │──────►│ 1. Increment       │ Detached ││
│  │ on Node A  │       │    generation      │          ││
│  │            │       │ 2. Attach to A     │          ││
│  └────────────┘       │ 3. Update observed │          ││
│                       └────────────┘       └──────────┘│
└─────────────────────────────────────────────────────────┘
```

**Special Cases**:
- **Live migration**: Zero-downtime tenant moves
  1. Create secondary location on target node
  2. Wait for WAL catchup
  3. Promote secondary to primary (atomic)
  4. Remove old location

- **Generation safety**:
  - ALWAYS increment generation before attach
  - Use PostgreSQL transactions for atomicity
  - Prevents data corruption from split-brain

**Reconcile Timeout**: 30 seconds default

#### 4. Heartbeater (`heartbeater.rs`)
**Role**: Health monitoring and failure detection

**Process**:
1. Send HTTP health check every 5 seconds (default)
2. Track response times and failures
3. After 30 seconds no response → mark node offline
4. Trigger failover reconciliation for affected tenants
5. More tolerant during warmup (5 minutes)

**Node States**:
- `Active`: Healthy and accepting tenants
- `WarmingUp`: Just started, extended grace period
- `Offline`: Unresponsive, failover in progress

#### 5. Persistence (`persistence.rs`)
**Role**: Database abstraction layer

**What IS Persisted** (Critical for safety):
- Tenant metadata and configuration
- Shard definitions and IDs
- Node registrations
- **Generation numbers** (CRITICAL!)
- Scheduling policies
- Split states

**What is NOT Persisted** (Rebuilt at startup):
- Tenant-to-node attachments (learned from pageservers)
- Secondary locations (recomputed by scheduler)
- Observed state (gathered via re-attach API)
- In-flight reconciliations (restarted if needed)

**Why?**
- Reduces database I/O bottlenecks
- Enables in-memory performance
- Self-healing on restart via pageserver queries

**Database Schema** (`schema.rs`, generated by diesel):
```sql
-- Key tables (simplified)
CREATE TABLE tenants (
    tenant_id TEXT PRIMARY KEY,
    shard_number INTEGER,
    shard_count INTEGER,
    shard_stripe_size INTEGER,
    generation INTEGER,
    placement_policy JSON,
    config JSON,
    scheduling_policy TEXT
);

CREATE TABLE nodes (
    node_id BIGINT PRIMARY KEY,
    scheduling_policy TEXT,
    listen_http_addr TEXT,
    listen_pg_addr TEXT,
    availability_zone_id TEXT
);

CREATE TABLE controllers (
    controller_id TEXT PRIMARY KEY,
    address TEXT,
    started_at TIMESTAMP
);
```

---

## Part 3: API Interfaces

### Storage Controller HTTP APIs

#### 1. Pageserver-Compatible API (`/v1/...`)
Mimics pageserver API so clients can use controller transparently:

```bash
# Tenant operations
POST   /v1/tenant                          # Create tenant
GET    /v1/tenant/:tenant_shard_id         # Get tenant status
DELETE /v1/tenant/:tenant_shard_id         # Delete tenant
PUT    /v1/tenant/:tenant_shard_id/location_config  # Configure placement

# Timeline operations
POST   /v1/tenant/:tenant_id/timeline      # Create timeline
DELETE /v1/tenant/:tenant_id/timeline/:timeline_id  # Delete timeline
GET    /v1/tenant/:tenant_id/timeline/:timeline_id  # Timeline info
```

**Request Example**:
```json
POST /v1/tenant
{
  "new_tenant_id": "0123456789abcdef0123456789abcdef",
  "generation": 1,
  "shard_parameters": {
    "count": 4,
    "stripe_size": 32768
  },
  "placement_policy": {
    "attached": 1,
    "secondary": 1
  },
  "config": {
    "gc_period": "1h",
    "checkpoint_distance": 268435456
  }
}
```

#### 2. Storage Controller Management API (`/control/v1/...`)
Advanced cluster management:

```bash
# Node management
POST   /control/v1/node                    # Register pageserver
GET    /control/v1/node                    # List all nodes
PUT    /control/v1/node/:node_id/config    # Configure node

# Tenant operations
GET    /control/v1/tenant/:tenant_id/describe  # Full tenant info
POST   /control/v1/tenant/:tenant_id/shard-split  # Split shards
POST   /control/v1/tenant/:tenant_id/migrate  # Migrate tenant

# Cluster operations
POST   /control/v1/node/:node_id/drain     # Drain node for maintenance
POST   /control/v1/node/:node_id/fill      # Enable node after drain
```

**Node Registration Example**:
```json
POST /control/v1/node
{
  "node_id": 42,
  "listen_http_addr": "10.0.1.5:9898",
  "listen_pg_addr": "10.0.1.5:64000",
  "availability_zone_id": "us-east-1a"
}
```

#### 3. Upcall API (`/upcall/v1/...`)
Called BY pageservers TO storage controller:

```bash
POST /upcall/v1/re-attach    # Pageserver startup registration
POST /upcall/v1/validate     # Validate generations before deletion
GET  /upcall/v1/timeline-import-status  # Check import progress
PUT  /upcall/v1/timeline-import-status  # Update import progress
```

**Re-attach Request** (pageserver → controller):
```json
POST /upcall/v1/re-attach
{
  "node_id": 42,
  "tenants": [
    {
      "id": "0123456789abcdef0123456789abcdef-0001",
      "gen": 15,
      "mode": "AttachedSingle"
    },
    {
      "id": "0123456789abcdef0123456789abcdef-0002",
      "gen": 8,
      "mode": "AttachedMulti"
    }
  ]
}
```

**Re-attach Response** (controller → pageserver):
```json
{
  "tenants": [
    {
      "id": "0123456789abcdef0123456789abcdef-0001",
      "gen": 16,  // Incremented!
      "mode": "AttachedSingle"
    },
    {
      "id": "0123456789abcdef0123456789abcdef-0002",
      "gen": null,  // Detach this one
      "mode": null
    }
  ]
}
```

#### 4. Debug API (`/debug/v1/...`)
For testing and operations:

```bash
GET /debug/v1/tenant/:tenant_id/consistency-check
GET /debug/v1/scheduler
GET /debug/v1/node/:node_id/shards
```

### Compute Hook Notifications

The storage controller calls external APIs to notify compute nodes of changes:

#### notify-attach Hook
Called when tenant's pageserver attachment changes:

```http
PUT {control_plane_url}/notify-attach
Authorization: Bearer {control_plane_jwt_token}
Content-Type: application/json

{
  "tenant_id": "1f359dd625e519a1a4e8d7509690f6fc",
  "stripe_size": 32768,
  "shards": [
    {"node_id": 344, "shard_number": 0},
    {"node_id": 722, "shard_number": 1},
    {"node_id": 455, "shard_number": 2},
    {"node_id": 888, "shard_number": 3}
  ]
}
```

**Handler must**:
1. Update compute's `neon.pageserver_connstring` to comma-separated list
2. Update `neon.shard_stripe_size` if present
3. Send SIGHUP to PostgreSQL process
4. Return 200 only if successful (controller retries on error)

#### notify-safekeepers Hook
Called when timeline's safekeeper membership changes:

```http
PUT {control_plane_url}/notify-safekeepers
Content-Type: application/json

{
  "tenant_id": "1f359dd625e519a1a4e8d7509690f6fc",
  "timeline_id": "8e5e3d0e9a4f5b6c7d8e9f0a1b2c3d4e",
  "generation": 42,
  "safekeepers": [
    {"id": 1, "hostname": "sk1.example.com"},
    {"id": 2, "hostname": "sk2.example.com"},
    {"id": 3, "hostname": "sk3.example.com"}
  ]
}
```

---

## Part 4: Component Interactions

### Flow 1: Creating a New Tenant

```
┌─────────┐         ┌────────────────┐         ┌────────────┐         ┌──────────┐
│  Client │         │Storage Ctrl    │         │ PostgreSQL │         │Pageserver│
└────┬────┘         └───────┬────────┘         └─────┬──────┘         └────┬─────┘
     │                      │                        │                     │
     │ POST /v1/tenant      │                        │                     │
     ├─────────────────────►│                        │                     │
     │                      │                        │                     │
     │                      │ 1. Schedule placement  │                     │
     │                      │    (via Scheduler)     │                     │
     │                      │                        │                     │
     │                      │ 2. INSERT tenant row   │                     │
     │                      ├───────────────────────►│                     │
     │                      │                        │                     │
     │                      │ 3. INSERT shard rows   │                     │
     │                      │    with generation=1   │                     │
     │                      ├───────────────────────►│                     │
     │                      │◄───────────────────────┤                     │
     │                      │ COMMIT                 │                     │
     │                      │                        │                     │
     │                      │ 4. Spawn Reconciler    │                     │
     │                      │    task                │                     │
     │                      │                        │                     │
     │                      │ 5. PUT /v1/tenant/     │                     │
     │                      │    location_config     │                     │
     │                      ├─────────────────────────────────────────────►│
     │                      │                        │                     │
     │                      │                        │      6. Create      │
     │                      │                        │         tenant      │
     │                      │                        │         locally     │
     │                      │◄─────────────────────────────────────────────┤
     │                      │ 200 OK                 │                     │
     │                      │                        │                     │
     │                      │ 7. Update observed     │                     │
     │                      │    state in memory     │                     │
     │                      │                        │                     │
     │◄─────────────────────┤                        │                     │
     │ 201 Created          │                        │                     │
     │ {tenant_id, shards}  │                        │                     │
```

### Flow 2: Pageserver Failure and Failover

```
┌────────────┐       ┌────────────┐       ┌────────────┐       ┌────────────┐
│Heartbeater │       │Service     │       │Scheduler   │       │Reconciler  │
└─────┬──────┘       └─────┬──────┘       └─────┬──────┘       └─────┬──────┘
      │                    │                    │                    │
      │ 1. Detect node     │                    │                    │
      │    offline (30s)   │                    │                    │
      ├───────────────────►│                    │                    │
      │                    │                    │                    │
      │                    │ 2. Mark node       │                    │
      │                    │    offline         │                    │
      │                    │                    │                    │
      │                    │ 3. Find affected   │                    │
      │                    │    tenants         │                    │
      │                    │                    │                    │
      │                    │ 4. Request new     │                    │
      │                    │    placement       │                    │
      │                    ├───────────────────►│                    │
      │                    │                    │                    │
      │                    │                    │ 5. Select new      │
      │                    │                    │    pageservers     │
      │                    │                    │    (from secondary │
      │                    │                    │    if available)   │
      │                    │◄───────────────────┤                    │
      │                    │                    │                    │
      │                    │ 6. Update intent   │                    │
      │                    │    state           │                    │
      │                    │                    │                    │
      │                    │ 7. Spawn reconcile │                    │
      │                    │    tasks           │                    │
      │                    ├─────────────────────────────────────────►│
      │                    │                    │                    │
      │                    │                    │       8. Increment │
      │                    │                    │          generation│
      │                    │                    │       9. Promote   │
      │                    │                    │          secondary │
      │                    │                    │      10. Notify    │
      │                    │                    │          compute   │
      │                    │◄─────────────────────────────────────────┤
      │                    │                    │                    │
      │                    │ 11. Update observed│                    │
      │                    │     state          │                    │
```

### Flow 3: Pageserver Startup/Registration

```
┌──────────┐         ┌────────────┐         ┌──────────┐
│Pageserver│         │Storage Ctrl│         │PostgreSQL│
└────┬─────┘         └─────┬──────┘         └────┬─────┘
     │                     │                     │
     │ 1. Read metadata.json│                    │
     │    (node_id, addrs) │                     │
     │                     │                     │
     │ 2. POST /upcall/v1/ │                     │
     │    re-attach        │                     │
     ├────────────────────►│                     │
     │  {node_id,          │                     │
     │   tenants:[...]}    │                     │
     │                     │                     │
     │                     │ 3. Validate node    │
     │                     │    registration     │
     │                     ├────────────────────►│
     │                     │◄────────────────────┤
     │                     │                     │
     │                     │ 4. For each tenant: │
     │                     │    Increment gen    │
     │                     │    or mark stale    │
     │                     ├────────────────────►│
     │                     │◄────────────────────┤
     │                     │                     │
     │◄────────────────────┤                     │
     │ {tenants:[          │                     │
     │   {id, gen, mode}]} │                     │
     │                     │                     │
     │ 5. Apply response:  │                     │
     │    - Update gens    │                     │
     │    - Detach stale   │                     │
     │    - Attach new     │                     │
```

---

## Part 5: Building a Production Control Plane

### What You Need to Build

Based on the Neon architecture, here's what's required for a production control plane:

#### 1. Database Backend (ESSENTIAL)
**Requirement**: Durable PostgreSQL instance
- **Size**: Small (1 CPU, 1GB RAM sufficient for most)
- **Critical**: MUST be durable (no ephemeral storage)
- **Why**: Stores generation numbers (data safety critical)
- **Schema**: Use Neon's migrations (`storage_controller/migrations/`)

#### 2. HTTP API Server
**Components**:
- REST API implementation (see `storage_controller/src/http.rs`)
- Four API surfaces:
  - Pageserver-compatible (`/v1/...`)
  - Management (`/control/v1/...`)
  - Upcall (`/upcall/v1/...`)
  - Debug (`/debug/v1/...`)
- JWT authentication
- Rate limiting per tenant

#### 3. Core Service Logic
**Components** (see `storage_controller/src/service.rs`):
- **State management**:
  - In-memory tenant/shard map
  - Node registry
  - Safekeeper registry
- **Reconciliation engine**:
  - Intent vs. observed state tracking
  - Concurrent reconciler tasks (default 128)
  - Generation number management
- **Scheduling**:
  - Placement algorithm (see `scheduler.rs`)
  - AZ awareness
  - Load balancing
- **Health monitoring**:
  - Heartbeat loop (5s intervals)
  - Failure detection (30s timeout)
  - Node state machine

#### 4. Compute Hook Integration
**Implementation**:
```rust
// Pseudo-code based on Neon's ComputeHook
async fn notify_compute(
    tenant_id: TenantId,
    shards: Vec<(NodeId, ShardNumber)>,
    stripe_size: Option<ShardStripeSize>,
) -> Result<()> {
    let body = ComputeHookNotifyRequest {
        tenant_id,
        stripe_size,
        shards: shards.into_iter().map(|(node_id, shard_number)| {
            ComputeHookNotifyRequestShard { node_id, shard_number }
        }).collect(),
    };

    let response = reqwest::Client::new()
        .put(format!("{}/notify-attach", control_plane_url))
        .bearer_auth(jwt_token)
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        // Retry logic here
        return Err(anyhow!("Compute notification failed"));
    }

    Ok(())
}
```

**Your compute management system must**:
1. Expose `/notify-attach` and `/notify-safekeepers` endpoints
2. Update PostgreSQL configuration files
3. Send SIGHUP to reload
4. Return 200 only on success (controller retries failures)

#### 5. Deployment Configuration

**Minimum Production Setup**:
```bash
# Storage Controller
./storage_controller \
  --listen 0.0.0.0:8080 \
  --database-url postgresql://user:pass@db-host:5432/storcon \
  --jwt-token <pageserver-auth-token> \
  --control-plane-jwt-token <compute-hook-auth-token> \
  --control-plane-url https://your-compute-api.com/hooks \
  --max-offline-interval 30s \
  --heartbeat-interval 5s \
  --reconciler-concurrency 128
```

**Pageserver Configuration** (`pageserver.toml`):
```toml
[control_plane]
control_plane_api = "http://storage-controller:8080/upcall/v1/"
control_plane_api_token = "<same-jwt-token>"
```

**Pageserver `metadata.json`** (auto-registration):
```json
{
  "host": "pageserver1.example.com",
  "http_host": "pageserver1.example.com",
  "http_port": 9898,
  "port": 64000
}
```

#### 6. High Availability (Optional but Recommended)
- Deploy multiple storage controller instances
- Use leadership coordination via database (see `leadership.rs`)
- Secondary controllers proxy to leader
- No strong consensus needed (by design)

### Critical Implementation Notes

#### Generation Number Safety
**NEVER** bypass generation number increments. This is the cornerstone of data safety:

```rust
// CRITICAL: Always increment generation before attach
async fn attach_tenant(
    tenant_shard_id: TenantShardId,
    node_id: NodeId,
) -> Result<Generation> {
    // 1. Begin database transaction
    let mut tx = db.begin().await?;

    // 2. Read current generation
    let current_gen = tx.get_generation(tenant_shard_id).await?;

    // 3. Increment generation
    let new_gen = current_gen.next();
    tx.set_generation(tenant_shard_id, new_gen).await?;

    // 4. COMMIT before making pageserver calls
    tx.commit().await?;

    // 5. Now safe to attach to pageserver
    pageserver_client.location_config(
        tenant_shard_id,
        LocationConfig {
            mode: LocationConfigMode::AttachedSingle,
            generation: new_gen,
            // ...
        }
    ).await?;

    Ok(new_gen)
}
```

#### Reconciliation Idempotency
All reconciliation operations must be idempotent:
- Attaching already-attached tenant: no-op
- Detaching already-detached tenant: no-op
- Generation mismatches: abort and retry

#### Minimal Viable Features

**Phase 1 (MVP)**:
- ✅ Tenant create/delete
- ✅ Timeline create/delete
- ✅ Basic scheduling (round-robin)
- ✅ Re-attach API
- ✅ Generation management
- ✅ Compute hooks

**Phase 2 (Production-ready)**:
- ✅ Heartbeat monitoring
- ✅ Automatic failover
- ✅ AZ-aware scheduling
- ✅ Load balancing
- ✅ Live migration

**Phase 3 (Advanced)**:
- ✅ Shard splitting
- ✅ Auto-scaling
- ✅ Drain/fill operations
- ✅ Multi-controller HA
- ✅ Optimization loop

---

## Part 6: Key Differences Summary

| Feature | neon_local | Storage Controller |
|---------|------------|-------------------|
| **Purpose** | Local testing | Production clusters |
| **Scope** | Single machine | Distributed cluster |
| **State** | In-memory | PostgreSQL backend |
| **Scheduling** | Simple/random | AZ-aware, optimized |
| **Failover** | Manual | Automatic (30s) |
| **Reconciliation** | Minimal | Full reconciliation loop |
| **Generation Mgmt** | Basic | Transactional, durable |
| **Scaling** | Fixed nodes | Dynamic add/remove |
| **HA** | None | Multi-controller support |
| **Monitoring** | Fast heartbeats | Production intervals |
| **Compute Hooks** | Direct integration | HTTP callbacks |
| **Database** | None | PostgreSQL required |

---

## Part 7: Reference Documentation

### Key Files to Study

1. **Storage Controller Core**:
   - `/storage_controller/src/service.rs` (8000+ lines) - Main logic
   - `/storage_controller/src/http.rs` (3000+ lines) - API layer
   - `/storage_controller/src/scheduler.rs` - Placement logic
   - `/storage_controller/src/reconciler.rs` - State reconciliation
   - `/storage_controller/src/persistence.rs` - Database layer

2. **Local Control Plane**:
   - `/control_plane/src/local_env.rs` - Configuration
   - `/control_plane/src/endpoint.rs` - Compute management
   - `/control_plane/src/storage_controller.rs` - SC interface

3. **Documentation**:
   - `/docs/storage_controller.md` - Production deployment guide
   - `/docs/rfcs/2025-02-14-storage-controller.md` - Architecture RFC
   - `/docs/rfcs/033-storage-controller-drain-and-fill.md` - Graceful deploys
   - `/docs/rfcs/037-storage-controller-restarts.md` - HA design

4. **API Definitions**:
   - `pageserver_api/src/controller_api.rs` - API structures
   - `pageserver_api/src/upcall_api.rs` - Upcall messages

### CLI Tools

**Storage Controller Management**:
```bash
# List nodes
storcon_cli node list

# Mark node for drain
storcon_cli node configure --node-id 42 --scheduling drain

# View tenant details
storcon_cli tenant describe <tenant-id>

# Force reconciliation
storcon_cli tenant reconcile <tenant-id>
```

---

## Part 8: Deployment Checklist

### Infrastructure Requirements
- [ ] Durable PostgreSQL instance (1 CPU, 1GB RAM minimum)
- [ ] Storage controller host(s) (2+ CPU, 4GB+ RAM recommended)
- [ ] Network connectivity: controller ↔ pageservers
- [ ] Network connectivity: controller ↔ safekeepers
- [ ] Network connectivity: controller ↔ compute management API

### Configuration
- [ ] Generate JWT tokens for:
  - [ ] Pageserver authentication
  - [ ] Safekeeper authentication
  - [ ] Compute hook authentication
- [ ] Configure database connection string
- [ ] Set control plane URL for compute hooks
- [ ] Configure heartbeat intervals
- [ ] Set reconciler concurrency limits

### Pageserver Setup
- [ ] Add `control_plane_api` to `pageserver.toml`
- [ ] Add `control_plane_api_token` to `pageserver.toml`
- [ ] Create `metadata.json` with node info
- [ ] Verify pageserver can reach controller

### Compute Integration
- [ ] Implement `/notify-attach` handler
- [ ] Implement `/notify-safekeepers` handler
- [ ] Add JWT validation
- [ ] Test configuration reload (SIGHUP)

### Testing
- [ ] Create test tenant
- [ ] Create test timeline
- [ ] Verify pageserver attachment
- [ ] Test compute endpoint connectivity
- [ ] Simulate pageserver failure (failover)
- [ ] Test tenant migration
- [ ] Verify generation increment on each change

### Monitoring
- [ ] Prometheus metrics endpoint (`/metrics`)
- [ ] Alert on reconciliation failures
- [ ] Alert on prolonged node offline
- [ ] Track database connection health
- [ ] Monitor generation number progression

---

## Conclusion

The Neon control plane is a **two-tiered system**:

1. **`neon_local`** provides a simplified, single-machine orchestrator perfect for development and testing. It manages all processes locally and keeps state in memory.

2. **Storage Controller** is the production-grade distributed system that manages a cluster of pageserver and safekeeper nodes, provides HA, handles automatic failover, and ensures data safety through durable generation number management.

**To build a production control plane**, you primarily need to:
1. Deploy the existing `storage_controller` binary (it's already production-ready)
2. Provide a durable PostgreSQL database
3. Implement compute hook handlers in your compute management system
4. Configure pageservers to communicate with the controller

The Neon storage controller is well-architected, thoroughly documented, and used in production by Neon's cloud service. You can use it as-is or as a reference for building your own implementation.
