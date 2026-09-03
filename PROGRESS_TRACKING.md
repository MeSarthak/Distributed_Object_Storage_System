Backend Developer — Project Objective & Development Context

You are working as the AI developer for a Distributed Object Storage System with Intelligent Data Placement and Adaptive Replication. Before making any implementation decision, understand that this is not a normal CRUD file-upload application. The primary objective is to build a prototype-scale distributed object storage platform that demonstrates core distributed-systems concepts: multiple independent storage nodes, centralized metadata management, intelligent placement, replication, heartbeat-based failure detection, automatic self-healing, data-integrity verification, load distribution, and adaptive replication.

Core Objective

Build a reliable, modular, and horizontally scalable object-storage system in which users can upload, download, search, and delete objects while the system automatically decides where objects should be stored and how many replicas should exist.

The system must:

Distribute objects across multiple storage nodes rather than storing everything on a single server.

Select storage nodes intelligently using storage availability, CPU, memory, network latency, node health, and current load.

Maintain replicas so objects remain available when a storage node fails.

Detect failed nodes through periodic heartbeats and automatically mark them offline.

Automatically recover lost replicas by copying data from healthy replicas to a newly selected healthy node.

Verify object integrity using SHA-256 checksums during storage, download, and recovery.

Adapt replication according to object access frequency using HOT/WARM/COLD classification.

Maintain consistent metadata using PostgreSQL transactions and atomic updates.

Provide REST APIs for authentication, object operations, metadata, internal storage-node communication, and monitoring.

Provide a React dashboard for users and administrators to observe and interact with the system.

Run the initial prototype through Docker Compose with one coordinator/API service, PostgreSQL, three or more independent Go storage nodes, and a React frontend.

Demonstrate distributed-system behavior through controlled fault scenarios such as node failure, corrupted objects, uneven load, and recovery.

Architectural Intent

Treat the Go API/coordinator as the central control and metadata layer, PostgreSQL as the metadata database, and each Go storage-node container as an independent storage server with its own persistent volume. The coordinator should not directly become the permanent storage location for objects; instead, it should authenticate requests, manage metadata, make placement decisions, coordinate replication/recovery, and communicate with storage nodes through internal APIs.

The architecture should be designed so that adding storage nodes does not require changing application logic. Node addresses, placement weights, replication thresholds, heartbeat intervals, timeouts, and other policies must remain configurable through environment variables/configuration rather than being hardcoded.

Development Principles for the Backend Developer

When modifying or extending the project:

Preserve the distributed-storage architecture. Do not simplify it into a single-server CRUD application.

Understand the existing architecture before changing it.

Explain major architectural decisions before implementing significant changes.

Prefer production-quality, modular, maintainable Go code with clear separation of concerns.

Use proper error handling, validation, transactions, structured logging, and standardized API responses.

Never hardcode storage-node addresses, credentials, secrets, thresholds, weights, or operational policies.

Keep services restartable and Docker-compatible.

Preserve metadata consistency and minimum replica guarantees during failures.

Add tests for important distributed-system behavior, especially placement, replication, failure detection, recovery, checksum validation, and concurrent operations.

Respect the documented limitations and prototype scale. Do not claim production-scale guarantees without actual benchmarking.

Expected End State

The completed system should allow a user to upload an object, authenticate the request, calculate its checksum, intelligently select suitable storage nodes, create the required replicas, and persist metadata. During downloads, the system should select a healthy replica and verify its checksum. If a storage node fails, heartbeat detection should identify the failure, placement should exclude the failed node, and the self-healing mechanism should recreate lost replicas automatically. Administrators should be able to observe cluster health, node metrics, placement decisions, replica distribution, failures, recovery operations, and system logs from the dashboard.

Use the detailed requirements and progress checklist below as the source of truth for implementation scope, architecture, features, testing, performance targets, scalability expectations, limitations, and current project status.

# Distributed Object Storage System - Progress Tracking

## Project Overview
Building a **Distributed Object Storage System with Intelligent Data Placement and Adaptive Replication** per the SRS document.

**Status**: In Progress

---

## Phase Plan & Status

### Phase 1: Infrastructure & Foundation
- [x] 1.1 Project Setup - Initialize Go module, Docker Compose, multi-service structure
- [x] 1.2 Database Schema - Create PostgreSQL tables (6 primary entities, indexes, constraints)
- [x] 1.3 Configuration System - Strongly typed environment variables & config loaders

### Phase 2: Core Services
- [ ] 2.1 Authentication Service - Register, login, JWT, bcrypt, RBAC middleware
- [ ] 2.2 Storage Node Service - Go service with internal APIs (store, get, delete, verify), heartbeats
- [ ] 2.3 Placement Engine - Multi-metric weighted scoring, exclusion thresholds, $N$-node ranking
- [ ] 2.4 Replication Manager - Adaptive replication, HOT/WARM/COLD classification
- [ ] 2.5 Heartbeat & Failure Detection - Periodic heartbeats, timeout handling, audit logging
- [ ] 2.6 Self-Healing System - Automatic replica recovery on node failure with atomic updates

### Phase 3: API Endpoints
- [ ] 3.1 Authentication APIs - register, login, validate
- [ ] 3.2 Object APIs - upload (multipart, placement, replicate to $K$ nodes, checksum, DB), download (replica fallback, stream, verify checksum, log access), delete (purge across all replicas, DB delete), search (filter by name/query)
- [ ] 3.3 Metadata APIs - lookup (`/api/metadata/:id` with replica details)
- [ ] 3.4 Storage Node internal APIs - chunk store, stream, verify, delete, heartbeat
- [ ] 3.5 Monitoring APIs - cluster status (`/api/cluster/status`), node list with metrics (`/api/cluster/nodes`), system audit logs (`/api/logs`)

### Phase 4: Frontend Dashboard
- [ ] 4.1 Login/Register Pages - High-performance dark obsidian auth console with demo credentials, role toggle (USER/ADMIN), form validation, and JWT persistence
- [ ] 4.2 User Dashboard - Live telemetry overview (stored objects, physical capacity, online node fleet, tier breakdown, and quick upload)
- [ ] 4.3 Object Explorer - Searchable object manager with access tier filters (HOT/WARM/COLD), sortable table & card grid views, SHA-256 copy helpers, streaming download, and cascade purge
- [ ] 4.4 Upload Interface - Drag-and-drop file selector, browser-native Web Crypto SHA-256 pre-calculation, upload progress bar, and multi-node placement visualizer
- [ ] 4.5 Admin Dashboard - Real-time cluster hardware metrics (CPU, RAM, storage disk utilization, latency, and heartbeat ages) with live system audit logs and mathematical placement formula inspector

### Phase 5: Fault Scenarios & Testing
- [ ] 5.1 Storage Node Failure - kill container → self-healing
- [ ] 5.2 Download During Failure - fallback to healthy replica
- [ ] 5.3 Storage Exclusion - node threshold → excluded from placement
- [ ] 5.4 Corrupted Object - checksum mismatch → reject + recover
- [ ] 5.5 Load Balancing - distribute uploads across nodes

### Phase 6: Performance & Reliability
- [ ] 6.1 Performance Targets - login ≤2s, list ≤2s, upload init ≤3s
- [ ] 6.2 Reliability Features - automatic failure detection, placement exclusion
- [ ] 6.3 Backup Strategy - PostgreSQL backups, metadata restoration

### Phase 7: Modular Architecture & Polish
- [ ] 7.1 Go Module Structure - clean separation of concerns
- [ ] 7.2 Structured Logging - all events with context
- [ ] 7.3 Error Handling - standardized error codes
- [ ] 7.4 Documentation - README, API specs, setup guide

---

## Requirements Checklist (from SRS)

### Technology Stack
- [ ] Frontend: React.js, Tailwind CSS, Axios
- [ ] Backend: Go, Gin framework, REST APIs
- [ ] Database: PostgreSQL (metadata only)
- [ ] Storage: Multiple Go storage nodes in Docker / multiple storage servers (in future)
- [ ] Deployment: Docker, Docker Compose

### Authentication & Security
- [ ] User registration with bcrypt password hashing
- [ ] Login with JWT authentication
- [ ] Protected APIs with JWT validation
- [ ] Role-based authorization (USER/ADMIN)
- [ ] Input validation
- [ ] Ownership validation

### Object Workflow
- [ ] Upload: authenticate → generate ID → SHA-256 checksum → placement → store on nodes → metadata → success
- [ ] Download: authenticate → ownership → metadata → replica locations → select best → download → SHA-256 verification
- [ ] Delete: authenticate → ownership → find replicas → delete all → verify cleanup → delete metadata
- [ ] Search: user's objects with filtering

### Intelligent Data Placement
- [ ] Placement Engine evaluates nodes on: storage availability, CPU, memory, network latency, node health, current load
- [ ] Configurable weights (not hardcoded)
- [ ] Rejects offline/overloaded/low-storage nodes
- [ ] Completes within 2 seconds
- [ ] Supports future strategies: Round Robin, Least Loaded, Consistent Hashing

### Adaptive Replication
- [ ] Track access frequency per object
- [ ] Classify: HOT (high access) → increase replicas
- [ ] CLASSIFY: WARM (moderate) → normal replication
- [ ] CLASSIFY: COLD (low access) → reduce replicas (min enforced)
- [ ] Periodic replication decisions
- [ ] Configurable thresholds and policies

### Heartbeat-Based Failure Detection
- [ ] Nodes send heartbeat every configurable interval
- [ ] Timeout threshold: missed heartbeats → OFFLINE
- [ ] Metadata tracks: lastHeartbeat, status, resource metrics
- [ ] Offline node: excluded from placement, triggers self-healing

### Self-Healing Replication
- [ ] On node failure: identify objects with replicas on failed node
- [ ] Find healthy replicas on other nodes
- [ ] Select new destination via Placement Engine
- [ ] Copy object, verify checksum
- [ ] Update metadata atomically
- [ ] Minimum replica count always maintained
- [ ] No manual intervention required for normal recovery

### Download Workflow
- [ ] Authenticate → verify ownership → metadata lookup → replica locations → filter healthy → select best → download → SHA-256 checksum verification

### Object Deletion
- [ ] Authenticate → verify ownership → metadata lookup → find all replicas → delete replicas → verify cleanup → delete metadata
- [ ] Frontend confirmation dialog before deletion

### Storage Node Service
- [ ] Internal APIs: POST /internal/storage/store, GET /internal/storage/{id}, DELETE /internal/storage/{id}, POST /internal/storage/heartbeat
- [ ] Reports: node ID, hostname, IP, storage capacity, used storage, CPU, memory, latency, health status, last heartbeat
- [ ] Local Docker volumes for independent storage behavior

### PostgreSQL Database Model
- [ ] users: user_id UUID PK, name, email UNIQUE, password_hash, role, created_at
- [ ] objects: object_id UUID PK, owner_id UUID FK, object_name, file_size, mime_type, checksum SHA-256, upload_time, last_accessed, replication_factor
- [ ] storage_nodes: node_id UUID PK, hostname, ip_address, total_storage, used_storage, cpu_usage, memory_usage, latency, status, last_heartbeat
- [ ] replicas: replica_id UUID PK, object_id UUID FK, node_id UUID FK, status (HEALTHY/RECOVERING/LOST), created_at
- [ ] access_logs: access_id UUID PK, object_id UUID FK, user_id UUID FK, access_time, response_time
- [ ] system_logs: log_id UUID PK, event_type, timestamp, description, severity

### Database Integrity
- [ ] Every object belongs to valid user
- [ ] Every replica references existing object
- [ ] Every replica belongs to valid storage node
- [ ] Object checksum unchanged
- [ ] Replication factor matches healthy replicas
- [ ] Unique emails
- [ ] Globally unique object IDs and node IDs
- [ ] Atomic metadata updates

### REST API Design
- [ ] Auth: POST /api/auth/register, POST /api/auth/login, GET /api/auth/validate
- [ ] Objects: POST /api/objects, GET /api/objects/{id}, DELETE /api/objects/{id}, GET /api/objects, GET /api/objects/search
- [ ] Metadata: GET /api/metadata/{id}, PUT /api/metadata/{id}
- [ ] Storage Nodes: POST /internal/storage/store, GET /internal/storage/{id}, DELETE /internal/storage/{id}, POST /internal/storage/heartbeat
- [ ] Monitoring: GET /api/cluster/status, GET /api/cluster/nodes, GET /api/logs
- [ ] Standardized responses with success/message/data
- [ ] Error codes: AUTH_401, AUTH_403, OBJ_404, NODE_503, META_500, REPL_500

### Frontend Requirements
- [ ] Login Page: email, password, login, register link
- [ ] User Dashboard: total objects, storage used, recent uploads, upload/download/delete
- [ ] Object Explorer: object name, size, upload date, replication factor, storage status, download, delete
- [ ] Upload Interface: file selector, progress bar, status notifications, cancel upload
- [ ] Admin Dashboard: cluster capacity, used/free capacity, online/offline nodes, CPU/RAM/storage, object stats, placement decisions, replica distribution, heartbeats, failures, self-healing events, system logs

### Monitoring & Observability
- [ ] Log all important events: login, upload, download, delete, node registration/leaving, heartbeat failure, replica creation/deletion, placement decisions, self-healing operations, errors
- [ ] Each log: timestamp, user_id, object_id, node_id, operation_type, status, error_message
- [ ] Admin dashboard exposes these logs

### Performance Targets
- [ ] Login ≤ 2 seconds
- [ ] List Objects ≤ 2 seconds
- [ ] Upload initialization ≤ 3 seconds
- [ ] Metadata lookup ≤ 1 second
- [ ] Download initialization ≤ 2 seconds
- [ ] Placement decision ≤ 2 seconds

### Scalability Requirements
- [ ] Horizontal scalability - adding storage nodes doesn't require changing application logic
- [ ] New nodes register, start heartbeat, become eligible for placement
- [ ] Support increasing: number of storage nodes, total storage capacity, stored object count

### Initial Deployment Scale
- [ ] Docker Compose: 1 × Go API/Coordinator, 1 × PostgreSQL, 3+ × Go Storage Nodes, 1 × React Frontend
- [ ] Separate persistent volumes per storage node
- [ ] Configurable number of storage nodes
- [ ] Local/Demonstration scale (not Internet scale)

### Docker Architecture
- [ ] Separate containers: frontend, backend/coordinator, postgres, storage-node-1/2/3/...
- [ ] Docker Compose networking via service names
- [ ] Per-node volumes: storage_node_1_data, storage_node_2_data, storage_node_3_data

### Fault Scenarios
- [ ] Scenario 1: Storage Node Failure - kill container → heartbeat timeout → OFFLINE → replica recovery
- [ ] Scenario 2: Download During Node Failure - failed replica ignored → healthy replica selected
- [ ] Scenario 3: Storage Exclusion - node exceeds threshold → excluded from placement
- [ ] Scenario 4: Corrupted Object - checksum mismatch → reject → find healthy replica
- [ ] Scenario 5: Uneven Load - multiple uploads → verify distribution across nodes

### Reliability & Availability
- [ ] System operational with one or more nodes unavailable (healthy replicas exist)
- [ ] Automatic node failure detection
- [ ] Placement exclusion of failed nodes
- [ ] Download redirected to healthy replicas
- [ ] Lost replicas restored
- [ ] Metadata consistency maintained

### Backup & Recovery
- [ ] Periodic PostgreSQL backups
- [ ] Metadata restoration support
- [ ] Replica recreation after node failure
- [ ] Note: Replication protects data, NOT metadata backups

### Architectural Trade-offs (documented)
- [ ] Centralized Metadata: simpler vs single coordination point
- [ ] Replication: high availability vs higher storage consumption
- [ ] REST APIs: simple/easy debug vs more overhead than binary protocols
- [ ] Docker Storage Nodes: easy deployment/testing vs doesn't represent heterogeneous production hardware
- [ ] Adaptive Replication: better storage efficiency vs additional monitoring overhead

### Known Limitations (acknowledged)
- [ ] Single Metadata Service
- [ ] Single PostgreSQL database
- [ ] No multi-region deployment
- [ ] Prototype-scale only
- [ ] JWT auth without OAuth/LDAP
- [ ] No object versioning
- [ ] No erasure coding
- [ ] No lifecycle policies
- [ ] Replication-based fault tolerance only
- [ ] Local Docker deployment

### Future-Ready Architecture
- [ ] Designed interfaces for: multiple metadata services, distributed metadata DB, leader election, Raft/Paxos, load balancer, microservices, Kubernetes, cloud deployment, multi-region, erasure coding, object versioning, lifecycle management, end-to-end encryption, content deduplication, object compression, advanced monitoring, AI-assisted placement

### Testing Requirements
#### Authentication
- [ ] Registration, duplicate email, login, wrong password, expired/invalid JWT, unauthorized API access

#### Object Operations
- [ ] Upload, download, delete, search, ownership checks, checksum validation

#### Placement
- [ ] Healthy node selection, offline node exclusion, low-storage exclusion, overloaded-node avoidance, load distribution

#### Replication
- [ ] Initial replica creation, hot-object replication, cold-object replica reduction, minimum replica enforcement

#### Failure Recovery
- [ ] Storage-node failure, heartbeat timeout, replica recovery, download during node failure

#### Data Integrity
- [ ] Correct checksum, corrupted object detection, replica recovery

#### Scalability
- [ ] Adding new storage node, increasing cluster storage, multiple concurrent operations

### Performance & Scale Interpretation
- [ ] Architectural scalability: designed for horizontal scaling
- [ ] Demonstrated scale: initial implementation constrained by local hardware
- [ ] Production scale: not claimed without benchmarking

### What AI Developer Should Deliver
- [ ] Understand complete architecture, don't simplify to ordinary CRUD
- [ ] Preserve distributed-storage behavior
- [ ] Explain architectural decisions before major changes
- [ ] Production-quality, modular Go code
- [ ] Proper error handling
- [ ] Transactions for metadata consistency
- [ ] Structured logging
- [ ] No hardcoded node addresses
- [ ] Configurable thresholds/weights/policies/intervals/timeouts
- [ ] Environment variables for configuration/secrets
- [ ] Clean Docker Compose configuration
- [ ] Restartable services
- [ ] Consistent API design
- [ ] Tests for core distributed-system behavior

---

## Current Phase: Phase 2: Core Services (Phase 1 Completed)