# GCON Roadmap

A high-level view of planned features, improvements, and milestones for GCON.

---

## Current Release: v0.10

**Status**: Stable

**Features**:
- ✅ Job submission and scheduling
- ✅ Cryptographically signed execution receipts
- ✅ Multi-step workflow support (DAGs)
- ✅ Web dashboard
- ✅ REST API
- ✅ Python SDK
- ✅ Offline receipt verification

**Known Limitations**:
- ⚠️ No authentication (local network assumed trusted)
- ⚠️ In-memory state (no persistence across restarts)
- ⚠️ Single coordinator (no HA setup)

---

## v0.11: Authentication & RBAC

**Status**: In Development

**Timeline**: Q3 2026

**Goals**:
- Implement JWT bearer token authentication
- Per-user API quotas and rate limiting
- Role-based access control (RBAC)
  - Admin: All permissions
  - Developer: Submit jobs, view receipts
  - Viewer: Read-only access
- API key management UI in dashboard

**Breaking Changes**: None (backward compatible)

**PRs/Issues**:
- [ ] JWT token validation middleware
- [ ] API key generation & storage
- [ ] User/role model design
- [ ] Dashboard API key management
- [ ] Scope-based permissions

---

## v0.12: Persistence & Storage Backends

**Status**: Planned

**Timeline**: Q3 2026

**Goals**:
- PostgreSQL backend for persistent storage
- Coordinator crash recovery
- Job history and audit log
- S3/GCS support for artifact storage

**Features**:
- Persistent job queue
- Database migrations
- Backup & restore procedures
- Configurable storage backend

**Breaking Changes**: None

---

## v0.13: High Availability

**Status**: Planned

**Timeline**: Q4 2026

**Goals**:
- Multi-coordinator clustering
- Shared state via etcd or Redis
- Leader election
- Failover testing

**Features**:
- Coordinator replication
- Distributed locking
- Health checks & monitoring
- Zero-downtime deployments

---

## v1.0: Production Ready

**Status**: Planned

**Timeline**: Q1 2027

**Goals**:
- Security audit completed
- Comprehensive documentation
- Performance benchmarks published
- Production deployment guide
- SLA guarantees

**Criteria for Release**:
- ✅ >85% test coverage
- ✅ All major features implemented
- ✅ Security hardening complete
- ✅ Performance benchmarks pass
- ✅ Documentation complete
- ✅ Community feedback incorporated

---

## Future Considerations (v1.1+)

### Resource Allocation

- GPU/CPU/memory job requirements
- Capacity-aware scheduling
- Cost estimation and chargeback
- Resource quotas per user

### Advanced Scheduling

- Priority queues
- Job affinity/anti-affinity
- Cost-aware scheduling
- Preemption & job interruption

### Observability

- Distributed tracing (OpenTelemetry)
- Custom metrics
- Log aggregation
- Performance profiling

### Integrations

- Kubernetes job controller
- Airflow/Prefect DAG integration
- GitHub Actions workflow integration
- Webhook notifications

### Scaling

- Horizontal scaling improvements
- Agent pool auto-scaling
- Load balancing optimizations
- Batch processing support

---

## Community Requests

We're tracking feature requests and community feedback. To suggest a feature:

1. Check existing [GitHub Discussions](https://github.com/briton-data/gcon/discussions)
2. Create a new discussion with the `feature-request` label
3. Upvote features you'd like to see prioritized

**Top Requested Features** (from community):
- [ ] Kubernetes integration
- [ ] Job priorities and preemption
- [ ] Webhook notifications
- [ ] Cost tracking and chargeback

---

## How to Contribute to the Roadmap

1. **Review** the roadmap and current priorities
2. **Discuss** features in [GitHub Discussions](https://github.com/briton-data/gcon/discussions)
3. **Propose** your own features or improvements
4. **Vote** on features by upvoting discussions
5. **Contribute** by submitting PRs (see [CONTRIBUTING.md](CONTRIBUTING.md))

---

## Milestone Status

### v0.10 (Current)
- [x] Job scheduling
- [x] Receipt signing
- [x] Workflows
- [x] Web dashboard
- [x] REST API

### v0.11
- [ ] JWT authentication
- [ ] RBAC
- [ ] API key management
- [ ] Rate limiting

### v0.12
- [ ] PostgreSQL backend
- [ ] Crash recovery
- [ ] Artifact storage backends (S3/GCS)

### v0.13
- [ ] Multi-coordinator HA
- [ ] etcd/Redis integration
- [ ] Leader election

### v1.0
- [ ] Security audit
- [ ] Production deployment guide
- [ ] SLA guarantees

---

## Getting Updates

- **Watch** this repository for releases
- **Subscribe** to [GitHub Discussions](https://github.com/briton-data/gcon/discussions) for announcements
- **Read** [CHANGELOG.md](CHANGES.md) for detailed release notes

---

## Questions?

Have a question about the roadmap? Start a discussion or ask in [GitHub Discussions](https://github.com/briton-data/gcon/discussions)!
