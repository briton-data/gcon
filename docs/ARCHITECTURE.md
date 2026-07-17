# GCON Architecture

## System Overview

GCON (Decentralized Verified GPU Compute Network) is designed to enable verifiable execution of AI workloads on distributed GPU resources.

```
┌─────────────────────────────────────────────────────────────────┐
│                        Customer/Client                           │
│                  (Submits AI Workload)                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Job Queue / Scheduler                       │
│              (Matches resources to workload)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GCON Agent (Provider)                         │
│                  (GPU Provider's Machine)                        │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 1. Workload Executor                                     │   │
│  │    - Detects GPU hardware                                │   │
│  │    - Executes job in sandbox                             │   │
│  │    - Captures stdout/stderr                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 2. Metrics Collector                                     │   │
│  │    - GPU utilization                                     │   │
│  │    - CPU usage                                           │   │
│  │    - Memory consumption                                  │   │
│  │    - Execution time                                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 3. Verification Engine                                   │   │
│  │    - Hash inputs (SHA256)                                │   │
│  │    - Hash outputs (SHA256)                               │   │
│  │    - Generate HMAC signatures                            │   │
│  │    - Create execution proofs                             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 4. Receipt Generator                                     │   │
│  │    - Issue signed receipt                                │   │
│  │    - Store proof of work                                 │   │
│  │    - Return verification package                         │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Execution Receipt                             │
│                                                                   │
│  {
    "receipt_id": "abc123",
    "job_id": "job-001",
    "status": "success",
    "input_hash": "9f86d0...",
    "output_hash": "a665a4...",
    "proof": {
      "gpu": "RTX 4090",
      "runtime_seconds": 120.5,
      "signature": "9e7d3c...",
      "verified": true
    }
  }
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Agent (`agent.py`)

**Responsibility:** Execute workloads and collect metrics

**Key Methods:**
- `detect_gpu()`: Identify GPU hardware
- `execute_job()`: Run workload and capture output
- `collect_metrics()`: Record system metrics during execution

**Data Flow:**
```
Job Script → Subprocess Execute → Metrics Collection → Results
```

### 2. Verifier (`verifier.py`)

**Responsibility:** Generate cryptographic proofs and validate receipts

**Key Methods:**
- `hash_data()`: Generate SHA256/SHA512 hash
- `hash_file()`: Hash file contents
- `sign_data()`: Create HMAC signature
- `verify_signature()`: Validate signatures
- `generate_execution_proof()`: Create signed proof
- `validate_proof()`: Verify proof authenticity

**Cryptographic Approach:**
```
Input Data
    ↓
[SHA256] → Input Hash
    ↓
[HMAC-SHA256 with secret] → Signature
    ↓
Verified Proof Package
```

### 3. Receipt Manager (`receipt.py`)

**Responsibility:** Store and manage execution receipts

**Key Methods:**
- `save_receipt()`: Persist receipt to JSON
- `load_receipt()`: Retrieve receipt from storage
- `list_receipts()`: Query receipts (with filtering)
- `delete_receipt()`: Remove receipt

**Storage Format:**
```
receipts/
├── abc123def456.json
├── xyz789uvw456.json
└── ...
```

### 4. Job Runner (`run_job.py`)

**Responsibility:** Orchestrate end-to-end job execution

**Key Methods:**
- `run_job()`: Execute job with full verification pipeline
- `get_job_receipt()`: Retrieve receipt
- `list_job_receipts()`: List all receipts
- `print_receipt()`: Format receipt for display

**Execution Pipeline:**
```
1. Calculate Input Hash
         ↓
2. Execute Job (Agent)
         ↓
3. Collect Metrics
         ↓
4. Calculate Output Hash
         ↓
5. Generate Proof (Verifier)
         ↓
6. Create Receipt
         ↓
7. Store Receipt (Manager)
         ↓
8. Return Complete Result
```

## Data Structures

### ExecutionMetrics
```python
@dataclass
class ExecutionMetrics:
    job_id: str
    gpu_name: str
    gpu_memory_total: int
    gpu_memory_used: int
    cpu_percent: float
    memory_percent: float
    runtime_seconds: float
    timestamp: str
```

### Execution Receipt
```json
{
  "receipt_id": "unique_id",
  "job_id": "job_identifier",
  "agent_id": "agent_identifier",
  "status": "success|failed|error|timeout",
  "input_hash": "sha256_hash",
  "output_hash": "sha256_hash",
  "proof": {
    "job_id": "job_identifier",
    "gpu": "GPU_name",
    "runtime_seconds": 120.5,
    "input_hash": "sha256_hash",
    "output_hash": "sha256_hash",
    "timestamp": "ISO_8601_timestamp",
    "signature": "HMAC_signature",
    "verified": true
  },
  "issued_at": "ISO_8601_timestamp"
}
```

## Security Model

### Threat Model

1. **Provider Dishonesty**
   - Provider claims to have run job but didn't
   - Provider runs on cheaper hardware than advertised
   - Provider returns fraudulent results

2. **Customer Verification**
   - Proof that job ran with claimed metrics
   - Proof that hardware matched specifications
   - Proof of output integrity

### Security Mechanisms

1. **Input/Output Hashing**
   - SHA256 hashing of all inputs
   - SHA256 hashing of all outputs
   - Enables customer to verify output wasn't modified

2. **HMAC Signatures**
   - HMAC-SHA256 of proof data
   - Uses provider's secret key
   - Enables signature verification

3. **Hardware Attestation** (Future)
   - GPU hardware identification
   - Hardware capability verification
   - Attestation from trusted hardware sources

4. **Timestamp Validation**
   - Receipts must be recent (< 24 hours)
   - Prevents replay attacks
   - Enables temporal verification

## Execution Flow - Detailed

### Step 1: Job Submission
```
Customer: gcon submit train.py --gpu RTX-4090
```

### Step 2: Provider Agent Setup
```python
agent = GCONAgent(job_id)
gpu_info = agent.detect_gpu()
assert gpu_info['gpu_name'] == 'RTX 4090'
```

### Step 3: Job Execution with Monitoring
```python
result = agent.execute_job("python train.py")
# Agent records:
# - Execution time
# - GPU utilization
# - Memory usage
# - CPU load
# - Output hash
```

### Step 4: Proof Generation
```python
verifier = ExecutionVerifier(secret_key)
proof = verifier.generate_execution_proof(
    job_id=job_id,
    gpu_name=gpu_info['gpu_name'],
    runtime=result['runtime_seconds'],
    input_hash=input_hash,
    output_hash=output_hash,
    metrics=result['metrics']
)
# Verifier signs proof with HMAC
```

### Step 5: Receipt Issuance
```python
receipt = receipt_manager.create_receipt(
    job_id=job_id,
    execution_result=result,
    proof=proof
)
receipt_manager.save_receipt(receipt)
```

### Step 6: Receipt Return to Customer
```json
{
  "verified": true,
  "proof": {...},
  "timestamp": "2024-01-15T10:30:45Z"
}
```

## Scalability Considerations

### Current (MVP)
- Single agent per GPU node
- Local receipt storage (JSON)
- Direct verification

### Phase 2 (Network)
- Distributed scheduler
- Provider registry
- Network-level verification

### Phase 3 (Decentralized)
- Blockchain-based receipt anchoring
- Decentralized verification
- Smart contracts for dispute resolution

## Future Enhancements

1. **Container Support**
   - Docker/Singularity support
   - Reproducible execution environments

2. **Advanced Verification**
   - Zero-knowledge proofs
   - Hardware attestation (TPM/SGX)
   - Trusted execution environments

3. **API & Monitoring**
   - REST API
   - WebSocket real-time monitoring
   - Dashboard UI

4. **Performance**
   - Multi-GPU support
   - Distributed training
   - Result caching

5. **Economics**
   - Token system
   - Reputation scoring
   - Dispute resolution
