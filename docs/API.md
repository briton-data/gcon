# GCON API Reference

## JobRunner

Main interface for executing verified jobs.

### Constructor

```python
JobRunner(agent_id: Optional[str] = None, storage_dir: str = "./receipts")
```

**Parameters:**
- `agent_id` (str, optional): Identifier for this agent. Auto-generated if not provided.
- `storage_dir` (str): Directory to store receipt files. Default: `./receipts`

**Example:**
```python
from run_job import JobRunner

runner = JobRunner(agent_id="provider-1", storage_dir="./my_receipts")
```

### run_job()

Execute a job with full verification pipeline.

```python
run_job(
    job_script: str,
    job_id: Optional[str] = None,
    timeout: Optional[int] = None,
    input_file: Optional[str] = None,
    output_file: Optional[str] = None
) -> Dict[str, Any]
```

**Parameters:**
- `job_script` (str): Path to script or command to execute. Required.
- `job_id` (str, optional): Unique job identifier. Auto-generated if not provided.
- `timeout` (int, optional): Maximum execution time in seconds. No limit if not provided.
- `input_file` (str, optional): Path to input file to hash.
- `output_file` (str, optional): Path to output file to hash.

**Returns:** Dictionary with execution result and receipt.

**Example:**
```python
result = runner.run_job(
    job_script="python train.py",
    job_id="training-001",
    timeout=300,
    input_file="data.csv",
    output_file="model.pkl"
)
```

### get_job_receipt()

Retrieve a receipt by ID.

```python
get_job_receipt(receipt_id: str) -> Optional[Dict[str, Any]]
```

**Parameters:**
- `receipt_id` (str): Receipt identifier. Required.

**Returns:** Receipt dictionary or None if not found.

**Example:**
```python
receipt = runner.get_job_receipt("abc123def456")
if receipt:
    print(f"Job: {receipt['job_id']}, Status: {receipt['status']}")
else:
    print("Receipt not found")
```

### list_job_receipts()

List all receipts, optionally filtered by job ID.

```python
list_job_receipts(job_id: Optional[str] = None) -> List[Dict[str, Any]]
```

**Parameters:**
- `job_id` (str, optional): Filter by job ID.

**Returns:** List of receipt dictionaries.

**Example:**
```python
# List all receipts
all_receipts = runner.list_job_receipts()

# List receipts for specific job
job_receipts = runner.list_job_receipts(job_id="training-001")
```

### print_receipt()

Format and print a receipt.

```python
print_receipt(receipt_id: str, format: str = "summary") -> str
```

**Parameters:**
- `receipt_id` (str): Receipt identifier. Required.
- `format` (str): Output format: "summary", "json", or "csv". Default: "summary"

**Returns:** Formatted receipt string.

**Example:**
```python
print(runner.print_receipt("abc123def456", format="summary"))
print(runner.print_receipt("abc123def456", format="json"))
```

## GCONAgent

Executes jobs and collects metrics.

### Constructor

```python
GCONAgent(job_id: str)
```

**Parameters:**
- `job_id` (str): Unique identifier for this job. Required.

**Example:**
```python
from agent import GCONAgent

agent = GCONAgent("job-001")
```

### detect_gpu()

Detect available GPU hardware.

```python
detect_gpu() -> Dict[str, Any]
```

**Returns:** Dictionary with GPU information.

**Example:**
```python
gpu_info = agent.detect_gpu()
print(f"GPU: {gpu_info['gpu_name']}")
print(f"Memory: {gpu_info['memory_total']}MB")
```

### execute_job()

Execute a job script and monitor execution.

```python
execute_job(
    job_script: str,
    timeout: Optional[int] = None
) -> Dict[str, Any]
```

**Parameters:**
- `job_script` (str): Path to script or command to execute. Required.
- `timeout` (int, optional): Maximum execution time in seconds.

**Returns:** Dictionary with execution results and metrics.

**Example:**
```python
result = agent.execute_job("python train.py", timeout=300)
print(f"Status: {result['status']}")
print(f"Runtime: {result['runtime_seconds']}s")
print(f"Return code: {result['return_code']}")
```

### collect_metrics()

Collect current system metrics.

```python
collect_metrics() -> ExecutionMetrics
```

**Returns:** ExecutionMetrics dataclass with current metrics.

**Example:**
```python
metrics = agent.collect_metrics()
print(f"CPU: {metrics.cpu_percent}%")
print(f"Memory: {metrics.memory_percent}%")
print(f"GPU Memory Used: {metrics.gpu_memory_used}MB")
```

### get_metrics_summary()

Get summary of collected metrics.

```python
get_metrics_summary() -> Dict[str, Any]
```

**Returns:** Summary dictionary with statistics.

**Example:**
```python
summary = agent.get_metrics_summary()
print(f"Samples: {summary['total_samples']}")
print(f"Avg CPU: {summary['avg_cpu_percent']}%")
```

## ExecutionVerifier

Generates cryptographic proofs and validates receipts.

### Constructor

```python
ExecutionVerifier(secret_key: Optional[str] = None)
```

**Parameters:**
- `secret_key` (str, optional): Secret key for HMAC signing. Default: "gcon-default-key"

**Example:**
```python
from verifier import ExecutionVerifier

verifier = ExecutionVerifier("my-secret-key")
```

### hash_data()

Generate cryptographic hash of data.

```python
@staticmethod
hash_data(data: Any, algorithm: str = "sha256") -> str
```

**Parameters:**
- `data` (str or dict): Data to hash.
- `algorithm` (str): Hash algorithm: "sha256" or "sha512". Default: "sha256"

**Returns:** Hex digest of hash.

**Example:**
```python
hash1 = ExecutionVerifier.hash_data("input data")
hash2 = ExecutionVerifier.hash_data({"key": "value"})
print(f"String hash: {hash1}")
print(f"Dict hash: {hash2}")
```

### hash_file()

Generate hash of a file.

```python
@staticmethod
hash_file(
    filepath: str,
    algorithm: str = "sha256",
    chunk_size: int = 65536
) -> str
```

**Parameters:**
- `filepath` (str): Path to file. Required.
- `algorithm` (str): Hash algorithm: "sha256" or "sha512". Default: "sha256"
- `chunk_size` (int): Chunk size for reading large files. Default: 65536

**Returns:** Hex digest of file hash.

**Example:**
```python
file_hash = ExecutionVerifier.hash_file("data.csv")
print(f"File hash: {file_hash}")
```

### sign_data()

Create HMAC signature of data.

```python
sign_data(data: Dict[str, Any]) -> str
```

**Parameters:**
- `data` (dict): Data to sign. Required.

**Returns:** Hex digest of HMAC signature.

**Example:**
```python
data = {"job_id": "job-001", "status": "success"}
signature = verifier.sign_data(data)
print(f"Signature: {signature}")
```

### verify_signature()

Verify HMAC signature of data.

```python
verify_signature(data: Dict[str, Any], signature: str) -> bool
```

**Parameters:**
- `data` (dict): Data to verify. Required.
- `signature` (str): Expected signature. Required.

**Returns:** True if signature is valid, False otherwise.

**Example:**
```python
data = {"job_id": "job-001", "status": "success"}
signature = verifier.sign_data(data)
is_valid = verifier.verify_signature(data, signature)
print(f"Valid: {is_valid}")
```

### generate_execution_proof()

Generate execution proof receipt.

```python
generate_execution_proof(
    job_id: str,
    gpu_name: str,
    runtime: float,
    input_hash: str,
    output_hash: str,
    metrics: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]
```

**Parameters:**
- `job_id` (str): Job identifier. Required.
- `gpu_name` (str): GPU used. Required.
- `runtime` (float): Execution time in seconds. Required.
- `input_hash` (str): Hash of input data. Required.
- `output_hash` (str): Hash of output data. Required.
- `metrics` (dict, optional): Execution metrics.

**Returns:** Proof dictionary with signature.

**Example:**
```python
proof = verifier.generate_execution_proof(
    job_id="job-001",
    gpu_name="RTX 4090",
    runtime=120.5,
    input_hash="abc123...",
    output_hash="def456..."
)
print(f"Verified: {proof['verified']}")
```

### validate_proof()

Validate execution proof.

```python
validate_proof(proof: Dict[str, Any]) -> Tuple[bool, str]
```

**Parameters:**
- `proof` (dict): Proof to validate. Required.

**Returns:** Tuple of (is_valid: bool, message: str)

**Example:**
```python
is_valid, message = verifier.validate_proof(proof)
if is_valid:
    print("Proof is valid!")
else:
    print(f"Proof is invalid: {message}")
```

## ReceiptManager

Manages execution receipts.

### Constructor

```python
ReceiptManager(storage_dir: str = "./receipts")
```

**Parameters:**
- `storage_dir` (str): Directory to store receipts. Default: "./receipts"

**Example:**
```python
from receipt import ReceiptManager

manager = ReceiptManager("./my_receipts")
```

### save_receipt()

Save receipt to storage.

```python
save_receipt(receipt: Dict[str, Any]) -> bool
```

**Parameters:**
- `receipt` (dict): Receipt to save. Required.

**Returns:** True if successful, False otherwise.

### load_receipt()

Load receipt from storage.

```python
load_receipt(receipt_id: str) -> Optional[Dict[str, Any]]
```

**Parameters:**
- `receipt_id` (str): Receipt identifier. Required.

**Returns:** Receipt dictionary or None if not found.

### list_receipts()

List receipts, optionally filtered by job ID.

```python
list_receipts(job_id: Optional[str] = None) -> List[Dict[str, Any]]
```

**Parameters:**
- `job_id` (str, optional): Filter by job ID.

**Returns:** List of receipt dictionaries.

### delete_receipt()

Delete receipt from storage.

```python
delete_receipt(receipt_id: str) -> bool
```

**Parameters:**
- `receipt_id` (str): Receipt identifier. Required.

**Returns:** True if successful, False otherwise.

## ReceiptFormatter

Formats receipts for display and export.

### to_json_string()

Convert receipt to JSON string.

```python
@staticmethod
to_json_string(receipt: Dict[str, Any], pretty: bool = True) -> str
```

**Parameters:**
- `receipt` (dict): Receipt to convert. Required.
- `pretty` (bool): Use pretty printing. Default: True

**Returns:** JSON string representation.

### to_summary()

Create human-readable summary.

```python
@staticmethod
to_summary(receipt: Dict[str, Any]) -> str
```

**Parameters:**
- `receipt` (dict): Receipt to format. Required.

**Returns:** Formatted summary string.

### to_csv()

Convert receipts to CSV format.

```python
@staticmethod
to_csv(receipts: List[Dict[str, Any]]) -> str
```

**Parameters:**
- `receipts` (list): List of receipts. Required.

**Returns:** CSV formatted string.
