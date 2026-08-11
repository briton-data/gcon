# GCON Quick Start Guide

This guide covers the standalone `JobRunner` path (single machine, no
coordinator/agents needed) — the fastest way to see a signed receipt.
For the full multi-node cluster (coordinator + agents + dashboard), see
[QUICKSTART in the main README](../README.md#quickstart) and
[DEPLOYMENT.md](DEPLOYMENT.md).

Rebuilt against the actual code in `src/gcon/execution/run_job.py`,
`agent.py`, and `receipt.py` — the old version of this guide referenced a
`run_job.py`/`agent.py`/`receipt.py` at the repo root; those live under
`src/gcon/execution/` and are used as `gcon.execution.<module>`.

## Installation

### Prerequisites
- Python 3.8+ (`pyproject.toml` sets `requires-python = ">=3.8"`)
- pip
- GPU optional — `detect_gpu()` falls back to CPU-only info if none is
  found or `GPUtil` isn't installed

### Install GCON

```bash
git clone https://github.com/briton-data/GCON.git
cd GCON
pip install -r requirements.txt
```

## First Run

### 1. Simple echo job (CLI)

The CLI entry point lives inside the package, so it's invoked with
`python -m`, not as a standalone script:

```bash
python -m gcon.execution.run_job "echo 'Hello GCON'" --job-id hello-world-001
```

This prints the full JSON result, then a formatted receipt summary, e.g.:

```
============================================================
EXECUTION RESULT
============================================================
{
  "job_id": "hello-world-001",
  "agent_id": "...",
  "execution": { "status": "success", "runtime_seconds": 0.02, ... },
  "receipt": { "receipt_id": "...", "status": "success", ... },
  "verification": { "input_hash": "", "output_hash": "...", "proof_valid": true }
}

╔════════════════════════════════════════════════════════════╗
║                  GCON EXECUTION RECEIPT                    ║
╠════════════════════════════════════════════════════════════╣
║ Receipt ID:      <receipt id>                              ║
║ Job ID:          hello-world-001                           ║
║ Status:          success                                   ║
╚════════════════════════════════════════════════════════════╝
```
(Exact fields come from `ReceiptFormatter.to_summary()` — run it yourself
to see the current format rather than trusting a hardcoded example here.)

Available CLI flags (`run_job.main()`): `script` (positional),
`--job-id`, `--timeout`, `--input`, `--output`, `--agent-id`.

### 2. Run a Python script

```bash
cat > compute.py <<'EOF'
import time
print("Starting computation...")
time.sleep(2)
result = sum(range(1000000))
print(f"Result: {result}")
EOF

python -m gcon.execution.run_job "python compute.py" \
  --job-id compute-001 \
  --timeout 10 \
  --output compute-output.txt
```

### 3. Python API

```python
from gcon.execution.run_job import JobRunner

runner = JobRunner(agent_id="my-agent")

result = runner.run_job(
    job_script="python train.py",
    job_id="training-001",
    timeout=300,
    input_file="data.csv",
    output_file="model.pkl",
)

receipt_id = result["receipt"]["receipt_id"]
receipt = runner.get_job_receipt(receipt_id)
print(runner.print_receipt(receipt_id, format="summary"))
```

`JobRunner(agent_id=None, storage_dir="./receipts")` — `storage_dir` is
where `ReceiptManager` persists receipts as JSON; it defaults to
`./receipts` relative to wherever you run the process from, same as
before.

## Working with Receipts

### List all receipts

```python
from gcon.execution.run_job import JobRunner

runner = JobRunner()
receipts = runner.list_job_receipts()

for receipt in receipts:
    print(f"Job: {receipt['job_id']}, Status: {receipt['status']}")
```

### Filter by job ID

```python
receipts = runner.list_job_receipts(job_id="training-001")
```

### Export a receipt as JSON

```python
from gcon.execution.receipt import ReceiptFormatter

formatter = ReceiptFormatter()
json_str = formatter.to_json_string(receipt, pretty=True)
print(json_str)
```

### Export receipts as CSV

```python
from gcon.execution.receipt import ReceiptFormatter

receipts = runner.list_job_receipts()
csv_str = ReceiptFormatter.to_csv(receipts)
print(csv_str)
```

## Examples

The scripts in [`examples/`](../examples/) all import from the real
package path (`gcon.execution.run_job`), so they run as-is:

```bash
# Simple computation
python examples/simple_job.py

# PyTorch training (requires PyTorch)
pip install torch torchvision
python examples/pytorch_example.py

# Multiple jobs
python examples/multi_job.py
```

## Testing

```bash
pytest tests/ -v
```
(`pytest` is the test runner actually used by this repo — see
`pyproject.toml` and `tests/`. Plain `unittest discover` will also pick up
the suite, but pytest is what CI and the coverage config target.)

## Troubleshooting

### GPU not detected

```python
from gcon.execution.agent import GCONAgent

agent = GCONAgent("test-job")
gpu_info = agent.detect_gpu()
print(gpu_info)
```

`detect_gpu()` tries `GPUtil` first and falls back to a CPU-only info dict
if `GPUtil` isn't installed or finds nothing:

```bash
pip install GPUtil
```

### Job timeout

```bash
python -m gcon.execution.run_job "python slow_script.py" --timeout 600
```

### Receipt not found

Check the receipt directory (default `./receipts/`, or whatever
`storage_dir` you passed to `JobRunner`):

```bash
ls -la receipts/
```

## Next Steps

1. **Explore the full cluster**: coordinator + agents + dashboard — see
   the [main README](../README.md) and [ARCHITECTURE.md](ARCHITECTURE.md)
2. **Call the public API**: see [API.md](API.md) for `/api/v1` and the
   `gcon_sdk` Python client
3. **Run the examples**: [`examples/`](../examples/)
4. **Contribute**: see [`CONTRIBUTING.md`](../CONTRIBUTING.md)

## Common Commands

```bash
# Run a simple job
python -m gcon.execution.run_job "python script.py" --job-id job-001

# Run with a timeout
python -m gcon.execution.run_job "python script.py" --timeout 300

# Run with input/output files
python -m gcon.execution.run_job "python script.py" --input data.csv --output result.pkl

# Run with a custom agent ID
python -m gcon.execution.run_job "python script.py" --agent-id my-agent-1
```

## Getting Help

- Check the [`docs/`](.) directory for the rest of the documentation
- Review example scripts in [`examples/`](../examples/)
- Run the tests to verify your install: `pytest tests/ -v`
- Open an issue: https://github.com/briton-data/GCON/issues
