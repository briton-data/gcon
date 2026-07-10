# GCON Quick Start Guide

## Installation

### Prerequisites
- Python 3.8+
- pip
- GPU (optional, but recommended)

### Install GCON

```bash
# Clone repository
git clone https://github.com/Jug-data/GCON.git
cd GCON

# Install dependencies
pip install -r requirements.txt
```

## First Run

### 1. Simple Echo Job

```bash
python run_job.py "echo 'Hello GCON'" --job-id hello-world-001
```

**Output:**
```
============================================================
EXECUTION RESULT
============================================================

Job ID: hello-world-001
Status: success
Runtime: 0.05s
GPU: Unknown GPU
Verified: True

╔════════════════════════════════════════════════════════════╗
║                  GCON EXECUTION RECEIPT                    ║
╠════════════════════════════════════════════════════════════╣
║ Receipt ID:      abc123def456                              ║
║ Job ID:          hello-world-001                           ║
║ Status:          success                                   ║
║ Issued At:       2024-01-15T10:30:45.123456               ║
╠════════════════════════════════════════════════════════════╣
║ Input Hash:      9f86d081884c7d6d9ffd60014fc7ee77e2b6 ║
║ Output Hash:     a665a45920422f9d417e4867efdc4fb8a04a ║
╠════════════════════════════════════════════════════════════╣
║ GPU:             Unknown GPU                               ║
║ Runtime:         0.05s                                     ║
║ Verified:        True                                      ║
╚════════════════════════════════════════════════════════════╝
```

### 2. Run Python Script

Create `compute.py`:
```python
import time

print("Starting computation...")
time.sleep(2)
result = sum(range(1000000))
print(f"Result: {result}")
```

Run it:
```bash
python run_job.py "python compute.py" \
  --job-id compute-001 \
  --timeout 10 \
  --output compute-output.txt
```

### 3. Python API

```python
from run_job import JobRunner

# Create runner
runner = JobRunner(agent_id="my-agent")

# Execute job
result = runner.run_job(
    job_script="python train.py",
    job_id="training-001",
    timeout=300,
    input_file="data.csv",
    output_file="model.pkl"
)

# Get receipt
receipt_id = result['receipt']['receipt_id']
receipt = runner.get_job_receipt(receipt_id)

# Print receipt
print(runner.print_receipt(receipt_id, format="summary"))
```

## Working with Receipts

### List All Receipts

```python
from run_job import JobRunner

runner = JobRunner()
receipts = runner.list_job_receipts()

for receipt in receipts:
    print(f"Job: {receipt['job_id']}, Status: {receipt['status']}")
```

### Filter by Job ID

```python
receipts = runner.list_job_receipts(job_id="training-001")
```

### Export Receipt as JSON

```python
from receipt import ReceiptFormatter

formatter = ReceiptFormatter()
json_str = formatter.to_json_string(receipt, pretty=True)
print(json_str)
```

### Export Receipts as CSV

```python
from receipt import ReceiptFormatter

receipts = runner.list_job_receipts()
csv_str = ReceiptFormatter.to_csv(receipts)
print(csv_str)
```

## Examples

Run the included examples:

### Simple Computation
```bash
python examples/simple_job.py
```

### PyTorch Training (requires PyTorch)
```bash
pip install torch torchvision
python examples/pytorch_example.py
```

### Multiple Jobs
```bash
python examples/multi_job.py
```

## Testing

Run the test suite:

```bash
# Using unittest
python -m unittest discover tests/ -v

# Using pytest (if installed)
pytest tests/ -v
```

## Troubleshooting

### GPU Not Detected

If GPU detection fails, check:

```python
from agent import GCONAgent

agent = GCONAgent("test-job")
gpu_info = agent.detect_gpu()
print(gpu_info)
```

If using fallback GPU detection, install GPUtil:
```bash
pip install GPUtil
```

### Job Timeout

Increase timeout:
```bash
python run_job.py "python slow_script.py" --timeout 600
```

### Receipt Not Found

Check receipt directory:
```bash
ls -la receipts/
```

Receipts are stored as JSON files in `./receipts/` by default.

## Next Steps

1. **Explore the API**: Check `run_job.py` for full API documentation
2. **Read Architecture**: See `docs/ARCHITECTURE.md` for system design
3. **Run Examples**: Try the examples in `examples/` directory
4. **Contribute**: Help improve GCON!

## Common Commands

```bash
# Run simple job
python run_job.py "python script.py" --job-id job-001

# Run with timeout
python run_job.py "python script.py" --timeout 300

# Run with input/output files
python run_job.py "python script.py" --input data.csv --output result.pkl

# Run with custom agent ID
python run_job.py "python script.py" --agent-id my-agent-1
```

## Getting Help

- Check `docs/` directory for detailed documentation
- Review example scripts in `examples/`
- Run tests to verify installation: `python -m unittest discover tests/`
- Open an issue on GitHub: https://github.com/Jug-data/GCON/issues
