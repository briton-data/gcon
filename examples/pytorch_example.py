#!/usr/bin/env python3
"""
PyTorch example: Train a simple neural network with GCON verification.

This example requires PyTorch to be installed:
    pip install torch torchvision
"""

from run_job import JobRunner
import json
import os


def main():
    print("\n" + "="*60)
    print("GCON PyTorch Training Example")
    print("="*60 + "\n")
    
    # Create a simple PyTorch training script
    script_content = '''
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

print(f"PyTorch version: {torch.__version__}")
print(f"GPU available: {torch.cuda.is_available()}")

# Generate dummy data
X = torch.randn(1000, 10)
y = torch.randn(1000, 1)
dataset = TensorDataset(X, y)
dataloader = DataLoader(dataset, batch_size=32)

# Define model
model = nn.Sequential(
    nn.Linear(10, 64),
    nn.ReLU(),
    nn.Linear(64, 32),
    nn.ReLU(),
    nn.Linear(32, 1)
)

# Training loop
optimizer = optim.SGD(model.parameters(), lr=0.01)
loss_fn = nn.MSELoss()

print("Training...")
for epoch in range(5):
    epoch_loss = 0
    for batch_x, batch_y in dataloader:
        optimizer.zero_grad()
        predictions = model(batch_x)
        loss = loss_fn(predictions, batch_y)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item()
    
    avg_loss = epoch_loss / len(dataloader)
    print(f"Epoch {epoch+1}/5 - Loss: {avg_loss:.4f}")

print("Training complete!")
torch.save(model.state_dict(), "/tmp/model.pth")
print("Model saved to /tmp/model.pth")
'''
    
    # Write script to file
    script_path = "/tmp/pytorch_train.py"
    with open(script_path, "w") as f:
        f.write(script_content)
    
    # Run the training job with verification
    runner = JobRunner(agent_id="pytorch-agent")
    
    print("Submitting PyTorch training job...")
    result = runner.run_job(
        job_script=f"python {script_path}",
        job_id="pytorch-training-001",
        timeout=300,
        output_file="/tmp/model.pth" if os.path.exists("/tmp/model.pth") else None
    )
    
    # Display results
    print("\n" + "="*60)
    print("TRAINING RESULT")
    print("="*60 + "\n")
    
    print(f"Job ID: {result['job_id']}")
    print(f"Status: {result['execution']['status']}")
    print(f"Runtime: {result['execution']['runtime_seconds']:.2f}s")
    print(f"GPU: {result['execution']['metrics']['gpu_name']}")
    print(f"Verified: {result['receipt']['proof']['verified']}")
    
    # Display receipt
    print("\n" + runner.print_receipt(
        result['receipt']['receipt_id'],
        format="summary"
    ))
    
    # Save result
    with open("/tmp/pytorch_result.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    
    print(f"\nFull result saved to: /tmp/pytorch_result.json")
    print("\nOutput from training:")
    print("-" * 60)
    print(result['execution']['stdout'])


if __name__ == "__main__":
    main()
