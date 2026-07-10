#!/usr/bin/env python3
"""
Simple example: Run a basic Python script with GCON verification.
"""

from run_job import JobRunner
import json


def main():
    print("\n" + "="*60)
    print("GCON Simple Job Example")
    print("="*60 + "\n")
    
    # Create a simple Python script to execute
    script_content = '''
import time
import json

print("Starting computation...")
time.sleep(2)
result = sum(range(1000000))
print(f"Result: {result}")
print("Computation complete!")
'''
    
    # Write script to file
    with open("/tmp/simple_compute.py", "w") as f:
        f.write(script_content)
    
    # Run the job with verification
    runner = JobRunner(agent_id="example-agent-1")
    
    print("Submitting job...")
    result = runner.run_job(
        job_script="python /tmp/simple_compute.py",
        job_id="example-job-001",
        timeout=10
    )
    
    # Display results
    print("\n" + "="*60)
    print("EXECUTION RESULT")
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
    
    # Save full result
    with open("/tmp/gcon_result.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    
    print(f"\nFull result saved to: /tmp/gcon_result.json")


if __name__ == "__main__":
    main()
