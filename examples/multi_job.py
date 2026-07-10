#!/usr/bin/env python3
"""
Multi-job example: Run multiple jobs and compare receipts.
"""

from run_job import JobRunner
import json


def main():
    print("\n" + "="*60)
    print("GCON Multi-Job Example")
    print("="*60 + "\n")
    
    runner = JobRunner(agent_id="multi-job-agent")
    results = []
    
    jobs = [
        {
            "id": "job-001",
            "cmd": "python -c 'import time; time.sleep(1); print(\"Job 1 done\")'" 
        },
        {
            "id": "job-002",
            "cmd": "python -c 'import time; time.sleep(2); print(\"Job 2 done\")'" 
        },
        {
            "id": "job-003",
            "cmd": "python -c 'import time; time.sleep(1); print(\"Job 3 done\")'" 
        }
    ]
    
    print(f"Running {len(jobs)} jobs...\n")
    
    for job in jobs:
        print(f"Submitting {job['id']}...")
        result = runner.run_job(
            job_script=job['cmd'],
            job_id=job['id'],
            timeout=10
        )
        results.append(result)
        print(f"  Status: {result['execution']['status']}")
        print(f"  Receipt: {result['receipt']['receipt_id']}\n")
    
    # Display summary
    print("\n" + "="*60)
    print("EXECUTION SUMMARY")
    print("="*60 + "\n")
    
    total_runtime = 0
    for result in results:
        job_id = result['job_id']
        status = result['execution']['status']
        runtime = result['execution']['runtime_seconds']
        verified = result['receipt']['proof']['verified']
        total_runtime += runtime
        
        status_emoji = "✅" if status == "success" else "❌"
        verified_emoji = "✓" if verified else "✗"
        
        print(f"{status_emoji} {job_id}: {runtime:.2f}s [{verified_emoji} Verified]")
    
    print(f"\nTotal runtime: {total_runtime:.2f}s")
    print(f"Average per job: {total_runtime/len(results):.2f}s")
    
    # List all receipts
    print("\n" + "="*60)
    print("ALL RECEIPTS")
    print("="*60 + "\n")
    
    receipts = runner.list_job_receipts()
    print(f"Total receipts stored: {len(receipts)}\n")
    
    for receipt in receipts[-3:]:  # Show last 3
        print(f"Receipt ID: {receipt['receipt_id']}")
        print(f"  Job ID: {receipt['job_id']}")
        print(f"  Status: {receipt['status']}")
        print(f"  GPU: {receipt['proof']['gpu']}")
        print(f"  Runtime: {receipt['proof']['runtime_seconds']:.2f}s\n")
    
    # Save results
    with open("/tmp/multi_job_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nFull results saved to: /tmp/multi_job_results.json")


if __name__ == "__main__":
    main()
