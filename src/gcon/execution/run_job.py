"""
GCON Job Runner - High-level interface for executing verified workloads.

Orchestrates:
1. Agent execution
2. Hash collection
3. Verification
4. Receipt generation
"""

import os
import sys
import json
import logging
from typing import Dict, Any, Optional
from uuid import uuid4

from agent import GCONAgent
from verifier import ExecutionVerifier
from receipt import ReceiptManager, ReceiptFormatter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobRunner:
    """High-level interface for running verified jobs."""
    
    def __init__(self, agent_id: Optional[str] = None, storage_dir: str = "./receipts"):
        """
        Initialize JobRunner.
        
        Args:
            agent_id: Identifier for this agent
            storage_dir: Directory for receipt storage
        """
        self.agent_id = agent_id or str(uuid4())[:8]
        self.verifier = ExecutionVerifier()
        self.receipt_manager = ReceiptManager(storage_dir)
        logger.info(f"JobRunner initialized (agent_id: {self.agent_id})")
    
    def run_job(
        self,
        job_script: str,
        job_id: Optional[str] = None,
        timeout: Optional[int] = None,
        input_file: Optional[str] = None,
        output_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run a job with full verification.
        
        Args:
            job_script: Path to script or command to execute
            job_id: Optional job identifier
            timeout: Optional execution timeout
            input_file: Optional input file to hash
            output_file: Optional output file to hash
            
        Returns:
            Dictionary with execution result and receipt
        """
        job_id = job_id or str(uuid4())[:16]
        logger.info(f"Starting job: {job_id}")
        
        # Calculate input hash
        input_hash = ""
        if input_file and os.path.exists(input_file):
            input_hash = self.verifier.hash_file(input_file)
            logger.info(f"Input hash: {input_hash}")
        
        # Create and execute agent
        agent = GCONAgent("local-node")
        execution_result = agent.execute_job(
            job_id,
            job_script,
            timeout
)
        
        # Calculate output hash
        output_hash = ""
        if output_file and os.path.exists(output_file):
            output_hash = self.verifier.hash_file(output_file)
            logger.info(f"Output hash: {output_hash}")
        elif execution_result.get("status") == "success":
            # Hash stdout if available
            stdout = execution_result.get("stdout", "")
            output_hash = self.verifier.hash_data(stdout)
            logger.info(f"Output hash (from stdout): {output_hash}")
        
        # Generate receipt
        receipt = self.receipt_manager.create_receipt(
            job_id=job_id,
            agent_id=self.agent_id,
            execution_result=execution_result,
            input_hash=input_hash,
            output_hash=output_hash
        ) if hasattr(self.receipt_manager, 'create_receipt') else self._create_receipt_fallback(
            job_id, execution_result, input_hash, output_hash
        )
        
        # Save receipt
        self.receipt_manager.save_receipt(receipt)
        
        # Compile final result
        result = {
            "job_id": job_id,
            "agent_id": self.agent_id,
            "execution": execution_result,
            "receipt": receipt,
            "verification": {
                "input_hash": input_hash,
                "output_hash": output_hash,
                "proof_valid": receipt.get("proof", {}).get("verified", False)
            }
        }
        
        logger.info(f"Job completed: {job_id}")
        return result
    
    def _create_receipt_fallback(
        self,
        job_id: str,
        execution_result: Dict[str, Any],
        input_hash: str,
        output_hash: str
    ) -> Dict[str, Any]:
        """Fallback receipt creation if manager doesn't have the method."""
        from datetime import datetime, UTC
        
        proof = self.verifier.generate_execution_proof(
            job_id=job_id,
            gpu_name=execution_result.get("metrics", {}).get("gpu_name", "Unknown"),
            runtime=execution_result.get("runtime_seconds", 0),
            input_hash=input_hash,
            output_hash=output_hash,
            metrics=execution_result.get("metrics", {})
        )
        
        return {
            "receipt_id": self.verifier.hash_data(f"{job_id}-{datetime.now(UTC).isoformat()}")[:16],
            "job_id": job_id,
            "agent_id": self.agent_id,
            "status": execution_result.get("status", "unknown"),
            "input_hash": input_hash,
            "output_hash": output_hash,
            "proof": proof,
            "issued_at": datetime.now(UTC).isoformat()
        }
    
    def get_job_receipt(self, receipt_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a job receipt.
        
        Args:
            receipt_id: Receipt identifier
            
        Returns:
            Receipt dictionary or None
        """
        return self.receipt_manager.load_receipt(receipt_id)
    
    def list_job_receipts(self, job_id: Optional[str] = None) -> list:
        """
        List all receipts for a job.
        
        Args:
            job_id: Optional job identifier to filter by
            
        Returns:
            List of receipt dictionaries
        """
        return self.receipt_manager.list_receipts(job_id)
    
    def print_receipt(self, receipt_id: str, format: str = "summary") -> str:
        """
        Print a receipt in specified format.
        
        Args:
            receipt_id: Receipt identifier
            format: Output format ("summary", "json", or "csv")
            
        Returns:
            Formatted receipt string
        """
        receipt = self.get_job_receipt(receipt_id)
        if not receipt:
            return f"Receipt not found: {receipt_id}"
        
        formatter = ReceiptFormatter()
        
        if format == "summary":
            return formatter.to_summary(receipt)
        elif format == "json":
            return formatter.to_json_string(receipt, pretty=True)
        else:
            return formatter.to_json_string(receipt)


def main():
    """CLI entry point for GCON job runner."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="GCON - Verified GPU Compute Network"
    )
    parser.add_argument("script", help="Script or command to execute")
    parser.add_argument("--job-id", help="Job identifier")
    parser.add_argument("--timeout", type=int, help="Execution timeout in seconds")
    parser.add_argument("--input", help="Input file to hash")
    parser.add_argument("--output", help="Output file to hash")
    parser.add_argument("--agent-id", help="Agent identifier")
    
    args = parser.parse_args()
    
    runner = JobRunner(agent_id=args.agent_id)
    result = runner.run_job(
        job_script=args.script,
        job_id=args.job_id,
        timeout=args.timeout,
        input_file=args.input,
        output_file=args.output
    )
    
    print("\n" + "="*60)
    print("EXECUTION RESULT")
    print("="*60)
    print(json.dumps(result, indent=2, default=str))
    print("\n" + runner.print_receipt(result["receipt"]["receipt_id"], format="summary"))


if __name__ == "__main__":
    main()
