"""
GCON Verifier - Cryptographic verification and proof generation.

The verifier:
1. Hashes inputs and outputs
2. Creates cryptographic signatures
3. Validates execution receipts
4. Generates proof of work
"""

import hashlib
import json
import hmac
from datetime import datetime, UTC
from typing import Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class ExecutionVerifier:
    """Verifies execution and generates cryptographic proofs."""
    
    def __init__(self, secret_key: Optional[str] = None):
        """
        Initialize ExecutionVerifier.
        
        Args:
            secret_key: Secret key for HMAC signing (optional)
        """
        self.secret_key = secret_key or "gcon-default-key"
        logger.info("ExecutionVerifier initialized")
    
    @staticmethod
    def hash_data(data: Any, algorithm: str = "sha256") -> str:
        """
        Generate cryptographic hash of data.
        
        Args:
            data: Data to hash (string or dict)
            algorithm: Hash algorithm to use
            
        Returns:
            Hex digest of hash
        """
        if isinstance(data, dict):
            data = json.dumps(data, sort_keys=True)
        
        if isinstance(data, str):
            data = data.encode()
        
        if algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif algorithm == "sha512":
            return hashlib.sha512(data).hexdigest()
        else:
            return hashlib.sha256(data).hexdigest()
    
    @staticmethod
    def hash_file(filepath: str, algorithm: str = "sha256", chunk_size: int = 65536) -> str:
        """
        Generate hash of a file.
        
        Args:
            filepath: Path to file to hash
            algorithm: Hash algorithm to use
            chunk_size: Chunk size for reading large files
            
        Returns:
            Hex digest of file hash
        """
        if algorithm == "sha256":
            hasher = hashlib.sha256()
        elif algorithm == "sha512":
            hasher = hashlib.sha512()
        else:
            hasher = hashlib.sha256()
        
        try:
            with open(filepath, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            return ""
    
    def sign_data(self, data: Dict[str, Any]) -> str:
        """
        Create HMAC signature of data.
        
        Args:
            data: Data to sign
            
        Returns:
            Hex digest of HMAC signature
        """
        message = json.dumps(data, sort_keys=True).encode()
        signature = hmac.new(
            self.secret_key.encode(),
            message,
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def verify_signature(self, data: Dict[str, Any], signature: str) -> bool:
        """
        Verify HMAC signature of data.
        
        Args:
            data: Data to verify
            signature: Expected signature
            
        Returns:
            True if signature is valid
        """
        computed_signature = self.sign_data(data)
        return hmac.compare_digest(computed_signature, signature)
    
    def generate_execution_proof(
        self,
        job_id: str,
        gpu_name: str,
        runtime: float,
        input_hash: str,
        output_hash: str,
        metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate execution proof receipt.
        
        Args:
            job_id: Unique job identifier
            gpu_name: GPU used for execution
            runtime: Execution time in seconds
            input_hash: Hash of input data
            output_hash: Hash of output data
            metrics: Execution metrics
            
        Returns:
            Proof dictionary with signature
        """
        proof_data = {
            "job_id": job_id,
            "gpu": gpu_name,
            "runtime_seconds": runtime,
            "input_hash": input_hash,
            "output_hash": output_hash,
            "timestamp": datetime.now(UTC).isoformat(),
            "metrics": metrics or {}
        }
        
        signature = self.sign_data(proof_data)
        
        proof = {
            **proof_data,
            "signature": signature,
            "verified": True
        }
        
        logger.info(f"Execution proof generated for job {job_id}")
        return proof
    
    def validate_proof(self, proof: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate execution proof.
        
        Args:
            proof: Proof dictionary to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        if "signature" not in proof:
            return False, "Proof missing signature"
        
        signature = proof["signature"]
        proof_copy = {
    k: v
    for k, v in proof.items()
    if k not in ("signature", "verified")
}
        
        if not self.verify_signature(proof_copy, signature):
            return False, "Invalid signature"
        
        # Check timestamp is recent (within 24 hours)
        try:
            timestamp = datetime.fromisoformat(proof.get("timestamp", ""))
            now = datetime.now(UTC)
            diff = (now - timestamp).total_seconds()
            if diff > 86400:  # 24 hours
                return False, "Proof timestamp is too old"
        except (ValueError, TypeError):
            return False, "Invalid timestamp format"
        
        return True, "Proof is valid"
    
    def create_receipt(
        self,
        job_id: str,
        agent_id: str,
        execution_result: Dict[str, Any],
        input_hash: str,
        output_hash: str
    ) -> Dict[str, Any]:
        """
        Create a complete execution receipt.
        
        Args:
            job_id: Job identifier
            agent_id: Agent identifier
            execution_result: Result from agent execution
            input_hash: Hash of input
            output_hash: Hash of output
            
        Returns:
            Complete receipt with proof
        """
        receipt = {
            "receipt_id": self.hash_data(f"{job_id}-{datetime.now(UTC).isoformat()}")[:16],
            "job_id": job_id,
            "agent_id": agent_id,
            "status": execution_result.get("status", "unknown"),
            "input_hash": input_hash,
            "output_hash": output_hash,
            "proof": self.generate_execution_proof(
                job_id=job_id,
                gpu_name=execution_result.get("metrics", {}).get("gpu_name", "Unknown"),
                runtime=execution_result.get("runtime_seconds", 0),
                input_hash=input_hash,
                output_hash=output_hash,
                metrics=execution_result.get("metrics", {})
            ),
            "issued_at": datetime.now(UTC).isoformat()
        }
        
        logger.info(f"Receipt created: {receipt['receipt_id']} for job {job_id}")
        return receipt