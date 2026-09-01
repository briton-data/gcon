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
import os
import secrets
from pathlib import Path
from datetime import datetime, UTC
from typing import Dict, Any, Optional, Tuple
import logging

from gcon.execution.hmac_keyring import HmacKeyring

logger = logging.getLogger(__name__)


class ExecutionVerifier:
    """Verifies execution and generates cryptographic proofs."""

    def __init__(self, secret_key: Optional[str] = None):
        """
        Initialize ExecutionVerifier.

        Args:

            secret_key: Explicit HMAC key (used directly, single-key
                mode, no rotation/persistence -- this is what the test
                suite passes to get a fixed, known key). If not
                provided, a rotation-capable keyring is loaded from
                (or generated into) GCON_HMAC_KEY_PATH, defaulting to
                "./keys/hmac_secret.key" -- see HmacKeyring. Never
                falls back to a hardcoded constant, since anyone with
                source access could forge signatures with a known key.
        """
        if secret_key is not None:
            self._keyring = None
            self._explicit_key = secret_key
        else:
            self._keyring = HmacKeyring.load_or_create(
                os.environ.get("GCON_HMAC_KEY_PATH", "./keys/hmac_secret.key")
            )
            self._explicit_key = None
        logger.info("ExecutionVerifier initialized")

    @property
    def secret_key(self) -> str:
        """The key currently used to sign new proofs. Kept for
        backward compatibility with anything reading this directly;
        prefer sign_data/verify_signature, which are rotation-aware."""
        if self._explicit_key is not None:
            return self._explicit_key
        return self._keyring.current_secret

    def rotate_key(self) -> str:
        """Generates and activates a new signing key, retiring (not
        deleting) the previous one -- see HmacKeyring.rotate. Returns
        the new key_id. Raises RuntimeError in explicit-single-key
        mode (nothing to rotate; that mode exists for tests that want
        a fixed key)."""
        if self._keyring is None:
            raise RuntimeError(
                "This ExecutionVerifier was constructed with an explicit "
                "secret_key (single-key mode); there is no keyring to rotate."
            )
        return self._keyring.rotate()

    
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
        Create HMAC signature of data, using the currently-active
        signing key (self.secret_key).

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
    
    def verify_signature(self, data: Dict[str, Any], signature: str, key_id: Optional[str] = None) -> bool:
        """
        Verify HMAC signature of data.

        Args:
            data: Data to verify
            signature: Expected signature
            key_id: Which key in the keyring signed this (see
                HmacKeyring) -- None means "whatever key
                self.secret_key currently resolves to", which is
                right for single-key mode and for verifying a proof
                that predates key rotation being wired in (no key_id
                field at all, signed against the one key that existed
                at the time). A key_id that names a since-retired key
                is still honored, as long as HmacKeyring still has it
                on file -- see its docstring for the revocation window.

        Returns:
            True if signature is valid
        """
        if key_id is not None and self._keyring is not None:
            secret = self._keyring.get_secret(key_id)
            if secret is None:
                return False
        else:
            secret = self.secret_key
        message = json.dumps(data, sort_keys=True).encode()
        computed_signature = hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
        return hmac.compare_digest(computed_signature, signature)
    
    def generate_execution_proof(
        self,
        job_id: str,
        gpu_name: str,
        runtime: float,
        input_hash: str,
        output_hash: str,
        metrics: Optional[Dict[str, Any]] = None,
        attested_node_id: Optional[str] = None,
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
            attested_node_id: The node identity the coordinator has on
                file as mTLS-authenticated for this connection (see
                gcon.transport.grpc_transport's Register handler,
                which only ever records a node's certificate Common
                Name after verifying it matches the claimed node_id --
                and gcon.persistence.repositories.nodes.NodeRepository
                .upsert, which preserves that fingerprint across
                reconnects rather than overwriting it with None).
                None when there's no control_plane to look it up in,
                or the node connected over a transport with no
                cryptographic identity at all (e.g. LocalTransport,
                used for in-process/dev/test nodes) -- both real,
                legitimate cases, not errors. When given, it's folded
                into the signed payload itself, so the receipt makes a
                checkable claim about a transport-authenticated
                identity, not just whatever create_receipt's caller
                asserts the node's id to be.
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
        if attested_node_id is not None:
            proof_data["attested_node_id"] = attested_node_id
        # key_id is folded into the signed payload itself (not just
        # attached alongside it) -- see HmacKeyring -- so it's
        # tamper-evident the same way every other proof field is: an
        # attacker can't point verification at a different (weaker or
        # compromised) key by editing this field post-hoc without
        # invalidating the signature. None in single-key/explicit-key
        # mode (no keyring, nothing to identify) or for a coordinator
        # that predates rotation -- both verify the same way they
        # always did.
        if self._keyring is not None:
            proof_data["key_id"] = self._keyring.current_key_id

        signature = self.sign_data(proof_data)

        # No "verified" field here. A proof at the moment of creation
        # hasn't been verified by anyone -- it's only been signed.
        # Whether a proof is actually valid can only be answered by
        # calling validate_proof() on it, every time, live; storing a
        # static verdict here would be meaningless at best (it was
        # never included in what's signed -- see validate_proof()'s
        # proof_copy below -- so it's also freely editable without
        # invalidating the signature) and actively misleading at worst
        # to any consumer that read it instead of calling
        # validate_proof(). (A previous version of this method set
        # "verified": True unconditionally right here, before any
        # check ever ran -- run_job.py's JobRunner was reading that
        # exact field as "proof_valid" in its own output, meaning it
        # reported every job's proof as valid regardless of whether
        # the signature actually checked out. Fixed there too -- see
        # run_job.py's execute_job().)
        proof = {
            **proof_data,
            "signature": signature,
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
        
        key_id = proof.get("key_id")
        if not self.verify_signature(proof_copy, signature, key_id=key_id):
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
        output_hash: str,
        attested_node_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a complete execution receipt.
        
        Args:
            job_id: Job identifier
            agent_id: Agent identifier, as claimed by whatever called
                this (e.g. the coordinator's own NodeRegistry bookkeeping)
            execution_result: Result from agent execution
            input_hash: Hash of input
            output_hash: Hash of output
            attested_node_id: See generate_execution_proof's docstring.
                When given and it disagrees with agent_id, this raises
                rather than signing a receipt that would otherwise
                assert two different things about who ran this job --
                a mismatch here means either a real attack (a stream
                serving results under a different identity than the
                one the coordinator dispatched to) or a coordinator
                bookkeeping bug; either way, silently picking one and
                signing it would hide the problem instead of surfacing
                it.

        Returns:
            Complete receipt with proof

        Raises:
            ValueError: if attested_node_id is given and != agent_id.
        """
        if attested_node_id is not None and attested_node_id != agent_id:
            raise ValueError(
                f"Refusing to create a receipt for job '{job_id}': claimed "
                f"agent_id '{agent_id}' does not match the mTLS-authenticated "
                f"node identity on file ('{attested_node_id}'). This could "
                f"mean a compromised/hijacked connection reported this "
                f"result under the wrong node's bookkeeping, or a real "
                f"coordinator bug -- either way, not something to sign over."
            )

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
                metrics=execution_result.get("metrics", {}),
                attested_node_id=attested_node_id,
            ),
            "issued_at": datetime.now(UTC).isoformat()
        }
        
        logger.info(f"Receipt created: {receipt['receipt_id']} for job {job_id}")
        return receipt