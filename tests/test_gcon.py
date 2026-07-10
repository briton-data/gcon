"""
GCON Tests - Unit tests for core components.

Tests cover:
- Agent execution
- Verification and hashing
- Receipt management
- Proof validation
"""

import unittest
import tempfile
import os
import json
from datetime import datetime

from agent import GCONAgent, ExecutionMetrics
from verifier import ExecutionVerifier
from receipt import (
    ReceiptManager,
    ReceiptFormatter,
    ReceiptGenerator,
    ReceiptVerifier,
)
from run_job import JobRunner


class TestExecutionMetrics(unittest.TestCase):
    """Test ExecutionMetrics dataclass."""
    
    def test_metrics_creation(self):
        """Test creating execution metrics."""
        metrics = ExecutionMetrics(
            job_id="test-job-1",
            gpu_name="RTX 4090",
            gpu_memory_total=24576,
            gpu_memory_used=12288,
            cpu_percent=45.5,
            memory_percent=60.0,
            runtime_seconds=120.5,
            timestamp=datetime.utcnow().isoformat()
        )
        
        self.assertEqual(metrics.job_id, "test-job-1")
        self.assertEqual(metrics.gpu_name, "RTX 4090")
        self.assertEqual(metrics.gpu_memory_total, 24576)
    
    def test_metrics_to_dict(self):
        """Test converting metrics to dictionary."""
        metrics = ExecutionMetrics(
            job_id="test-job-1",
            gpu_name="RTX 4090",
            gpu_memory_total=24576,
            gpu_memory_used=12288,
            cpu_percent=45.5,
            memory_percent=60.0,
            runtime_seconds=120.5,
            timestamp=datetime.utcnow().isoformat()
        )
        
        d = metrics.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["job_id"], "test-job-1")


class TestExecutionVerifier(unittest.TestCase):
    """Test ExecutionVerifier functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.verifier = ExecutionVerifier("test-secret-key")
    
    def test_hash_string(self):
        """Test hashing a string."""
        data = "test data"
        hash1 = self.verifier.hash_data(data)
        hash2 = self.verifier.hash_data(data)
        
        # Same input should produce same hash
        self.assertEqual(hash1, hash2)
        # Hash should be valid SHA256 hex string
        self.assertEqual(len(hash1), 64)
        self.assertTrue(all(c in '0123456789abcdef' for c in hash1))
    
    def test_hash_dict(self):
        """Test hashing a dictionary."""
        data = {"key": "value", "number": 42}
        hash_val = self.verifier.hash_data(data)
        
        self.assertEqual(len(hash_val), 64)
        self.assertTrue(all(c in '0123456789abcdef' for c in hash_val))
    
    def test_hash_file(self):
        """Test hashing a file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test file content")
            temp_path = f.name
        
        try:
            hash_val = self.verifier.hash_file(temp_path)
            self.assertEqual(len(hash_val), 64)
        finally:
            os.unlink(temp_path)
    
    def test_sign_and_verify(self):
        """Test signing and verifying data."""
        data = {"job_id": "test-1", "status": "success"}
        
        signature = self.verifier.sign_data(data)
        self.assertTrue(len(signature) > 0)
        
        # Verify correct signature
        self.assertTrue(self.verifier.verify_signature(data, signature))
        
        # Verify invalid signature fails
        invalid_sig = "invalid" * 8
        self.assertFalse(self.verifier.verify_signature(data, invalid_sig))
    
    def test_generate_execution_proof(self):
        """Test generating execution proof."""
        proof = self.verifier.generate_execution_proof(
            job_id="test-job-1",
            gpu_name="RTX 4090",
            runtime=100.5,
            input_hash="abc123",
            output_hash="def456"
        )
        
        self.assertIn("signature", proof)
        self.assertEqual(proof["job_id"], "test-job-1")
        self.assertEqual(proof["gpu"], "RTX 4090")
        self.assertTrue(proof["verified"])
    
    def test_validate_proof(self):
        """Test validating execution proof."""
        proof = self.verifier.generate_execution_proof(
            job_id="test-job-1",
            gpu_name="RTX 4090",
            runtime=100.5,
            input_hash="abc123",
            output_hash="def456"
        )
        
        is_valid, message = self.verifier.validate_proof(proof)
        self.assertTrue(is_valid)
        self.assertEqual(message, "Proof is valid")


class TestReceiptManager(unittest.TestCase):
    """Test ReceiptManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = ReceiptManager(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_save_and_load_receipt(self):
        """Test saving and loading receipts."""
        receipt = {
            "receipt_id": "receipt-001",
            "job_id": "job-001",
            "status": "success"
        }
        
        # Save receipt
        success = self.manager.save_receipt(receipt)
        self.assertTrue(success)
        
        # Load receipt
        loaded = self.manager.load_receipt("receipt-001")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["job_id"], "job-001")
    
    def test_list_receipts(self):
        """Test listing receipts."""
        receipts = [
            {"receipt_id": "r1", "job_id": "job-001", "status": "success"},
            {"receipt_id": "r2", "job_id": "job-002", "status": "success"},
            {"receipt_id": "r3", "job_id": "job-001", "status": "failed"}
        ]
        
        for receipt in receipts:
            self.manager.save_receipt(receipt)
        
        # List all receipts
        all_receipts = self.manager.list_receipts()
        self.assertEqual(len(all_receipts), 3)
        
        # Filter by job_id
        job_001_receipts = self.manager.list_receipts("job-001")
        self.assertEqual(len(job_001_receipts), 2)
    
    def test_delete_receipt(self):
        """Test deleting receipts."""
        receipt = {"receipt_id": "receipt-001", "job_id": "job-001"}
        
        self.manager.save_receipt(receipt)
        loaded = self.manager.load_receipt("receipt-001")
        self.assertIsNotNone(loaded)
        
        success = self.manager.delete_receipt("receipt-001")
        self.assertTrue(success)
        
        loaded = self.manager.load_receipt("receipt-001")
        self.assertIsNone(loaded)


class TestReceiptFormatter(unittest.TestCase):
    """Test ReceiptFormatter functionality."""
    
    def test_to_json_string(self):
        """Test converting receipt to JSON."""
        receipt = {
            "receipt_id": "r1",
            "job_id": "job-001",
            "status": "success"
        }
        
        json_str = ReceiptFormatter.to_json_string(receipt, pretty=True)
        self.assertIn("receipt_id", json_str)
        self.assertIn("job-001", json_str)
    
    def test_to_summary(self):
        """Test creating receipt summary."""
        receipt = {
            "receipt_id": "r1",
            "job_id": "job-001",
            "status": "success",
            "input_hash": "abc123",
            "output_hash": "def456",
            "proof": {
                "gpu": "RTX 4090",
                "runtime_seconds": 100.5,
                "verified": True
            },
            "issued_at": datetime.utcnow().isoformat()
        }
        
        summary = ReceiptFormatter.to_summary(receipt)
        self.assertIn("GCON EXECUTION RECEIPT", summary)
        self.assertIn("job-001", summary)
        self.assertIn("RTX 4090", summary)

class TestReceiptVerifier(unittest.TestCase):
    """Test ReceiptVerifier functionality."""

    def setUp(self):
        """Create a sample execution result."""
        self.execution_result = {
            "job_id": "job-001",
            "status": "completed",
            "timestamp": datetime.utcnow().isoformat(),
            "stdout": "hello world",
            "stderr": "",
            "metrics": {
                "gpu_name": "RTX 4090",
                "runtime_seconds": 1.25,
                "cpu_percent": 15.0,
                "memory_percent": 25.0,
            },
        }

    def test_valid_receipt_verifies(self):
        """A newly generated receipt should verify."""
        receipt = ReceiptGenerator.generate(self.execution_result)

        self.assertTrue(ReceiptVerifier.verify(receipt))

    def test_modified_receipt_fails(self):
        """Changing the receipt should invalidate it."""
        receipt = ReceiptGenerator.generate(self.execution_result)

        receipt["stdout"] = "tampered output"

        self.assertFalse(ReceiptVerifier.verify(receipt))

    def test_missing_hash_fails(self):
        """Receipt without a hash should fail verification."""
        receipt = ReceiptGenerator.generate(self.execution_result)

        receipt.pop("receipt_hash")

        self.assertFalse(ReceiptVerifier.verify(receipt))

    def test_corrupted_hash_fails(self):
        """Receipt with an incorrect hash should fail verification."""
        receipt = ReceiptGenerator.generate(self.execution_result)

        receipt["receipt_hash"] = "0" * 64

        self.assertFalse(ReceiptVerifier.verify(receipt))

class TestJobRunner(unittest.TestCase):
    """Test JobRunner functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.runner = JobRunner(storage_dir=self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_job_runner_initialization(self):
        """Test initializing job runner."""
        runner = JobRunner()
        self.assertIsNotNone(runner.agent_id)
        self.assertIsNotNone(runner.verifier)
        self.assertIsNotNone(runner.receipt_manager)


if __name__ == "__main__":
    unittest.main()
