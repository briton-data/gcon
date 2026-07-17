"""
GCON Agent - Executes AI workloads and monitors GPU execution.

The agent:
1. Runs workloads in isolated containers
2. Monitors GPU utilization and resources
3. Records execution metrics
4. Collects evidence for verification
"""
import threading
import time
import os
import sys
import json
import time
import subprocess
import hashlib
import psutil
import logging
import shlex
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from gcon.monitoring.monitor import ResourceMonitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ExecutionMetrics:
    """Metrics collected during job execution."""
    job_id: str
    gpu_name: str
    gpu_memory_total: int
    gpu_memory_used: int
    cpu_percent: float
    memory_percent: float
    runtime_seconds: float
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class GCONAgent:
    """Main GCON Agent for executing verified workloads."""
    
    def __init__(self, node_id: str):
        """
        Initialize GCON Agent.
        
        Args:
        job_id: Unique identifier for the job
        """
        self.node_id= node_id   
        self.status ="idle"       
        self.start_time = None
        
        self.end_time = None
        self.metrics = []
        self.process = None
        
        self.monitor = ResourceMonitor(self)
        self.heartbeat_running = False
        self.heartbeat_thread = None
        logger.info(f"GCON Agent initialized for node {node_id}")
    
    def detect_gpu(self) -> Dict[str, Any]:
        """
        Detect available GPU hardware.
        
        Returns:
            Dict containing GPU information
        """
        try:
            import GPUtil
            gpus = GPUtil.getGPUs()
            if gpus:
                gpu = gpus[0]
                gpu_info = {
                    "gpu_id": gpu.id,
                    "gpu_name": gpu.name,
                    "memory_total": gpu.memoryTotal,
                    "memory_available": gpu.memoryAvailable,
                    "memory_used": gpu.memoryUsed,
                    "load": gpu.load,
                    "temperature": gpu.temperature
                }
                logger.info(f"GPU detected: {gpu.name} ({gpu.memoryTotal}MB)")
                return gpu_info
            logger.info("No GPU detected. Using fallback GPU detection.")
            return self._fallback_gpu_detection()
            
        except ImportError:
            logger.warning("GPUtil not installed. Using fallback GPU detection.")
            return self._fallback_gpu_detection()
        except Exception as e:
            logger.warning(f"GPU detection failed: {e}. Using fallback.")
            return self._fallback_gpu_detection()
    
    def _fallback_gpu_detection(self) -> Dict[str, Any]:
        """Fallback GPU detection when GPUtil is not available."""
        return {
            "gpu_id": 0,
            "gpu_name": "Unknown GPU",
            "memory_total": 0,
            "memory_available": 0,
            "memory_used": 0,
            "load": 0,
            "temperature": 0
        }
    
    def collect_metrics(self, job_id) -> ExecutionMetrics:
        """
        Collect current system metrics during execution.
        
        Returns:
            ExecutionMetrics object with current metrics
        """
        gpu_info = self.detect_gpu()
        metrics = ExecutionMetrics(
            job_id=job_id,
            gpu_name=gpu_info.get("gpu_name", "Unknown"),
            gpu_memory_total=gpu_info.get("memory_total", 0),
            gpu_memory_used=gpu_info.get("memory_used", 0),
            cpu_percent=psutil.cpu_percent(interval=0.1),
            memory_percent=psutil.virtual_memory().percent,
            runtime_seconds=time.time() - self.start_time if self.start_time else 0,
            timestamp=datetime.now(UTC).isoformat()
        )
        
        self.metrics.append(metrics)
        return metrics
    
    def cancel(self):
        """
        Terminate the currently running job's process, if any.
        Returns True if a live process was actually killed.
        """
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            return True
        return False

    def execute_job(self, job_id, job_script: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a job script and monitor execution.
        
        Args:
            job_script: Path to Python script or command to execute
            timeout: Maximum execution time in seconds
            
        Returns:
            Dict containing execution results and metrics
        """
        logger.info(f"Starting job execution: {job_script}")
        self.start_time = time.time()
        self.status = "busy"
        
        
        try:
            # Determine if it's a file or command
            if os.path.isfile(job_script) and job_script.endswith('.py'):
                command = [sys.executable, job_script]
            else:
                command = shlex.split(job_script)
            logger.info(f"Executing command: {' '.join(command)}")
            
            # Execute the job
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Monitor execution
            stdout, stderr = self.process.communicate(timeout=timeout)
            self.end_time = time.time()
            
            runtime = self.end_time - self.start_time
            final_metrics = self.collect_metrics(job_id)            
            result = {
                "job_id": job_id,
                "status": "success" if self.process.returncode == 0 else "failed",
                "return_code": self.process.returncode,
                "runtime_seconds": runtime,
                "stdout": stdout,
                "stderr": stderr,
                "metrics": final_metrics.to_dict(),
                "timestamp": datetime.now(UTC).isoformat()
            }
            
            logger.info(f"Job completed in {runtime:.2f}s with return code {self.process.returncode}")
            self.status = "idle"
            
            return result
            
        except subprocess.TimeoutExpired:
            logger.error(f"Job timeout after {timeout}s")
            self.process.kill()
            self.end_time = time.time()
            self.status = "idle"
            return {
                "job_id": job_id,
                "status": "timeout",
                "runtime_seconds": self.end_time - self.start_time,
                "error": f"Execution timeout after {timeout}s",
                "timestamp": datetime.now(UTC).isoformat()
            }
        except Exception as e:
            logger.error(f"Job execution failed: {e}")
            self.end_time = time.time()
            self.status = "idle"
            return {
                "job_id": job_id,
                "status": "error",
                "runtime_seconds": self.end_time - self.start_time,
                "error": str(e),
                "timestamp": datetime.now(UTC).isoformat()
            }
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics."""
        if not self.metrics:
            return {"error": "No metrics collected"}
        
        return {
            "node_id": self.node_id,
            "total_samples": len(self.metrics),
            "first_sample": self.metrics[0].to_dict(),
            "last_sample": self.metrics[-1].to_dict(),
            "avg_cpu_percent": sum(m.cpu_percent for m in self.metrics) / len(self.metrics),
            "avg_memory_percent": sum(m.memory_percent for m in self.metrics) / len(self.metrics)
        }
    
    def is_available(self,):
        """
        Return True if this node is available to execute jobs.
        """
        return self.status == "idle"   
    
    from datetime import datetime, UTC

    def heartbeat(self):
        """
        Generate a heartbeat for this node.
        """

        return {
            "node_id": self.node_id,
            "status": self.status,
            "timestamp": datetime.now(UTC)
    }
     
        
    def start_heartbeat(self, coordinator, interval=2):
        """
        Start sending heartbeats periodically.
        """
        if self.heartbeat_running:
            return
        self.heartbeat_running = True

         
        def heartbeat_loop():
           
            while self.heartbeat_running:
                 
                coordinator.receive_heartbeat(self.heartbeat())

                time.sleep(interval)

        self.heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            daemon=True
    )

        self.heartbeat_thread.start() 
        
    def stop_heartbeat(self):
        """
        Stop sending heartbeats.
        """

        self.heartbeat_running = False

        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join(timeout=1)
            
    def report_resources(self):
        """
        Return the current node resource usage.
        """
        return self.monitor.collect()