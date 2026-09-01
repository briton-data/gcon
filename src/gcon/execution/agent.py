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
import signal
import subprocess
import psutil
import logging
import shlex
import json
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
            cpu_percent=psutil.cpu_percent(interval=None),
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

        Two real bugs fixed here, found by the stress suite
        (test_cancel_job_kills_process_and_frees_node):

        1. self.process.kill() only killed the shell wrapper when the
           job used shell syntax (verified: shell exits, the actual
           command it ran keeps going as an orphan) -- now kills the
           whole process group instead, via the session started in
           execute_job().
        2. cancel() could be called in the brief window after a job
           is marked "running" but before its background thread has
           actually reached subprocess.Popen() yet, and would then
           silently report nothing was killed even though a process
           was about to start. A short bounded wait closes that gap.
        """
        deadline = time.time() + 2.0
        while self.process is None and time.time() < deadline:
            time.sleep(0.02)

        if self.process is not None and self.process.poll() is None:
            pid = self.process.pid
            try:
                if os.name == "posix":
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                else:
                    self.process.kill()
            except (ProcessLookupError, PermissionError):
                # Already exited between the poll() check and the
                # kill, or the group is gone -- either way there's
                # nothing left to kill, not a real failure.
                return False
            return True
        return False

    def execute_job(
        self,
        job_id,
        job_script: str,
        timeout: Optional[int] = None,
        usage_report_path: Optional[str] = None,
        stage_report_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a job script and monitor execution.
        
        Args:
            job_script: Path to Python script or command to execute
            timeout: Maximum execution time in seconds
            usage_report_path: If given, this path is exported to the
                job's subprocess as GCON_USAGE_REPORT_PATH. GCON never
                inspects what a job's command actually does (it may be
                an arbitrary shell command, not necessarily an LLM
                call), so consumption metering -- LLM token counts in
                particular -- can only be captured by convention: if
                the job script chooses to write a small JSON object
                (e.g. {"llm_tokens": {"input": 1200, "output": 340,
                "model": "..."}}) to the path in that env var before
                it exits, it's picked up here and returned as
                result["usage"]. A job that doesn't cooperate with the
                convention (most won't -- it's opt-in) simply leaves
                result["usage"] as None; this is never fabricated or
                estimated.
            stage_report_path: Same opt-in convention as
                usage_report_path, for "staged" jobs (see
                GCONCoordinator.submit_job's `kind`/`stages`). Exported
                to the subprocess as GCON_STAGE_REPORT_PATH. Unlike the
                usage report -- read once, after the process exits --
                this path is polled by a background thread WHILE the
                subprocess is still running: once per stage, the job's
                own code is expected to append one JSON line (e.g.
                {"stage": 3, "metrics": {"loss": 0.31}}) to this file.
                Each newly-seen line is timestamped with THIS agent's
                own wall clock at the moment it's observed -- not
                whatever the job itself claims -- so a stage's proof
                reflects when GCON actually saw it happen, not
                something the job process could pad or backdate.
                Returned as result["stages"]: always a list, empty if
                the job never cooperates with the convention (most
                won't -- it's opt-in, same rule as usage_report_path).
            
        Returns:
            Dict containing execution results and metrics
        """
        logger.info(f"Starting job execution: {job_script}")
        self.start_time = time.time()
        self.status = "busy"

        job_env = None
        if usage_report_path:
            job_env = dict(os.environ)
            job_env["GCON_USAGE_REPORT_PATH"] = usage_report_path
            # Belt-and-braces: if a stale file from a previous job
            # happens to already exist at this path, don't let its
            # content leak into this job's usage report.
            try:
                if os.path.exists(usage_report_path):
                    os.remove(usage_report_path)
            except OSError:
                pass

        stage_observations: list = []
        stage_thread = None
        stage_stop = None
        if stage_report_path:
            job_env = job_env or dict(os.environ)
            job_env["GCON_STAGE_REPORT_PATH"] = stage_report_path
            try:
                if os.path.exists(stage_report_path):
                    os.remove(stage_report_path)
            except OSError:
                pass
            stage_stop = threading.Event()
            stage_thread = threading.Thread(
                target=self._watch_stage_reports,
                args=(stage_report_path, stage_observations, stage_stop),
                daemon=True,
            )
            stage_thread.start()

        try:
            # Determine if it's a file or command
            if os.path.isfile(job_script) and job_script.endswith('.py'):
                command = [sys.executable, job_script]
                use_shell = False
            else:
                # JobSubmitRequest.command is documented as "Shell
                # command the job will run" -- shlex.split() here
                # would silently strip that meaning: subprocess would
                # exec the first token as a literal program name with
                # the rest as its argv, so shell operators like ||,
                # &&, |, or ; are never interpreted, just passed
                # through as inert extra arguments. Run the raw
                # string through a real shell instead, so a job can
                # actually use the shell syntax the API promises.
                command = job_script
                use_shell = True
            logger.info(f"Executing command: {command if use_shell else ' '.join(command)}")
            
            # Execute the job
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=use_shell,
                env=job_env,
                # With shell=True, self.process IS /bin/sh, not the
                # real command -- killing just that PID leaves
                # whatever the shell spawned running as an orphan
                # (verified: shell exits, grandchild keeps running).
                # start_new_session puts the whole tree in its own
                # process group so cancel() can kill all of it at
                # once via os.killpg, not just the shell wrapper.
                start_new_session=(os.name == "posix"),
            )
            
            # Monitor execution
            stdout, stderr = self.process.communicate(timeout=timeout)
            self.end_time = time.time()
            self._stop_stage_watcher(stage_thread, stage_stop, stage_report_path)

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
                "usage": self._read_usage_report(usage_report_path),
                # Always a list -- possibly partial (see the timeout/
                # error branches below) or empty (job never opted in
                # or didn't report before failing), never fabricated.
                "stages": stage_observations,
                "timestamp": datetime.now(UTC).isoformat()
            }
            
            logger.info(f"Job completed in {runtime:.2f}s with return code {self.process.returncode}")
            self.status = "idle"
            
            return result
            
        except subprocess.TimeoutExpired:
            logger.error(f"Job timeout after {timeout}s")
            self.process.kill()
            self.end_time = time.time()
            self._stop_stage_watcher(stage_thread, stage_stop, stage_report_path)
            self.status = "idle"
            return {
                "job_id": job_id,
                "status": "timeout",
                "runtime_seconds": self.end_time - self.start_time,
                "error": f"Execution timeout after {timeout}s",
                "usage": self._read_usage_report(usage_report_path),
                # Whatever stages were genuinely observed before the
                # timeout killed the process -- real partial proof of
                # how far a long-running (e.g. training) job actually
                # got, not thrown away just because it didn't finish.
                "stages": stage_observations,
                "timestamp": datetime.now(UTC).isoformat()
            }
        except Exception as e:
            logger.error(f"Job execution failed: {e}")
            self.end_time = time.time()
            self._stop_stage_watcher(stage_thread, stage_stop, stage_report_path)
            self.status = "idle"
            return {
                "job_id": job_id,
                "status": "error",
                "stages": stage_observations,
                "runtime_seconds": self.end_time - self.start_time,
                "error": str(e),
                "usage": self._read_usage_report(usage_report_path),
                "timestamp": datetime.now(UTC).isoformat()
            }

    @staticmethod
    def _read_usage_report(usage_report_path: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Read back and remove the (optional) usage report a job script
        may have written to GCON_USAGE_REPORT_PATH. Returns None --
        never a fabricated/zeroed default -- if no path was given, no
        file was written, or the file isn't valid JSON, so a missing
        report is always distinguishable from a genuine "zero usage"
        report the job wrote on purpose.
        """
        if not usage_report_path or not os.path.exists(usage_report_path):
            return None
        try:
            with open(usage_report_path, "r") as f:
                content = f.read()
            return json.loads(content) if content.strip() else None
        except (OSError, ValueError) as e:
            logger.warning(f"Could not read usage report at {usage_report_path}: {e}")
            return None
        finally:
            try:
                os.remove(usage_report_path)
            except OSError:
                pass

    @staticmethod
    def _watch_stage_reports(path: str, observed: list, stop_event: "threading.Event") -> None:
        """
        Background thread body for a "staged" job: while the
        subprocess is still running, poll `path` (GCON_STAGE_REPORT_PATH)
        for newly-appended JSON lines and record each one, stamped
        with THIS agent's own wall clock at the moment it's observed --
        not anything the job process itself claims -- so a stage's
        proof reflects when GCON actually saw it happen. Malformed
        lines are skipped rather than aborting the whole job. Opt-in
        and best-effort, same rule as _read_usage_report: a job that
        never writes to this path just produces an empty `observed`
        list, never a fabricated one.

        Structured as "poll, then check stop" rather than "check stop,
        then poll" so that calling stop_event.set() is always followed
        by one more poll before the thread exits -- otherwise a stage
        written in the ~0.5s window right before the process exits
        could be missed entirely.
        """
        lines_seen = 0
        while True:
            try:
                if os.path.exists(path):
                    with open(path, "r") as f:
                        lines = f.readlines()
                    for line in lines[lines_seen:]:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except ValueError:
                            continue
                        observed.append({
                            "stage": entry.get("stage"),
                            "metrics": entry.get("metrics", {}),
                            "observed_at": datetime.now(UTC).isoformat(),
                        })
                    lines_seen = len(lines)
            except OSError:
                pass
            if stop_event.is_set():
                break
            stop_event.wait(0.5)

    @staticmethod
    def _stop_stage_watcher(
        thread: Optional["threading.Thread"],
        stop_event: Optional["threading.Event"],
        stage_report_path: Optional[str],
    ) -> None:
        """Stop and join the stage-watcher thread (no-op if this
        job wasn't "staged"), then remove the report file -- same
        belt-and-braces cleanup as _read_usage_report, so a stale
        file from this job never leaks into a future one on this
        node."""
        if thread is not None and stop_event is not None:
            stop_event.set()
            thread.join(timeout=5)
        if stage_report_path:
            try:
                os.remove(stage_report_path)
            except OSError:
                pass

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