"""
PolicyEngine -- evaluates an already-completed, cryptographically
verified receipt against configurable resource/runtime limits
(policy.json at the repo root, or GCON_POLICY_FILE): max runtime, max
CPU%, max memory%, and whether a GPU is required.

This was previously test-only support code (tests/support/policy.py),
fully correct and covered by its own tests, but never imported by any
real coordinator code -- policy.json existed and looked authoritative,
but nothing ever read it at runtime. Moved here verbatim (see
tests/support/policy.py, now a thin re-export so existing test imports
keep working unchanged) and wired into GCONCoordinator._run_job, right
after a receipt is created.

Design note on WHEN this runs, and why: this evaluates *metrics a job
already produced* (runtime_seconds, cpu_percent, etc. -- see
ExecutionMetrics), so it cannot run before or during execution the way
a resource *request* like "resourced" jobs' `requires` does (that's
matched by the scheduler before dispatch, on reported capabilities --
a different kind of check entirely, comparing what a node CAN do vs.
what a job DID do). A policy violation here is fundamentally
after-the-fact: "this job ran, but consumed more than policy allows."
It can't be rejected before running, and by the time evaluate() runs
the job has already finished -- there is nothing left to reject. So
the outcome is a trust signal, not a scheduling decision: the job's
receipt is annotated with the evaluation (report["trusted"], each
individual check), it never changes the job's own status
("completed"/"failed" is still purely about whether the command
itself succeeded), and a violation additionally publishes a
POLICY_VIOLATION event through the existing event bus, so it reaches
the dashboard's notification center the same way offline nodes and
failed jobs already do -- no new UI mechanism invented for this.
"""
import json


class PolicyEngine:

    def __init__(self, policy_file="policy.json"):
        """Load policy configuration."""

        default_policy = {
            "version": "1.0",
            "max_runtime": 30.0,
            "max_cpu_percent": 90.0,
            "max_memory_percent": 95.0,
            "require_gpu": False
        }

        try:
            with open(policy_file, "r") as file:
                self.policy = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            self.policy = default_policy

    def check_runtime(self, receipt):
        """
        Check whether the runtime satisfies the policy.
        """

        proof = receipt.get("proof", {})
        metrics = proof.get("metrics", {})

        runtime = metrics.get("runtime_seconds")

        if runtime is None:
            return False, "Runtime missing"

        if runtime > self.policy["max_runtime"]:
            return (
                False,
                f"Runtime {runtime:.2f}s exceeds limit of {self.policy['max_runtime']:.2f}s"
            )

        return True, f"Runtime {runtime:.2f}s within policy"

    def check_cpu(self, receipt):
        """
        Check whether CPU usage satisfies the policy.
        """

        proof = receipt.get("proof", {})
        metrics = proof.get("metrics", {})

        cpu = metrics.get("cpu_percent")

        if cpu is None:
            return False, "CPU usage missing"

        if cpu > self.policy["max_cpu_percent"]:
            return (
                False,
                f"CPU usage {cpu:.1f}% exceeds limit of "
                f"{self.policy['max_cpu_percent']:.1f}%"
            )

        return True, f"CPU usage {cpu:.1f}% within policy"

    def check_memory(self, receipt):
        """
        Check whether memory usage satisfies the policy.
        """
        proof = receipt.get("proof", {})
        metrics = proof.get("metrics", {})

        memory = metrics.get("memory_percent")

        if memory is None:
            return False, "Memory usage missing"

        if memory > self.policy["max_memory_percent"]:
            return (
                False,
                f"Memory usage {memory:.1f}% exceeds limit of "
                f"{self.policy['max_memory_percent']:.1f}%"
            )

        return True, f"Memory usage {memory:.1f}% within policy"

    def check_gpu(self, receipt):
        """
        Check whether GPU usage satisfies the policy.
        """

        proof = receipt.get("proof", {})
        metrics = proof.get("metrics", {})

        gpu_total = metrics.get("gpu_memory_total")
        gpu_used = metrics.get("gpu_memory_used")

        if gpu_total is None or gpu_used is None:
            return False, "GPU metrics missing"

        # GPU is optional
        if not self.policy["require_gpu"] and gpu_total == 0:
            return True, "GPU not required"

        if self.policy["require_gpu"] and gpu_total == 0:
            return False, "GPU required but not available"

        if gpu_used > gpu_total:
            return False, "GPU memory usage exceeds total memory"

        return True, "GPU policy satisfied"

    def evaluate(self, receipt):
        """
        Evaluate a validated receipt against policy.
        """

        report = {
            "trusted": True,
            "checks": []
        }

        checks = [
            ("Runtime Policy", self.check_runtime),
            ("CPU Policy", self.check_cpu),
            ("Memory Policy", self.check_memory),
            ("GPU Policy", self.check_gpu),
        ]

        for name, check in checks:
            passed, message = check(receipt)

            report["checks"].append({
                "name": name,
                "passed": passed,
                "message": message
            })

            if not passed:
                report["trusted"] = False

        return report
