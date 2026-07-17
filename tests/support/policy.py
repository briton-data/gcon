
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

        if runtime >self.policy["max_runtime"]:
            return (
                False,
                f"Runtime {runtime:.2f}s exceeds limit of {self.max_runtime:.2f}s"
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