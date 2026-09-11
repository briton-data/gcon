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

A second, separate check -- check_submission()/evaluate_submission()
below -- runs at submit_job() time, BEFORE a job is ever queued or
dispatched. Unlike evaluate() above (which can only ever be a
post-hoc trust signal, per the design note above), the submission
check looks only at what's knowable before a job runs at all: its
declared kind, requested resources, requested replica count, and
org attribution. Nothing here inspects actual runtime behavior, so
it CAN reject outright -- this is a real gate, not an annotation.
The two checks are deliberately independent and never merged: a job
can fail evaluate() (ran, but exceeded a resource ceiling) without
ever having been rejectable at submission (declared nothing policy
could object to in advance), and vice versa.
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
            "require_gpu": False,
            # Submission-time gate settings (see module docstring).
            # All default to "no restriction", so an existing
            # policy.json with none of these keys behaves exactly as
            # before -- these are additive, not a breaking change to
            # the policy file format.
            "max_replicas": None,
            "max_requires": {},
            "require_org_id": False,
        }

        self.policy = dict(default_policy)
        try:
            with open(policy_file, "r") as file:
                loaded = json.load(file)
            # Merge over the defaults rather than replacing outright:
            # an existing policy.json written before max_replicas/
            # max_requires/require_org_id existed (e.g. this repo's
            # own root policy.json) still has every OLD key it
            # already sets, but would KeyError on the new ones if we
            # discarded the defaults instead of merging. New keys the
            # file doesn't mention keep their "no restriction"
            # default; any key the file DOES set overrides it.
            if isinstance(loaded, dict):
                self.policy.update(loaded)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

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

    def check_submission(self, kind=None, requires=None, verify=None, org_id=None):
        """
        Real, pre-dispatch gate -- everything checked here is known at
        submit_job() time, before the job has run at all, so (unlike
        evaluate() above) a failure here means genuinely rejecting the
        submission, not just annotating it after the fact.

        Returns (allowed: bool, reason: str | None). `reason` is None
        when allowed is True.
        """
        max_replicas = self.policy.get("max_replicas")
        if verify is not None and max_replicas is not None:
            replicas = verify.get("replicas", 2)
            if replicas > max_replicas:
                return False, (
                    f"Requested {replicas} replicas exceeds policy's "
                    f"max_replicas of {max_replicas}"
                )

        max_requires = self.policy.get("max_requires") or {}
        if requires:
            for key, ceiling in max_requires.items():
                requested = requires.get(key)
                if requested is not None and ceiling is not None and requested > ceiling:
                    return False, (
                        f"Requested {key}={requested} exceeds policy's "
                        f"max_requires.{key} ceiling of {ceiling}"
                    )

        if self.policy.get("require_org_id") and org_id is None:
            return False, (
                "Policy requires every job to carry an org_id "
                "(require_org_id=true), but this submission has none"
            )

        return True, None

    def evaluate_submission(self, kind=None, requires=None, verify=None, org_id=None):
        """
        Same inputs/logic as check_submission(), but returns a full
        report shaped like evaluate()'s, for callers (e.g. a future
        dashboard "why was this rejected" view) that want the
        structured form rather than a single (bool, reason) pair.
        """
        allowed, reason = self.check_submission(
            kind=kind, requires=requires, verify=verify, org_id=org_id
        )
        return {
            "allowed": allowed,
            "checks": [{
                "name": "Submission Policy",
                "passed": allowed,
                "message": reason or "Submission within policy",
            }],
        }
