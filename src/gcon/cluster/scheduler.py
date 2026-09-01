class Scheduler:
    """
    GCON Job Scheduler.

    Selects an available node from the NodeRegistry.
    """

    def __init__(self, registry, control_plane=None):
        """
        Initialize the scheduler.

        Args:
            registry (NodeRegistry): The node registry.
            control_plane: Optional -- gives select_node() access to
                each node's reported capabilities (gpu name, etc.) for
                `requires`-based matching on "resourced" jobs. Nodes
                report these at registration
                (node_capabilities.set_capabilities), but until now
                nothing ever read them back for scheduling -- every
                job could land on any idle node regardless of whether
                it actually had a GPU. Optional and defaulted to None
                so every existing caller that constructs
                Scheduler(registry) with one argument keeps working;
                `requires` is simply never enforced without a
                control_plane to look capabilities up in.
        """
        self.registry = registry
        self.control_plane = control_plane
        # See gcon.execution.staking module docstring: off unless
        # GCON_STAKING_REQUIRED is set, so this never changes
        # scheduling behavior for existing deployments.
        self.stake_ledger = None
        if control_plane is not None:
            from gcon.execution.staking import StakeLedger
            self.stake_ledger = StakeLedger(control_plane)

    def select_node(self, requires=None):
        """
        Select the least-loaded idle node satisfying `requires` (if
        given) and atomically claim it (marks it busy in the
        registry) in one locked operation, so two concurrent callers
        can never both walk away with the same node (AUDIT_REPORT.md
        2.3 / audit finding C-1) -- see
        NodeRegistry.claim_best_idle_node() for how the race is
        closed.

        `requires`, e.g. {"gpu": true, "min_vram_gb": 12,
        "min_cpu_cores": 4}, filters candidates by their reported
        capabilities *before* scoring by load -- a node that doesn't
        satisfy it is never selected, rather than being selected and
        then failing the job (e.g. a CUDA assert) with no warning.
        """

        def score(info):
            return (
                info["cpu"] * 0.5 +
                info["memory"] * 0.3 +
                info["running_jobs"] * 20
            )

        filters = []
        if requires:
            filters.append(lambda info: self._satisfies(info, requires))
        if self.stake_ledger is not None and self.stake_ledger.staking_required:
            filters.append(lambda info: self.stake_ledger.meets_minimum(info["node"].node_id))

        filter_fn = None
        if filters:
            filter_fn = lambda info: all(f(info) for f in filters)

        return self.registry.claim_best_idle_node(score, filter_fn=filter_fn)

    def _satisfies(self, info, requires):
        """
        True if the node behind `info` reports capabilities matching
        every key in `requires`. No control_plane -> capabilities were
        never reported anywhere durable -> nothing can be verified, so
        the node is excluded rather than optimistically assumed to
        qualify (a resourced job asking for a GPU should never
        silently land on a node GCON has no capability record for).
        """
        if self.control_plane is None:
            return False

        node_id = info["node"].node_id
        try:
            capabilities = self.control_plane.node_capabilities.get_capabilities(node_id)
        except Exception:
            return False

        if requires.get("gpu"):
            gpu_name = capabilities.get("gpu", "")
            if not gpu_name or gpu_name == "Unknown GPU":
                return False

        min_vram_gb = requires.get("min_vram_gb")
        if min_vram_gb:
            try:
                vram_mb = float(capabilities.get("gpu_memory_total_mb", 0))
            except (TypeError, ValueError):
                vram_mb = 0
            if vram_mb < float(min_vram_gb) * 1024:
                return False

        min_cpu_cores = requires.get("min_cpu_cores")
        if min_cpu_cores:
            try:
                cores = float(capabilities.get("cpu_cores", 0))
            except (TypeError, ValueError):
                cores = 0
            if cores < float(min_cpu_cores):
                return False

        return True
    

    def has_available_node(self):
        """
        Check whether an idle node exists.

        Returns:
            bool
        """

        return len(self.registry.available_nodes()) > 0

    def node_count(self):
        """
        Return the number of registered nodes.
        """

        return len(self.registry.nodes)