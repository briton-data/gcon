class MetricsCollector:
    """
    Collect raw metrics from the GCON cluster.

    This class gathers information about nodes and jobs.
    It does not perform calculations or display output.
    """

    def __init__(self, coordinator):
        self.coordinator=coordinator

    def collect_node_metrics(self):
        """
        Collect metrics for every registered node.

        Returns:
            list[dict]: Raw node metrics.
        """
        node_metrics = []

        # Iterate through registered nodes
        for node_id, info in self.coordinator.registry.nodes.items():
             # Build a dictionary for each node
            
            metrics = {
                "node_id": node_id,
                "status": info["status"],
                "cpu": info.get("cpu"),
                "memory": info.get("memory"),
                "running_jobs": info.get("running_jobs"),
                "last_seen": info["last_seen"]
}
       
        # Append to node_metrics
            node_metrics.append(metrics)
        
        return node_metrics

    def collect_job_metrics(self):
        """
        Collect metrics for every job.

        Returns:
            list[dict]: Raw job metrics.
        """
        job_metrics = []

        # Iterate through coordinator.jobs
        for job_id, job in self.coordinator.jobs.items():
        
        # Build a dictionary for each job
            metrics = {
                "job_id": job_id,
                "status": job["status"],
                "command": job["command"],
                "node_id": job["node_id"],
        "result": job.get("result")
}
        
        # Append to job_metrics
        job_metrics.append(metrics)

        return job_metrics
    
class MetricsSummary:
    """
    Generate summary statistics from collected metrics.
    """

    def __init__(self, node_metrics, job_metrics):
        self.node_metrics = node_metrics
        self.job_metrics = job_metrics
        
    
    def summarize_nodes(self):
        """
        Generate a summary of node statuses.

        Returns:
            dict: Summary statistics for all registered nodes.
        """

        total_nodes = len(self.node_metrics)

        busy_nodes = 0
        idle_nodes = 0
        offline_nodes = 0

        for node in self.node_metrics:

            status = node["status"]

            if status == "busy":
                busy_nodes += 1

            elif status == "idle":
                idle_nodes += 1

            elif status == "offline":
                offline_nodes += 1

        return {
            "total_nodes": total_nodes,
            "busy_nodes": busy_nodes,
            "idle_nodes": idle_nodes,
            "offline_nodes": offline_nodes,
    }
        
        
    def summarize_jobs(self):
        """
        Generate a summary of job statuses.

        Returns:
            dict: Summary statistics for all jobs.
        """

        total_jobs = len(self.job_metrics)

        pending_jobs = 0
        running_jobs = 0
        completed_jobs = 0
        failed_jobs = 0

        for job in self.job_metrics:

            status = job["status"]

            if status == "pending":
                pending_jobs += 1

            elif status == "running":
                running_jobs += 1

            elif status == "completed":
                completed_jobs += 1

            elif status == "failed":
                failed_jobs += 1

        return {
            "total_jobs": total_jobs,
            "pending_jobs": pending_jobs,
            "running_jobs": running_jobs,
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
    }
        
    
    def summarize_resources(self):
        """
        Generate a summary of cluster resource usage.

        Returns:
            dict: Average CPU and memory usage.
        """

        total_cpu = 0
        total_memory = 0
        reported_nodes = 0

        for node in self.node_metrics:

            cpu = node.get("cpu")
            memory = node.get("memory")

        # Skip nodes that haven't reported resources yet
            if cpu is None or memory is None:
                continue

            total_cpu += cpu
            total_memory += memory
            reported_nodes += 1

        if reported_nodes == 0:
            return {
                "average_cpu": 0,
                "average_memory": 0
        }

        return {
            "average_cpu": round(total_cpu / reported_nodes, 2),
            "average_memory": round(total_memory / reported_nodes, 2)
    }
        
    def cluster_summary(self):
        """
        Generate a complete summary of the cluster.

        Returns:
            dict: Combined node, job, and resource summaries.
        """

        return {
            "nodes": self.summarize_nodes(),
            "jobs": self.summarize_jobs(),
            "resources": self.summarize_resources()
    }