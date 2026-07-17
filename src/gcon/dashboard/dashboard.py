class Dashboard:
    """
    Display GCON cluster metrics in a human-readable format.
    """

    def __init__(self, coordinator):
        self.coordinator = coordinator
        self.cluster = {}
        self.nodes = []
        self.jobs = []
        self.events = []
        
    def refresh(self):
        """
        Refresh dashboard data from the coordinator.
        """
        self.cluster = self.coordinator.get_cluster_status()
        self.nodes = self.coordinator.get_nodes()
        self.jobs = self.coordinator.get_jobs()

    def handle_event(self, event):
        """
        Receive cluster events from the EventBus.
        """
        self.events.append(event)
        
        if len(self.events) > 100:
            self.events.pop(0)

        self.refresh()
        self.display()
    
    def format_event(self, event):
        """
        Return a human-readable description of an event.
        """
        lines = [
            f"{event.timestamp} | {event.event_type}",
            f"Source : {event.source}"
    ]

        if event.payload:
            for key, value in event.payload.items():
                lines.append(f"{key}: {value}")

        return "\n".join(lines)
    
    def display(self):
        """
        Display the current cluster dashboard.
        """

        # Retrieve data from the Coordinator
        self.refresh()

        print("=" * 60)
        print("              GCON CLUSTER DASHBOARD")
        print("=" * 60)

        #
        # Node Information
        #
        print("\nNODES")
        print("-" * 60)

        for node in self.nodes:

            cpu = node["cpu"] if node["cpu"] is not None else "N/A"
            memory = node["memory"] if node["memory"] is not None else "N/A"

            print(
                f"{node['node_id']:<12}"
                f"{node['status']:<10}"
                f"CPU: {cpu}%   "
                f"MEM: {memory}%   "
                f"Jobs: {node['running_jobs']}"
            )

        #
        # Job Information
        #
        print("\nJOBS")
        print("-" * 60)

        for job in self.jobs:

            print(
                f"{job['job_id']:<12}"
                f"{job['status']:<12}"
                f"{job['node_id']}"
            )

        #
        # Cluster Summary
        #
        print("\nCLUSTER SUMMARY")
        print("-" * 60)

        print(f"Total Nodes      : {self.cluster['total_nodes']}")
        print(f"Online Nodes     : {self.cluster['online_nodes']}")
        print(f"Total Jobs       : {self.cluster['total_jobs']}")
        print(f"Running Jobs     : {self.cluster['running_jobs']}")
        print(f"Completed Jobs   : {self.cluster['completed_jobs']}")
        print(f"Total Receipts   : {self.cluster['total_receipts']}")
        print(f"Total Artifacts  : {self.cluster['total_artifacts']}")
        
        print("\nRECENT EVENTS")
        print("-" * 60)

        for event in self.events[-10:]:
            print(self.format_event(event))
            print("-" * 60)
             

        print("=" * 60)