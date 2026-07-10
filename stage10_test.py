from network import GCONNetwork
from coordinator import GCONCoordinator
from agent import GCONAgent
from node import GCONNode
from metrics import MetricsCollector, MetricsSummary
from dashboard import Dashboard

print("=" * 70)
print("        GCON STAGE 10 - RESOURCE MONITORING & LOAD SCHEDULING TEST")
print("=" * 70)


coordinator = GCONCoordinator()

node1 = GCONAgent("node-001")
node2 = GCONAgent("node-002")
node3 = GCONAgent("node-003")

coordinator.register_agent(node1)
coordinator.register_agent(node2)
coordinator.register_agent(node3)

node1.start_heartbeat(coordinator)
node2.start_heartbeat(coordinator)
node3.start_heartbeat(coordinator)

print("\n=== RESOURCE REPORTS ===")

for node in [node1, node2, node3]:
    resources = node.report_resources()
    coordinator.receive_resource_report(resources)

    print(
        node.node_id,
        "| CPU =", resources["cpu"],
        "| Memory =", resources["memory"],
        "| Jobs =", resources["running_jobs"]
    )

coordinator.registry.nodes["node-001"]["cpu"] = 70
coordinator.registry.nodes["node-001"]["memory"] = 80

coordinator.registry.nodes["node-002"]["cpu"] = 25
coordinator.registry.nodes["node-002"]["memory"] = 40

coordinator.registry.nodes["node-003"]["cpu"] = 10
coordinator.registry.nodes["node-003"]["memory"] = 20

selected = coordinator.scheduler.select_node()

assert selected.node_id == "node-003"

info = coordinator.registry.get_node_info("node-001")

print("\n=== HEARTBEAT TEST ===")
print(f"Node: node-001")
print(f"Status: {info['status']}")
print(f"Last Seen: {info['last_seen']}")


print("\nRegistered Nodes:")
print(coordinator.registry.list_nodes())

# --------------------------------------------------
# Submit a job
# --------------------------------------------------

coordinator.submit_job(
    "job-001",
    " GCON STAGE 10 - RESOURCE MONITORING & LOAD SCHEDULING TEST    Working"
)

print("\nSubmitting job...")

result = coordinator.assign_job("job-001")

# --------------------------------------------------
# Results
# --------------------------------------------------

print("\n=== JOB RESULT ===")
print(result)

print("\n=== NODE STATUS ===")

for node_id in coordinator.registry.list_nodes():
    node = coordinator.registry.get_node(node_id)
    print(f"{node.node_id}: {node.status}")

print("\n=== JOB STATUS ===")
print(coordinator.get_job_status("job-001")) 

print("\nGCONSTAGE 10 - RESOURCE MONITORING & LOAD SCHEDULING TEST Complete.")

#
print("\nStopping heartbeat for node-001...\n")
node1.stop_heartbeat()

import time

print("\nWaiting for heartbeat timeout...")
time.sleep(12)



print("\n=== BEFORE HEALTH CHECK ===")

for node_id in coordinator.registry.list_nodes():
    info = coordinator.registry.get_node_info(node_id)
    print(
        node_id,
        "| status =", info["status"],
        "| last_seen =", info["last_seen"]
    )


coordinator.check_cluster_health()

print("\n=== SCHEDULER FAILURE TEST ===")

try:
    coordinator.submit_job(
        "job-002",
        "echo This job should not run"
    )

    coordinator.assign_job("job-002")

except RuntimeError as e:
    print(e)


print("\n=== FAILOVER SCHEDULER TEST ===")

selected = coordinator.scheduler.select_node()

print(f"Scheduler selected: {selected.node_id}")

assert selected.node_id != "node-001"

print("PASS: Offline node was ignored.")

print("\n=== AFTER HEALTH CHECK ===")

for node_id in coordinator.registry.list_nodes():
    info = coordinator.registry.get_node_info(node_id)
    print(
        node_id,
        "| status =", info["status"],
        "| last_seen =", info["last_seen"]
    )


print("\n=== NODE HEALTH ===")

for node_id in coordinator.registry.list_nodes():
    info = coordinator.registry.get_node_info(node_id)

    print(node_id, "->", info["status"])
    
collector = MetricsCollector(coordinator)

print("\n=== NODE METRICS ===")
print(collector.collect_node_metrics())

print("\n=== JOB METRICS ===")
print(collector.collect_job_metrics())

collector = MetricsCollector(coordinator)

node_metrics = collector.collect_node_metrics()
job_metrics = collector.collect_job_metrics()

summary = MetricsSummary(node_metrics, job_metrics)

print("\n=== NODE SUMMARY ===")
print(summary.summarize_nodes())

print("\n=== JOB SUMMARY ===")
print(summary.summarize_jobs())

print("\n=== RESOURCE SUMMARY ===")
print(summary.summarize_resources())

print("\n=== CLUSTER SUMMARY ===")
print(summary.cluster_summary())

print("\n=== DASHBOARD ===")
coordinator.dashboard()