import os

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent

print("=" * 60)
print("STAGE 17 - DISTRIBUTED STORAGE INTEGRATION TEST")
print("=" * 60)

# ==========================================================
# Initialize Coordinator
# ==========================================================

coordinator = GCONCoordinator()

# ==========================================================
# Register Agent
# ==========================================================

print("\nRegistering agent...")

agent = GCONAgent("node-001")
coordinator.register_agent(agent)

print("PASS: Agent registered.")

# ==========================================================
# Create Completed Job
# ==========================================================

print("\nCreating completed job...")

job_id = "job-001"

coordinator.jobs[job_id] = {
    "command": "echo Hello GCON",
    "agent": "node-001",
    "status": "completed",
    "artifacts": []
}

print("PASS: Job created.")

# ==========================================================
# Create Sample Artifact
# ==========================================================

print("\nCreating sample artifact...")

sample_file = "sample_artifact.txt"

with open(sample_file, "w") as f:
    f.write("Hello from GCON Stage 17!")

print("PASS: Sample artifact created.")

# ==========================================================
# Register Artifact Through Coordinator
# ==========================================================

print("\nRegistering artifact through coordinator...")

artifact_id = coordinator.register_job_artifact(
    job_id,
    "node-001",
    sample_file
)

print(f"Artifact ID: {artifact_id}")
print("PASS: Artifact registered.")

# ==========================================================
# Verify Artifact Stored
# ==========================================================

print("\nVerifying storage...")

stored_path = coordinator.storage_manager.retrieve_artifact(
    "node-001",
    "sample_artifact.txt"
)

assert os.path.isfile(stored_path)

print("PASS: Artifact stored successfully.")

# ==========================================================
# Verify Registry
# ==========================================================

print("\nVerifying registry...")

artifact = coordinator.artifact_registry.get_artifact(
    artifact_id
)

assert artifact is not None

print("PASS: Artifact registry verified.")

# ==========================================================
# Verify SHA256
# ==========================================================

print("\nVerifying SHA-256...")

assert coordinator.artifact_registry.verify_artifact(
    artifact_id
)

print("PASS: SHA-256 verification successful.")

# ==========================================================
# Verify Job Link
# ==========================================================

print("\nVerifying job linkage...")

assert artifact_id in coordinator.jobs[job_id]["artifacts"]

print("PASS: Job linked to artifact.")

# ==========================================================
# Register Second Agent
# ==========================================================

print("\nRegistering second agent...")

agent2 = GCONAgent("node-002")
coordinator.register_agent(agent2)

print("PASS: Second agent registered.")

# ==========================================================
# Copy Artifact
# ==========================================================

print("\nCopying artifact to node-002...")

copied_path = coordinator.storage_manager.copy_artifact(
    "node-001",
    "node-002",
    "sample_artifact.txt"
)

assert os.path.isfile(copied_path)

print("PASS: Artifact copied.")

# ==========================================================
# List Node Artifacts
# ==========================================================

print("\nListing node artifacts...")

node1 = coordinator.storage_manager.list_node_artifacts("node-001")
node2 = coordinator.storage_manager.list_node_artifacts("node-002")

print("Node-001:", node1)
print("Node-002:", node2)

assert "sample_artifact.txt" in node1
assert "sample_artifact.txt" in node2

print("PASS: Node artifact listing verified.")

# ==========================================================
# Delete Replica
# ==========================================================

print("\nDeleting replica from node-002...")

deleted = coordinator.storage_manager.delete_artifact(
    "node-002",
    "sample_artifact.txt"
)

assert deleted

print("PASS: Replica deleted.")

# ==========================================================
# Verify Replica Deleted
# ==========================================================

print("\nVerifying replica deletion...")

try:
    coordinator.storage_manager.retrieve_artifact(
        "node-002",
        "sample_artifact.txt"
    )
    raise AssertionError("Replica still exists.")

except FileNotFoundError:
    print("PASS: Replica removed.")

# ==========================================================
# Verify Original Exists
# ==========================================================

print("\nVerifying original artifact...")

original = coordinator.storage_manager.retrieve_artifact(
    "node-001",
    "sample_artifact.txt"
)

assert os.path.isfile(original)

print("PASS: Original artifact still exists.")

# ==========================================================
# Cleanup
# ==========================================================

print("\nCleaning up...")

os.remove(sample_file)

print("PASS: Temporary source file removed.")

print("\n" + "=" * 60)
print("STAGE 17 PASSED")
print("=" * 60)