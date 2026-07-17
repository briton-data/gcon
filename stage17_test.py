import os

from gcon.storage.storage_manager import StorageManager
from gcon.execution.artifact_registry import ArtifactRegistry

print("=" * 60)
print("STAGE 17 - DISTRIBUTED STORAGE TEST")
print("=" * 60)

storage = StorageManager()
registry = ArtifactRegistry()

# ---------------------------------------------------------
# Initialize node storage
# ---------------------------------------------------------

print("\nInitializing node storage...")

storage.initialize_node_storage("node-001")
storage.initialize_node_storage("node-002")
storage.initialize_node_storage("node-003")

print("PASS: Node storage initialized.")

# ---------------------------------------------------------
# Create a sample artifact
# ---------------------------------------------------------

print("\nCreating sample artifact...")

sample_file = "sample_artifact.txt"

with open(sample_file, "w") as f:
    f.write("Hello from GCON Stage 17!")

print(f"Created: {sample_file}")

# ---------------------------------------------------------
# Store artifact on node-001
# ---------------------------------------------------------

print("\nStoring artifact on node-001...")

stored_path = storage.store_artifact("node-001", sample_file)

assert os.path.isfile(stored_path)

print(f"Stored at: {stored_path}")
print("PASS: Artifact stored successfully.")

# ---------------------------------------------------------
# Register artifact
# ---------------------------------------------------------

print("\nRegistering artifact...")

artifact_id = registry.register_artifact(stored_path)

artifact = registry.get_artifact(artifact_id)

assert artifact is not None

print(f"Artifact ID : {artifact.artifact_id}")
print(f"Filename    : {artifact.filename}")
print(f"Size        : {artifact.size} bytes")

print("PASS: Artifact registered.")

# ---------------------------------------------------------
# Verify checksum
# ---------------------------------------------------------

print("\nVerifying artifact...")

assert registry.verify_artifact(artifact_id)

print("PASS: SHA-256 verification successful.")

# ---------------------------------------------------------
# List artifacts
# ---------------------------------------------------------

print("\nListing registered artifacts...")

for artifact in registry.list_artifacts():
    print(
        f"{artifact.artifact_id} | "
        f"{artifact.filename} | "
        f"{artifact.size} bytes"
    )
# ---------------------------------------------------------
# Retrieve artifact
# ---------------------------------------------------------

print("\nRetrieving artifact...")

retrieved_path = storage.retrieve_artifact(
    "node-001",
    "sample_artifact.txt"
)

assert retrieved_path == stored_path

print(f"Retrieved Path: {retrieved_path}")
print("PASS: Artifact retrieved successfully.")

# # ---------------------------------------------------------
# Copy artifact to another node
# ---------------------------------------------------------

print("\nCopying artifact to node-002...")

copied_path = storage.copy_artifact(
    "node-001",
    "node-002",
    "sample_artifact.txt"
)

assert os.path.isfile(copied_path)

print(f"Copied To: {copied_path}")
print("PASS: Artifact copied successfully.")
# ---------------------------------------------------------
# Retrieve copied artifact
# ---------------------------------------------------------

print("\nRetrieving copied artifact...")

retrieved_copy = storage.retrieve_artifact(
    "node-002",
    "sample_artifact.txt"
)

assert retrieved_copy == copied_path

print(f"Retrieved Copy: {retrieved_copy}")
print("PASS: Copied artifact retrieved successfully.")

# ---------------------------------------------------------
# List node artifacts
# ---------------------------------------------------------

print("\nListing node artifacts...")

node1_artifacts = storage.list_node_artifacts("node-001")
node2_artifacts = storage.list_node_artifacts("node-002")
node3_artifacts = storage.list_node_artifacts("node-003")

print(f"Node-001: {node1_artifacts}")
print(f"Node-002: {node2_artifacts}")
print(f"Node-003: {node3_artifacts}")

assert "sample_artifact.txt" in node1_artifacts
assert "sample_artifact.txt" in node2_artifacts
assert node3_artifacts == []

print("PASS: Node artifact listing verified.")

# ---------------------------------------------------------
# Delete artifact from node-002
# ---------------------------------------------------------

print("\nDeleting artifact from node-002...")

deleted = storage.delete_artifact(
    "node-002",
    "sample_artifact.txt"
)

assert deleted

print("PASS: Artifact deleted successfully.")

# ---------------------------------------------------------
# Verify deletion
# ---------------------------------------------------------

print("\nVerifying deletion...")

try:
    storage.retrieve_artifact(
        "node-002",
        "sample_artifact.txt"
    )

    raise AssertionError(
        "Artifact still exists after deletion."
    )

except FileNotFoundError:
    print("PASS: Artifact removed from node-002.")
    
# ---------------------------------------------------------
# Verify original artifact still exists
# ---------------------------------------------------------

print("\nVerifying original artifact...")

original_path = storage.retrieve_artifact(
    "node-001",
    "sample_artifact.txt"
)

assert os.path.isfile(original_path)

print("PASS: Original artifact still exists on node-001.")

print("\nCleaning up...")
os.remove(sample_file)

print("PASS: Temporary source file removed.")

print("\n" + "=" * 60)
print("STAGE 17 TEST PASSED")
print("=" * 60)