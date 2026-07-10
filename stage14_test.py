import os

from artifact_registry import ArtifactRegistry


print("=" * 60)
print("GCON STAGE 14 - ARTIFACT REGISTRY TEST")
print("=" * 60)

registry = ArtifactRegistry()

print("[PASS] Artifact Registry Initialized")


# --------------------------------------------------
# Create a temporary artifact
# --------------------------------------------------

filename = "stage14_sample.txt"

with open(filename, "w") as file:
    file.write("Hello GCON Artifact Registry")


# --------------------------------------------------
# Register Artifact
# --------------------------------------------------

artifact_id = registry.register_artifact(filename)

print(f"[PASS] Registered Artifact: {artifact_id}")


# --------------------------------------------------
# Retrieve Artifact
# --------------------------------------------------

artifact = registry.get_artifact(artifact_id)

assert artifact is not None

print(f"[PASS] Retrieved Artifact: {artifact.filename}")


# --------------------------------------------------
# Artifact Exists
# --------------------------------------------------

assert registry.artifact_exists(filename)

print("[PASS] Artifact Exists")


# --------------------------------------------------
# Verify SHA256
# --------------------------------------------------

assert registry.verify_artifact(artifact_id)

print("[PASS] SHA256 Verification Successful")


# --------------------------------------------------
# Corrupt File
# --------------------------------------------------

with open(filename, "a") as file:
    file.write("\nCorrupted")


assert registry.verify_artifact(artifact_id) is False

print("[PASS] Corruption Successfully Detected")


# --------------------------------------------------
# Remove Artifact
# --------------------------------------------------

assert registry.remove_artifact(artifact_id)

print("[PASS] Artifact Removed")


# --------------------------------------------------
# Verify Removal
# --------------------------------------------------

assert registry.get_artifact(artifact_id) is None

print("[PASS] Artifact No Longer Exists")


# --------------------------------------------------
# Cleanup
# --------------------------------------------------

os.remove(filename)

registry.clear()

print("[PASS] Cleanup Complete")


print()
print("=" * 60)
print("STAGE 14 PASSED")
print("=" * 60)