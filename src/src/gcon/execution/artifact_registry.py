import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, UTC


@dataclass
class Artifact:
    """
    Represents a registered artifact.
    """
    artifact_id: str
    filename: str
    filepath: str
    sha256: str
    size: int
    uploaded_at: str


class ArtifactRegistry:
    """
    Maintains metadata for all registered artifacts.
    """

    def __init__(self):
        self.artifacts = {}
        self.filename_index = {}
        self.next_artifact_id = 1

    def _generate_artifact_id(self):
        """
        Generate a unique artifact ID.
        Example: ART-001
        """
        artifact_id = f"ART-{self.next_artifact_id:03d}"
        self.next_artifact_id += 1
        return artifact_id

    def _calculate_sha256(self, filepath):
        """
        Calculate the SHA-256 checksum of a file.
        """
        sha256 = hashlib.sha256()

        with open(filepath, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                sha256.update(chunk)

        return sha256.hexdigest()

    def register_artifact(self, filepath):
        """
        Register a file as an artifact.
        """

        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Artifact not found: {filepath}")

        filename = os.path.basename(filepath)

        # Prevent duplicate registrations
        if filename in self.filename_index:
            existing_id = self.filename_index[filename]
            return existing_id

        artifact = Artifact(
            artifact_id=self._generate_artifact_id(),
            filename=filename,
            filepath=os.path.abspath(filepath),
            sha256=self._calculate_sha256(filepath),
            size=os.path.getsize(filepath),
            uploaded_at=datetime.now(UTC).isoformat()
        )

        self.artifacts[artifact.artifact_id] = artifact
        self.filename_index[filename] = artifact.artifact_id

        return artifact.artifact_id

    def get_artifact(self, artifact_id):
        """
        Retrieve an artifact by ID.
        """
        return self.artifacts.get(artifact_id)

    def list_artifacts(self):
        """
        Return all registered artifacts.
        """
        return list(self.artifacts.values())

    def artifact_exists(self, filename):
        """
        Check whether a filename has been registered.
        """
        return filename in self.filename_index

    def verify_artifact(self, artifact_id):
        """
        Verify that the stored checksum matches the current file.
        """
        artifact = self.get_artifact(artifact_id)

        if artifact is None:
            return False

        if not os.path.isfile(artifact.filepath):
            return False

        current_hash = self._calculate_sha256(artifact.filepath)

        return current_hash == artifact.sha256

    def remove_artifact(self, artifact_id):
        """
        Remove an artifact from the registry.
        Does NOT delete the actual file.
        """
        artifact = self.artifacts.pop(artifact_id, None)

        if artifact is None:
            return False

        self.filename_index.pop(artifact.filename, None)

        return True

    def get_artifact_by_filename(self, filename):
        """
        Retrieve artifact metadata using its filename.
        """
        artifact_id = self.filename_index.get(filename)

        if artifact_id is None:
            return None

        return self.artifacts.get(artifact_id)

    def artifact_count(self):
        """
        Return the total number of registered artifacts.
        """
        return len(self.artifacts)

    def clear(self):
        """
        Clear the registry.
        Primarily intended for testing.
        """
        self.artifacts.clear()
        self.filename_index.clear()
        self.next_artifact_id = 1