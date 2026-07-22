import os
import shutil

class StorageManager:
    """
    Manages distributed artifact storage across GCON nodes.
    """

    def __init__(self, storage_root="storage"):
        self.storage_root = storage_root

        os.makedirs(self.storage_root, exist_ok=True)
        
    def initialize_node_storage(self, node_id):
        """
        Create a storage directory for a node.
        """

        node_path = os.path.join(self.storage_root, node_id)

        os.makedirs(node_path, exist_ok=True)

        return node_path
    
    def store_artifact(self, node_id, source_path):
        """
        Store an artifact inside a node's storage directory.
        Returns the stored file path.
        """

        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"Artifact not found: {source_path}")

        node_path = self.initialize_node_storage(node_id)

        filename = os.path.basename(source_path)
        destination = os.path.join(node_path, filename)

        shutil.copy2(source_path, destination)
        return destination
    
    def retrieve_artifact(self, node_id, filename):
        """
       Retrieve the full path to an artifact stored on a node.

        Returns:
            str: Full path to the artifact.

        Raises:
            FileNotFoundError: If the artifact does not exist.
        """

        artifact_path = os.path.join(
            self.storage_root,
            node_id,
            filename
    )

        if not os.path.isfile(artifact_path):
            raise FileNotFoundError(
                f"Artifact '{filename}' not found on node '{node_id}'."
        )

        return artifact_path
    
    def copy_artifact(self, source_node, destination_node, filename):
        """
        Copy an artifact from one node's storage to another.
        Returns:
            str: Path to the copied artifact.
        """
        source_path = self.retrieve_artifact(
            source_node,
            filename
    )
        destination_directory = self.initialize_node_storage(
            destination_node
    )
        destination_path = os.path.join(
            destination_directory,
            filename
    )
        shutil.copy2(source_path, destination_path)
        return destination_path
    
    def delete_artifact(self, node_id, filename):
        """
        Delete an artifact from a node's storage.

        Returns:
            True if the artifact was deleted.
            False if it did not exist.
        """

        artifact_path = os.path.join(
            self.storage_root,
            node_id,
            filename
    )
         
        # Already gone?
        if not os.path.isfile(artifact_path):
            return False

        
        # Windows may temporarily prevent deletion while another
        # thread is reading the file.
        for _ in range(20):          # ~200 ms total
            try:              
                os.remove(artifact_path)
                return True

            
            except PermissionError:
                time.sleep(0.01)

            except FileNotFoundError:
                return False

        # If it's still locked after retries, propagate the error.
        raise
    
    def list_node_artifacts(self, node_id):
        """
        Return a list of artifact filenames stored on a node.
        """

        node_path = os.path.join(
            self.storage_root,
            node_id
    )

        if not os.path.isdir(node_path):
            return []

        artifacts = []

        for entry in os.listdir(node_path):
            entry_path = os.path.join(node_path, entry)

            if os.path.isfile(entry_path):
                artifacts.append(entry)

        return sorted(artifacts)   