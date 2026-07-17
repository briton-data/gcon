"""
GCON Key Manager

Handles generation and loading of Ed25519 cryptographic keys.
"""

from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


class KeyManager:
    """Handles generation, storage, and loading of Ed25519 keys."""

    def __init__(self, key_dir: str = "keys"):
        """
        Initialize the key manager.

        Args:
            key_dir: Directory where keys are stored.
        """
        self.key_dir = Path(key_dir)
        self.private_key_path = self.key_dir / "private_key.pem"
        self.public_key_path = self.key_dir / "public_key.pem"

        self.key_dir.mkdir(parents=True, exist_ok=True)

    def generate_keypair(self):
        """
        Generate a new Ed25519 key pair and save it to disk.
        """

        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        # Save private key
        with open(self.private_key_path, "wb") as f:
            f.write(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        # Save public key
        with open(self.public_key_path, "wb") as f:
            f.write(
                public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            )

    def ensure_keys_exist(self):
        """
        Generate keys if they do not already exist.
        """

        if (
            not self.private_key_path.exists()
            or not self.public_key_path.exists()
        ):
            self.generate_keypair()

    def load_private_key(self) -> Ed25519PrivateKey:
        """
        Load the private key from disk.
        """
        self.ensure_keys_exist()

        with open(self.private_key_path, "rb") as f:
            return serialization.load_pem_private_key(
                f.read(),
                password=None,
            )

    def load_public_key(self) -> Ed25519PublicKey:
        """
        Load the public key from disk.
        """
        self.ensure_keys_exist()

        with open(self.public_key_path, "rb") as f:
            return serialization.load_pem_public_key(
                f.read()
            )