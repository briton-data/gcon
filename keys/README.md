# GCON Cryptographic Keys

This directory stores the cryptographic key material used by GCON.

During development the keys are generated locally.

Production deployments should use secure key storage such as:

- Hardware Security Modules (HSM)
- Trusted Platform Modules (TPM)
- Cloud Key Management Services
- Trusted Execution Environments (TEE)

Private keys must never be committed to source control.