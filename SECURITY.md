# GCON Security Policy

## Reporting Security Vulnerabilities

If you discover a security vulnerability in GCON, please **do not** open a public GitHub issue. Instead, please report it responsibly to the maintainers.

### Reporting Process

1. **Email**: Send details to the project maintainers (contact info below)
2. **Include**:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if you have one)
3. **Timeline**: We will acknowledge receipt within 48 hours and provide updates every 5 days
4. **Disclosure**: We will coordinate a fix and responsible disclosure timeline with you

### Contact

- **Email**: [nyongesabriton620@gmail.com]
- **GitHub**: [@briton-data](https://github.com/briton-data)

---

## Security Best Practices

### For Users

#### API Key Management

- ✅ **DO**: Store API keys in environment variables or secure key management systems
- ✅ **DO**: Rotate API keys periodically
- ✅ **DO**: Use different keys for different environments (dev/staging/prod)
- ❌ **DON'T**: Commit API keys to version control
- ❌ **DON'T**: Share API keys via email or Slack
- ❌ **DON'T**: Use the same API key across multiple applications

#### Network Security

- ✅ **DO**: Run the coordinator behind a reverse proxy (nginx/HAProxy) with SSL/TLS
- ✅ **DO**: Restrict coordinator access to trusted networks (firewall rules)
- ✅ **DO**: Use VPNs or private networks for agent-coordinator communication
- ❌ **DON'T**: Expose the coordinator directly to the internet without auth
- ❌ **DON'T**: Use HTTP without TLS in production

#### Cryptographic Keys

GCON's real key model has two distinct parts — don't apply RSA-specific
advice to either of them by mistake:

- **Agent transport identity**: X.509 client certificates (mTLS),
  issued by a shared CA via `scripts/generate_dev_certs.py` (dev/
  self-managed CA) or your own CA. The certificate Common Name is the
  node's identity, checked both at the TLS handshake and again in-band
  against the claimed `node_id`.
- **Receipt signing**: HMAC-SHA256, coordinator-held key
  (`gcon.execution.hmac_keyring.HmacKeyring`) — not per-agent, not
  RSA/asymmetric. See `docs/DEPLOYMENT.md`'s Cryptographic Keys section
  and `docs/ARCHITECTURE.md`'s Security Model for the full picture,
  including the honest limitation this implies (a receipt proves the
  coordinator's key signed it, not that the specific node independently
  attests to it with its own key).

- ✅ **DO**: Use `scripts/generate_dev_certs.py` (or your real CA) to
  issue each agent its own certificate — never share one cert/key pair
  across agents
- ✅ **DO**: Store private keys (both cert keys and
  `GCON_HMAC_KEY_PATH`) with restricted permissions (0600)
- ✅ **DO**: Back up private keys securely (offline, encrypted)
- ✅ **DO**: Protect key files at the OS/filesystem level in production
  (HSM/KMS integration is not built into GCON itself — see Known
  Limitations)
- ❌ **DON'T**: Commit private keys or certs to version control
- ❌ **DON'T**: Share private keys or certificates across agents

#### Receipt Verification

- ✅ **DO**: Verify receipt signatures before trusting execution results
- ✅ **DO**: Check receipt timestamps (reject stale receipts)
- ✅ **DO**: Audit and log receipt verification failures
- ✅ **DO**: Require verified receipts for payment/critical workflows

### For Developers

#### Input Validation

- Validate all user inputs (job IDs, commands, JSON)
- Sanitize inputs before logging or displaying
- Reject inputs with unexpected characters or sizes
- Use strong typing and schemas

#### Authentication & Authorization

- Implement API key validation on all endpoints
- Check permissions before allowing operations
- Log all authentication failures
- Use HTTPS/TLS for all API calls

#### Cryptography

- Use established libraries (e.g., `cryptography`, already a dependency
  for the mTLS/CA tooling) — never roll your own crypto
- GCON's actual receipt signing is HMAC-SHA256 (`ExecutionVerifier`), not
  RSA-PSS — if you're extending signing, match the existing scheme or
  document clearly why you're introducing a second one (see
  `receipt.py`'s module docstring for what happened last time this
  codebase had two parallel signing schemes: one quietly went unused and
  fell out of sync)
- Always validate signatures live via `validate_proof()` before trusting
  a receipt — never read a stored `verified`-like field (see
  `docs/ARCHITECTURE.md`'s Security Model for why that's explicitly not
  safe to do)

#### Data Protection

- Encrypt sensitive data at rest (API keys, private keys)
- Use TLS/SSL for data in transit
- Avoid logging sensitive data (commands, outputs, keys)
- Implement proper access controls

#### Dependency Management

- Keep dependencies up to date
- Monitor for security advisories: `pip-audit`
- Use dependency pinning for reproducibility
- Review dependencies before adding

```bash
# Check for known vulnerabilities
pip-audit
```

#### Error Handling

- Don't expose sensitive info in error messages
- Log errors securely (avoid logging credentials)
- Use generic error messages for users
- Provide detailed logs only to authorized personnel

---

## Known Limitations

**This section previously described GCON as having no authentication, no
persistence, and unprotected agent registration. That was accurate for
an earlier version of the codebase; it is not accurate now.** Rewritten
against the real source (`gcon.transport.tls`, `gcon.transport.grpc_transport`,
`gcon.persistence`) rather than an outdated snapshot:

### What's actually built (previously listed as missing)

1. **Agent authentication is real: mutual TLS.** Every agent connects
   over an mTLS gRPC channel; the coordinator requires a client
   certificate (`require_client_auth=True`) and rejects any connection
   without one during the handshake, before any RPC executes. The
   certificate's Common Name is checked again in-band against the
   claimed `node_id` at `Register` time — an agent cannot register as an
   identity it doesn't hold a certificate for. See
   `docs/TRANSPORT_AND_PERSISTENCE.md`'s Mutual authentication section.

2. **Agent registration is protected**, not open — see the mTLS point
   above. It is not "restrict access to the registration endpoint
   yourself"; the protection is built into the handshake and the
   `Register` RPC itself.

3. **State is durably persisted**, not in-memory-only. The control plane
   (`gcon.persistence`, SQLite today, WAL mode, foreign keys on,
   Postgres-compatible by construction) survives coordinator restarts —
   see `docs/TRANSPORT_AND_PERSISTENCE.md`. "In-memory" only describes
   the coordinator's *live* scheduling state (which node is currently
   busy, etc.), which is expected to be rebuilt from durable records and
   fresh agent reconnections after a restart, not a gap in persistence
   itself.

4. **The public API requires authentication unconditionally.** Every
   `/api/v1` route needs a real API key (`Authorization: Bearer ...` or
   `X-API-Key: ...`); there is no dev/bypass mode. See `docs/API.md`.

### What's still genuinely limited, as of this doc

1. **Receipts are coordinator-signed, not node-signed.** The HMAC key
   that signs every receipt belongs to the coordinator process, not the
   individual agent that ran the job — a receipt proves "the coordinator
   recorded this," not independently "node X attests to this with its
   own key." `attested_node_id` (binding a receipt to the coordinator's
   own record of an mTLS-verified identity) is real and narrows this
   gap, but doesn't close it — true per-node non-repudiation (the node
   signs with its own key, independently checkable without trusting the
   coordinator's key custody) is not built. See
   `docs/ARCHITECTURE.md`'s Security Model.

2. **No encryption at rest for the control-plane database.** Job
   metadata, receipts, and other control-plane data are stored in plain
   SQLite files.
   - **Mitigation**: restrict filesystem access to the coordinator's
     `data/` directory; encrypt the underlying disk/volume if your
     threat model requires it.

3. **No HSM/KMS integration.** Both the mTLS private keys and the HMAC
   signing key are plain files on disk, protected only by filesystem
   permissions — GCON doesn't integrate with an HSM, TPM, or cloud KMS
   itself.
   - **Mitigation**: protect key files at the OS/filesystem/disk-encryption
     level; consider a KMS-backed wrapper around key file access if
     required.

4. **Coordinator HA is real but limited.** `--ha` lease-based failover
   (see `docs/FAILOVER.md`) exists, but is safe only for multiple
   coordinator processes on **one host** sharing one SQLite file — not
   verified-safe across hosts on a network filesystem, not zero-downtime,
   not active-active, and includes no VIP/load-balancer integration.

5. **Session cookies require `GCON_FORCE_HTTPS=1` to be marked
   `Secure`** — it's not automatic; an operator running the dashboard
   over plain HTTP (e.g. behind a proxy that doesn't set
   `GCON_FORCE_HTTPS`) gets a cookie without the `Secure` flag. Always
   terminate TLS somewhere in front of a production deployment (either
   `GCON_FORCE_HTTPS=1` on the coordinator itself, or a reverse proxy —
   see `docs/DEPLOYMENT.md`).

---

## Security Updates

We release security patches for:

- ✅ Critical vulnerabilities (immediate)
- ✅ High-severity vulnerabilities (within 7 days)
- ✅ Medium-severity vulnerabilities (within 30 days)

Check [CHANGELOG.md](CHANGELOG.md) for security-related updates.

---

## Compliance

GCON is designed with the following security principles:

- **Confidentiality**: each agent's mTLS private key never leaves that
  agent
- **Integrity**: receipts are cryptographically signed (HMAC-SHA256);
  tampering with a signed field is detectable via `validate_proof()`
- **Authenticity**: agents prove their identity via mTLS certificate
  possession during the transport handshake, re-checked against the
  claimed `node_id` at registration
- **Non-repudiation: partial, not full.** A receipt's signature proves
  the coordinator's key signed it and the content hasn't been altered
  since — it does not, on its own, let a third party independently
  verify which specific node produced the underlying result, since
  signing is coordinator-side (see `docs/ARCHITECTURE.md`'s Security
  Model). `attested_node_id` narrows this by binding a receipt to the
  coordinator's own record of an mTLS-verified node identity, but true
  per-node non-repudiation (the node signs with its own key) isn't
  built. Don't represent GCON receipts as providing full non-repudiation
  in a compliance context without accounting for this.

---

## Security Audit

This project has not undergone a formal security audit. If you're using GCON for critical workloads, consider:

- Commissioning an independent security audit
- Having security experts review the codebase
- Implementing additional monitoring and logging
- Using in a restricted, trusted environment first

---

## Vulnerability Disclosure Timeline

When we receive a vulnerability report:

1. **T+0h**: Acknowledge receipt of report
2. **T+24h**: Confirm vulnerability and start fix
3. **T+5d**: Provide fix timeline or workaround
4. **T+14d**: Release patch (or request 90-day extension)
5. **T+30d**: Public disclosure of vulnerability and fix

---

## Questions?

If you have questions about security, please open a GitHub Discussion (not an issue).
