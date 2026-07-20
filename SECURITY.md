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

- **Email**: [Add maintainer email]
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

- ✅ **DO**: Generate RSA-2048+ keys for all agents
- ✅ **DO**: Store private keys with restricted permissions (0600)
- ✅ **DO**: Back up private keys securely (offline, encrypted)
- ✅ **DO**: Use Hardware Security Modules (HSM) for production
- ❌ **DON'T**: Commit private keys to version control
- ❌ **DON'T**: Share private keys
- ❌ **DON'T**: Use the same key across multiple agents

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

- Use established libraries (e.g., cryptography, pycryptodome)
- Never roll your own crypto
- Use RSA-PSS with SHA-256 for signing
- Validate signatures before trusting receipts

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

### Current (v0.10)

1. **No Authentication**: Local network assumed to be trusted
   - **Mitigation**: Deploy behind VPN or private network
   - **Future**: JWT bearer tokens planned for v0.11

2. **No Encryption at Rest**: Job metadata stored in plain text
   - **Mitigation**: Restrict access to coordinator's storage
   - **Future**: AES-256 encryption planned

3. **In-Memory State**: No persistence across restarts
   - **Mitigation**: Deploy coordinator with persistent storage backend
   - **Future**: PostgreSQL backend in development

4. **Agent Registration** is unprotected
   - **Mitigation**: Restrict access to coordinator's registration endpoint
   - **Future**: Pre-shared secrets or mTLS planned

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

- **Confidentiality**: Private keys never leave agents
- **Integrity**: Receipts are cryptographically signed
- **Authenticity**: Agents prove identity via key signatures
- **Non-repudiation**: Signed receipts provide audit trail

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
