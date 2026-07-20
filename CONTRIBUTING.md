# Contributing to GCON

Thank you for your interest in contributing to GCON! This document provides guidelines and instructions for contributing.

---

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Workflow](#development-workflow)
4. [Testing](#testing)
5. [Code Style](#code-style)
6. [Commit Messages](#commit-messages)
7. [Pull Request Process](#pull-request-process)
8. [Reporting Bugs](#reporting-bugs)
9. [Suggesting Features](#suggesting-features)
10. [Project Governance](#project-governance)

---

## Code of Conduct

This project adheres to the **Contributor Covenant Code of Conduct**.

### Our Pledge

We are committed to providing a welcoming and inspiring community for all, regardless of age, body size, disability, ethnicity, gender identity, level of experience, nationality, personal appearance, race, religion, sexual identity, or sexual orientation.

### Expected Behavior

- **Be respectful** — Treat all community members with respect
- **Be inclusive** — Welcome people of all backgrounds
- **Be constructive** — Provide helpful feedback
- **Be professional** — Keep discussions on-topic and productive

### Unacceptable Behavior

- Harassment, discrimination, or intimidation
- Hate speech or dehumanizing language
- Unsolicited sexual advances or comments
- Deliberate disruption or trolling

### Reporting Violations

If you witness or experience a violation, please report it to the maintainers at the project repository.

---

## Getting Started

### Prerequisites

- Python 3.12+
- Git
- GitHub account

### Fork & Clone

```bash
# 1. Fork the repository on GitHub (click "Fork" button)

# 2. Clone your fork
git clone https://github.com/YOUR_USERNAME/gcon.git
cd gcon

# 3. Add upstream remote (for keeping in sync)
git remote add upstream https://github.com/briton-data/gcon.git
```

### Set Up Development Environment

```bash
# Create virtual environment
python3.12 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -r requirements-dev.txt  # Includes pytest, black, flake8, etc.

# Run tests to verify setup
pytest
```

---

## Development Workflow

### 1. Create a Branch

```bash
# Keep main branch clean
git checkout main
git pull upstream main

# Create a feature branch with descriptive name
git checkout -b feature/short-description
# Examples:
# - feature/add-job-cancellation
# - fix/coordinator-race-condition
# - docs/api-reference
# - test/increase-coverage
```

### 2. Make Your Changes

```bash
# Edit files, write code, add tests
vim src/gcon/cluster/scheduler.py

# Run tests frequently
pytest tests/

# Check code style
black src/ tests/
flake8 src/ tests/
```

### 3. Keep in Sync

```bash
# Fetch updates from upstream
git fetch upstream

# Rebase your branch (if needed)
git rebase upstream/main
```

### 4. Push & Create PR

```bash
# Push to your fork
git push origin feature/short-description

# Go to GitHub and click "Create Pull Request"
```

---

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_coordinator.py

# Run specific test
pytest tests/test_coordinator.py::test_assign_job_idempotent

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=src/gcon
```

### Writing Tests

**Test File Location:** `tests/test_*.py`

**Example Test:**

```python
import pytest
from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent

def test_submit_job_creates_pending_job():
    """Verify that submit_job creates a job with pending status."""
    coordinator = GCONCoordinator()
    
    job_id = "test-job-001"
    command = "echo hello"
    
    job = coordinator.submit_job(job_id, command)
    
    assert job['job_id'] == job_id
    assert job['status'] == 'pending'
    assert job['command'] == command

def test_assign_job_idempotent():
    """Verify that assigning the same job twice is idempotent."""
    coordinator = GCONCoordinator()
    agent = GCONAgent('test-agent', capacity=1)
    
    coordinator.register_agent(agent)
    job = coordinator.submit_job('job-001', 'echo hi')
    
    # First assignment
    receipt1 = coordinator.assign_job(job, agent)
    assert receipt1 is not None
    
    # Second assignment should fail or be ignored
    receipt2 = coordinator.assign_job(job, agent)
    assert receipt2 == receipt1  # Same receipt, not a duplicate
```

### Test Coverage

Aim for **>80% code coverage** on new code.

```bash
# Generate coverage report
pytest --cov=src/gcon --cov-report=html

# View report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

### Integration Tests

For end-to-end tests, see `tests/stage*.py` — these test the full pipeline:

```bash
python tests/stage10_test.py  # Full coordinator + agent lifecycle
```

---

## Code Style

### Python

GCON follows **PEP 8** with Black formatting.

```bash
# Auto-format code
black src/ tests/

# Check style without changes
flake8 src/ tests/
```

### Naming Conventions

- **Files & Directories**: `snake_case` (e.g., `node_registry.py`)
- **Classes**: `PascalCase` (e.g., `GCONCoordinator`)
- **Functions & Variables**: `snake_case` (e.g., `submit_job`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_TIMEOUT`)

### Type Hints

Use type hints for clarity:

```python
from typing import List, Dict, Optional

def submit_job(
    self,
    job_id: str,
    command: str,
    timeout_seconds: Optional[int] = None,
    tags: Optional[List[str]] = None
) -> Dict[str, any]:
    """
    Submit a job for execution.
    
    Args:
        job_id: Unique job identifier
        command: Shell command to execute
        timeout_seconds: Max execution time (default: 300)
        tags: Optional job tags
    
    Returns:
        Job metadata dict
    
    Raises:
        ConflictError: If job_id already exists
    """
    # Implementation
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def assign_job(self, job: Job, agent: GCONAgent) -> Receipt:
    """
    Assign a job to an agent for execution.
    
    This method is idempotent: assigning the same job twice
    will not result in duplicate execution.
    
    Args:
        job: The job to assign
        agent: The target agent
    
    Returns:
        Execution receipt (signed proof)
    
    Raises:
        ValueError: If job is already completed
        RuntimeError: If agent is offline
    """
```

---

## Commit Messages

Write clear, descriptive commit messages.

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation changes
- **test**: Adding or updating tests
- **refactor**: Code refactoring (no feature/fix)
- **perf**: Performance improvements
- **ci**: CI/CD configuration
- **chore**: Maintenance tasks

### Examples

```
feat(scheduler): add capacity-aware scheduling

Implement a new scheduler that respects agent capacity
and load-balances jobs across the cluster.

- Query agent capacity from node registry
- Sort agents by available capacity
- Prefer idle agents over busy ones

Closes #42
```

```
fix(coordinator): prevent job double-dispatch

The assign_job method was not checking job status before
rdispatching, causing the same job to be sent to multiple
agents in a race condition.

Add status check before dispatch to make assign_job idempotent.

Fixes #99
```

```
docs(api): document receipt verification endpoint

Add comprehensive examples for the POST /receipts/{id}/verify
endpoint including error cases and verification failures.
```

---

## Pull Request Process

### Before You Submit

- [ ] Tests pass: `pytest`
- [ ] Code style passes: `black src/ tests/ && flake8 src/ tests/`
- [ ] Coverage maintained/improved: `pytest --cov=src/gcon`
- [ ] Branch is up to date: `git rebase upstream/main`
- [ ] Commit messages are clear
- [ ] Documentation updated (if applicable)

### PR Description Template

```markdown
## Description

Brief description of the changes.

## Problem Statement

What problem does this PR solve?

## Solution

How does this PR solve it?

## Testing

- [ ] Added unit tests
- [ ] Added integration tests
- [ ] All tests pass
- [ ] Coverage: X%

## Related Issues

Closes #123

## Checklist

- [ ] Code follows project style guidelines
- [ ] Documentation updated
- [ ] No breaking changes
- [ ] Backwards compatible
```

### Review Process

1. **Automated Checks**: CI/CD runs tests, linting, coverage
2. **Code Review**: Maintainers review for correctness, design, security
3. **Feedback**: Address review comments
4. **Approval**: Merge when approved and all checks pass

---

## Reporting Bugs

### Before You Report

- [ ] Search existing issues (might be already reported)
- [ ] Check the troubleshooting guide in the docs
- [ ] Collect reproduction steps

### Bug Report Template

```markdown
## Description

Brief description of the bug.

## Environment

- Python version: 3.12
- OS: Ubuntu 22.04
- GCON version: 0.10

## Steps to Reproduce

1. Start coordinator: `python -m gcon.dashboard.dashboard_server`
2. Register agent: `...
3. Submit job: `...
4. Observe: The bug happens

## Expected Behavior

What should happen instead?

## Actual Behavior

What actually happened?

## Logs

```
[paste relevant logs]
```

## Suggested Fix

If you have an idea, describe it here.
```

---

## Suggesting Features

### Feature Request Template

```markdown
## Description

Brief description of the feature.

## Problem It Solves

What pain point does this address?

## Proposed Solution

How would you implement this?

## Alternatives Considered

Are there other ways to solve this?

## Additional Context

Any other info (mockups, links, examples)?
```

---

## Project Governance

### Maintainers

- **Nyongesa Briton** (@briton-data) — Project lead, architecture decisions

### Decision Making

- **Minor changes** (docs, tests, small fixes): Merged by any maintainer
- **Major changes** (architecture, breaking API): Discussed in GitHub issues first
- **Release decisions**: Announced in CHANGELOG

### Roadmap

See [ROADMAP.md](ROADMAP.md) for planned features and milestones.

---

## Getting Help

- 📖 **Documentation**: [docs/](../docs/)
- 🐛 **Bug Reports**: [GitHub Issues](https://github.com/briton-data/gcon/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/briton-data/gcon/discussions)
- 🚀 **Roadmap**: [ROADMAP.md](ROADMAP.md)

---

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (see LICENSE file).

---

**Thank you for contributing to GCON!** 🙏
