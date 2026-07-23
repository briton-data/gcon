class TransportError(Exception):
    """Base class for all transport-layer failures."""


class NodeUnavailableError(TransportError):
    """Raised when a node is not registered, not connected, or has
    dropped its channel (e.g. missed heartbeats past the deadline)."""


class NodeUnauthenticatedError(TransportError):
    """Raised when a node fails mutual authentication (invalid or
    unrecognized client certificate)."""


class JobDispatchTimeoutError(TransportError):
    """Raised when a job is dispatched to a node but no result is
    received within the job's timeout."""


class DuplicateRegistrationError(TransportError):
    """Raised when a node attempts to register with an ID already
    bound to a different, currently-live connection."""
