from dataclasses import dataclass, field
from datetime import datetime, UTC
from uuid import uuid4


@dataclass
class Event:
    """
    Represents a single event in the GCON cluster.
    """
    event_type: str
    source: str
    payload: dict
    
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))