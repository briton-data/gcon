from event import Event

class EventBus:
    """
    Central event dispatcher for GCON.
    """
    def __init__(self):
        self._events = []
        self._subscribers=[]
    
    def subscribe(self, callback):
        """
        Register a subscriber to receive published events.
        """
        if callback not in self._subscribers:
            self._subscribers.append(callback)
            
    def unsubscribe(self, callback):
        """
        Remove a subscriber from the event bus.
        """
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def publish(self, event):
        """
        Publish an event to the event bus and notify subscibers.
        """
        self._events.append(event)
        for subscriber in self._subscribers:
            subscriber(event)
            
    def subscriber_count(self):
        """
        Return the number of registered subscribers.
        """
        return len(self._subscribers)

    def get_events(self):       
        """
        Return all published events.
        """
        return list(self._events)

    def get_recent_events(self, limit=10):
        """
        Return the most recent events.
        """
        return self._events[-limit:]

    def clear(self):
        self._events.clear()

    def count(self):
        """
        Return the total number of published events.
        """
        return len(self._events)
    