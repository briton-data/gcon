import threading

from .event import Event

class EventBus:
    """
    Central event dispatcher for GCON.
    """
    def __init__(self):
        self._events = []
        self._subscribers = []
        # Guards both lists. publish() is called from every coordinator
        # thread (scheduler loop, health-check loop, per-job worker
        # threads) while subscribe()/unsubscribe() can happen
        # concurrently from a request-handling thread -- without a
        # lock, iterating self._subscribers in publish() while another
        # thread appends/removes raises "list changed size during
        # iteration" / can skip or double-notify subscribers.
        self._lock = threading.RLock()

    def subscribe(self, callback):
        """
        Register a subscriber to receive published events.
        """
        with self._lock:
            if callback not in self._subscribers:
                self._subscribers.append(callback)

    def unsubscribe(self, callback):
        """
        Remove a subscriber from the event bus.
        """
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    def publish(self, event):
        """
        Publish an event to the event bus and notify subscribers.
        """
        with self._lock:
            self._events.append(event)
            # Snapshot so a subscribe()/unsubscribe() triggered by a
            # handler (or from another thread) mid-dispatch can't
            # mutate the list we're iterating.
            subscribers = list(self._subscribers)

        for subscriber in subscribers:
            try:
                subscriber(event)
            except Exception as e:
                # A misbehaving subscriber must not be able to break
                # the publisher's control flow (e.g. abort job
                # completion handling in coordinator._run_job).
                print(f"[EVENT_BUS] subscriber {subscriber!r} raised: {e}")

    def subscriber_count(self):
        """
        Return the number of registered subscribers.
        """
        with self._lock:
            return len(self._subscribers)

    def get_events(self):
        """
        Return all published events.
        """
        with self._lock:
            return list(self._events)

    def get_recent_events(self, limit=10):
        """
        Return the most recent events.
        """
        with self._lock:
            return self._events[-limit:]

    def clear(self):
        with self._lock:
            self._events.clear()

    def count(self):
        """
        Return the total number of published events.
        """
        with self._lock:
            return len(self._events)
    