class EventListener:
    """
    Base class for objects that listen for cluster events.
    """

    def __init__(self):
        self.events = []

    def handle_event(self, event):
        """
        Receive an event from the EventBus.
        """
        self.events.append(event)

    def get_events(self):
        """
        Return all received events.
        """
        return list(self.events)

    def clear(self):
        """
        Remove all stored events.
        """
        self.events.clear()