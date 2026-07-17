from datetime import datetime, UTC

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.dashboard import Dashboard
from gcon.events.event import Event

print("=" * 60)
print("STAGE 19 - EVENT HISTORY TEST")
print("=" * 60)

# Create coordinator
coordinator = GCONCoordinator()

# Create dashboard
dashboard = Dashboard(coordinator)

# Subscribe dashboard to EventBus
coordinator.event_bus.subscribe(dashboard.handle_event)

print("PASS: Dashboard subscribed.")

# Publish 150 test events
for i in range(150):
    coordinator.event_bus.publish(
        Event(
            timestamp=datetime.now(UTC),
            event_type="TEST_EVENT",
            source="Stage19",
            payload={
                "event_number": i
            }
        )
    )

print("PASS: Published 150 events.")

# Verify only the latest 100 are kept
assert len(dashboard.events) == 100

print("PASS: Dashboard history limited to 100 events.")

# Verify the oldest remaining event is #50
assert dashboard.events[0].payload["event_number"] == 50

print("PASS: Oldest retained event is #50.")

# Verify the newest event is #149
assert dashboard.events[-1].payload["event_number"] == 149

print("PASS: Latest event is #149.")

print("=" * 60)
print("STAGE 19 EVENT HISTORY TEST PASSED")
print("=" * 60)