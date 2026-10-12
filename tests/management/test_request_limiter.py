"""
RequestRateLimiter -- the per-organization request limit behind /api/v1.

Pure unit tests against a fake clock: no sleeping, no HTTP. The route-level
behaviour (429 + Retry-After, per-org counting, auth failures not counted)
is in tests/api/test_api_rate_limit.py.
"""
import threading

import pytest

from gcon.management.request_limiter import (
    ENV_VAR,
    RequestRateLimiter,
    env_override,
)


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make(limit=3, window=60.0):
    clock = FakeClock()
    return RequestRateLimiter(limit, window_seconds=window, clock=clock), clock


class TestWindow:
    def test_allows_up_to_the_limit_then_refuses(self):
        limiter, _ = make(limit=3)
        assert [limiter.hit("org:a")[0] for _ in range(3)] == [True, True, True]
        allowed, retry_after = limiter.hit("org:a")
        assert allowed is False
        assert retry_after == pytest.approx(60.0)

    def test_retry_after_counts_down_to_when_the_oldest_request_ages_out(self):
        limiter, clock = make(limit=2)
        limiter.hit("org:a")          # t = 1000
        clock.advance(20)
        limiter.hit("org:a")          # t = 1020
        clock.advance(10)             # now = 1030, window holds both
        allowed, retry_after = limiter.hit("org:a")
        assert allowed is False
        assert retry_after == pytest.approx(30.0)   # oldest (1000) leaves at 1060

    def test_allowed_again_exactly_when_the_oldest_request_leaves_the_window(self):
        limiter, clock = make(limit=2)
        limiter.hit("org:a")
        clock.advance(20)
        limiter.hit("org:a")
        clock.advance(39.9)           # 1059.9: oldest (1000) still inside
        assert limiter.hit("org:a")[0] is False
        clock.advance(0.1)            # 1060: oldest has aged out
        assert limiter.hit("org:a")[0] is True

    def test_refused_requests_are_not_counted(self):
        # Hammering while blocked must not push the unblock time further out.
        limiter, clock = make(limit=1)
        limiter.hit("org:a")                       # allowed at t=1000
        for _ in range(50):
            assert limiter.hit("org:a")[0] is False
            clock.advance(1)
        # 50s later; had refusals been recorded the window would still be full
        clock.advance(10)                          # t = 1060
        assert limiter.hit("org:a")[0] is True

    def test_full_window_resets_after_the_window_passes(self):
        limiter, clock = make(limit=3)
        for _ in range(3):
            limiter.hit("org:a")
        clock.advance(61)
        assert all(limiter.hit("org:a")[0] for _ in range(3))
        assert limiter.hit("org:a")[0] is False


class TestIdentities:
    def test_identities_are_counted_independently(self):
        limiter, _ = make(limit=1)
        assert limiter.hit("org:a")[0] is True
        assert limiter.hit("org:a")[0] is False
        assert limiter.hit("org:b")[0] is True
        assert limiter.hit("key:k1")[0] is True

    def test_idle_identities_are_dropped(self):
        limiter, clock = make(limit=5)
        for i in range(2000):
            limiter.hit(f"org:{i}")
        assert limiter.tracked_identities() == 2000
        clock.advance(61)
        for i in range(1100):          # crosses the sweep interval
            limiter.hit("org:busy")
        # everything that went quiet is gone, only the busy one remains
        assert limiter.tracked_identities() == 1


class TestConstruction:
    @pytest.mark.parametrize("limit", [0, -1])
    def test_rejects_a_non_positive_limit(self, limit):
        with pytest.raises(ValueError):
            RequestRateLimiter(limit)

    def test_rejects_a_non_positive_window(self):
        with pytest.raises(ValueError):
            RequestRateLimiter(5, window_seconds=0)


class TestThreadSafety:
    def test_concurrent_hits_never_exceed_the_limit(self):
        limiter = RequestRateLimiter(100)          # real clock; window is long
        allowed = []
        lock = threading.Lock()

        def worker():
            for _ in range(50):
                ok, _ = limiter.hit("org:a")
                with lock:
                    allowed.append(ok)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(allowed) == 400
        assert sum(allowed) == 100


class TestPerCallLimit:
    def test_each_call_can_use_its_own_limit(self):
        limiter, _ = make(limit=1)
        assert [limiter.hit("org:big", 3)[0] for _ in range(3)] == [True, True, True]
        assert limiter.hit("org:big", 3)[0] is False
        assert limiter.hit("org:small", 1)[0] is True
        assert limiter.hit("org:small", 1)[0] is False

    def test_raising_the_limit_takes_effect_on_the_next_call(self):
        limiter, _ = make(limit=1)
        assert limiter.hit("org:a", 1)[0] is True
        assert limiter.hit("org:a", 1)[0] is False
        assert limiter.hit("org:a", 2)[0] is True       # plan upgraded

    def test_lowering_the_limit_waits_for_enough_old_requests_to_age_out(self):
        limiter, clock = make(limit=5)
        for _ in range(4):                               # t = 1000, 1010, 1020, 1030
            limiter.hit("org:a", 5)
            clock.advance(10)
        # now = 1040. Downgraded to 2: a request only fits once at most ONE
        # earlier request is left in the window, so the three oldest must age
        # out; the third (t=1020) leaves at 1080, i.e. 40 seconds from now.
        allowed, retry_after = limiter.hit("org:a", 2)
        assert allowed is False
        assert retry_after == pytest.approx(40.0)
        clock.advance(39.9)
        assert limiter.hit("org:a", 2)[0] is False
        clock.advance(0.2)
        assert limiter.hit("org:a", 2)[0] is True

    @pytest.mark.parametrize("bad", [0, -1])
    def test_a_non_positive_per_call_limit_is_a_programming_error(self, bad):
        limiter, _ = make(limit=3)
        with pytest.raises(ValueError):
            limiter.hit("org:a", bad)


class TestEnvOverride:
    def test_none_when_unset(self, monkeypatch):
        monkeypatch.delenv(ENV_VAR, raising=False)
        assert env_override() is None

    def test_none_when_blank(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "   ")
        assert env_override() is None

    def test_explicit_value(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "25")
        assert env_override() == 25

    def test_zero_means_off_for_everyone(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "0")
        assert env_override() == 0

    @pytest.mark.parametrize("bad", ["abc", "-5", "1.5", "ten"])
    def test_a_bad_value_is_ignored_so_plans_still_apply_not_switched_off(self, monkeypatch, bad, caplog):
        monkeypatch.setenv(ENV_VAR, bad)
        assert env_override() is None
        assert ENV_VAR in caplog.text
