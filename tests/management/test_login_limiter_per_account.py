"""
Guessing one account's password from many addresses.

The limiter locked an (email, address) pair after 5 failures, so every new address got 5 more tries and a
distributed guess against a single account was never stopped. There is now also a per-account limit that
counts failures from every address together.
"""
import pytest

from gcon.management.rate_limit import LoginRateLimiter
from gcon.storage.database import Database


@pytest.fixture(params=["memory", "database"])
def limiter(request, tmp_path):
    db = Database(str(tmp_path / "rl.db")) if request.param == "database" else None
    return LoginRateLimiter(db=db)


def _fail_from_new_addresses(limiter, email, count, start=0):
    for i in range(start, start + count):
        ip = f"203.0.113.{i % 250}.{i // 250}"      # a different address each time
        limiter.check(email, ip)
        limiter.record_failure(email, ip)


class TestPerAccountLimit:
    def test_many_addresses_each_under_the_per_address_limit_are_still_stopped(self, limiter):
        email = "victim@example.com"
        with pytest.raises(ValueError):
            _fail_from_new_addresses(limiter, email, 200)
        # and from a brand-new address too
        with pytest.raises(ValueError):
            limiter.check(email, "198.51.100.77")

    def test_the_account_limit_is_higher_than_the_per_address_limit(self, limiter):
        email = "victim@example.com"
        _fail_from_new_addresses(limiter, email, limiter.account_max_attempts - 1)   # does not trip
        limiter.check(email, "198.51.100.1")

    def test_other_accounts_are_unaffected(self, limiter):
        with pytest.raises(ValueError):
            _fail_from_new_addresses(limiter, "victim@example.com", 200)
        limiter.check("someone-else@example.com", "198.51.100.2")

    def test_a_successful_login_clears_the_account_count(self, limiter):
        email = "victim@example.com"
        _fail_from_new_addresses(limiter, email, limiter.account_max_attempts - 2)
        limiter.record_success(email, "198.51.100.3")
        _fail_from_new_addresses(limiter, email, limiter.account_max_attempts - 2, start=500)
        limiter.check(email, "198.51.100.4")

    def test_the_admin_unlock_clears_an_account_lockout(self, limiter):
        email = "victim@example.com"
        with pytest.raises(ValueError):
            _fail_from_new_addresses(limiter, email, 200)
        assert limiter.is_locked_for_email(email)
        limiter.clear_lockouts_for_email(email)
        limiter.check(email, "198.51.100.5")
        assert not limiter.is_locked_for_email(email)

    def test_per_address_behaviour_is_unchanged(self, limiter):
        for _ in range(limiter.max_attempts):
            limiter.record_failure("a@example.com", "198.51.100.9")
        with pytest.raises(ValueError):
            limiter.check("a@example.com", "198.51.100.9")
        limiter.check("a@example.com", "198.51.100.10")
