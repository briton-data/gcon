"""
gcon.management.plans -- plan names, the editable per-plan limits, and how an
environment override combines with them.

Storage is exercised against a real ControlPlane settings table (SQLite in
tmp_path), because "stored value is damaged" is a real-bytes question.
"""
import json

import pytest

from gcon.management import plans
from gcon.persistence.control_plane import ControlPlane

pytestmark = pytest.mark.real_plan_defaults


@pytest.fixture
def settings(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "plans.db"))
    yield control_plane.settings
    control_plane.close()


class TestNames:
    @pytest.mark.parametrize("given, expected", [
        ("Standard", "Standard"), ("pro", "Pro"), ("  ENTERPRISE ", "Enterprise"),
    ])
    def test_canonical_plan_is_case_and_space_insensitive(self, given, expected):
        assert plans.canonical_plan(given) == expected

    @pytest.mark.parametrize("bad", ["Free", "Gold", "", "  ", None, 3])
    def test_unknown_plans_are_rejected_with_the_valid_choices(self, bad):
        with pytest.raises(ValueError) as exc:
            plans.canonical_plan(bad)
        assert "Standard, Pro, Enterprise" in str(exc.value)

    def test_plan_for_limits_never_raises_and_defaults_to_standard(self):
        assert plans.plan_for_limits("Pro") == "Pro"
        assert plans.plan_for_limits("Free") == "Standard"
        assert plans.plan_for_limits(None) == "Standard"
        assert plans.plan_for_limits("") == "Standard"


class TestDefaults:
    def test_standard_request_rate_is_what_the_api_always_allowed(self):
        assert plans.DEFAULT_PLAN_LIMITS["Standard"]["requests_per_minute"] == 600

    def test_every_plan_has_every_limit_and_tiers_only_go_up(self):
        for plan in plans.PLAN_NAMES:
            assert set(plans.DEFAULT_PLAN_LIMITS[plan]) == set(plans.LIMIT_FIELDS)
        for field in ("requests_per_minute", "max_concurrent_jobs", "max_runtime_seconds"):
            values = [plans.DEFAULT_PLAN_LIMITS[p][field] for p in plans.PLAN_NAMES]
            assert values == sorted(values) and len(set(values)) == 3
        # a higher plan may run everything a lower one may, and Enterprise anything
        standard, pro, enterprise = (plans.DEFAULT_PLAN_LIMITS[p]["allowed_programs"] for p in plans.PLAN_NAMES)
        assert set(standard) < set(pro)
        assert enterprise == ["*"]

    def test_standard_allows_the_programs_a_gpu_customer_needs(self):
        assert {"python", "python3", "nvidia-smi"} <= set(plans.DEFAULT_PLAN_LIMITS["Standard"]["allowed_programs"])


class TestValidation:
    def test_accepts_whole_numbers_including_zero(self):
        assert plans.validate_limits(0, 10, ["python"], 0) == {
            "requests_per_minute": 0, "max_concurrent_jobs": 10,
            "allowed_programs": ["python"], "max_runtime_seconds": 0, "max_workers": 0, "max_leased_workers": 0,
            "verification": False, "free_allowance_cents": 0}
        top = plans.validate_limits(plans.MAX_LIMIT_VALUE, 1, ["*"], plans.MAX_RUNTIME_SECONDS)
        assert top["requests_per_minute"] == plans.MAX_LIMIT_VALUE
        assert top["max_runtime_seconds"] == plans.MAX_RUNTIME_SECONDS

    @pytest.mark.parametrize("rpm, jobs, runtime", [
        (-1, 5, 60), (5, -1, 60), (5, 5, -1),
        (plans.MAX_LIMIT_VALUE + 1, 5, 60), (5, plans.MAX_LIMIT_VALUE + 1, 60),
        (5, 5, plans.MAX_RUNTIME_SECONDS + 1),
        (1.5, 5, 60), (5, "10", 60), (5, 5, 1.5), (None, 5, 60), (5, None, 60), (5, 5, None),
        (True, 5, 60), (5, False, 60), (5, 5, True),
    ])
    def test_rejects_everything_else(self, rpm, jobs, runtime):
        with pytest.raises(ValueError):
            plans.validate_limits(rpm, jobs, ["python"], runtime)

    def test_programs_are_cleaned_and_deduplicated(self):
        assert plans.validate_programs(["python", " python3 ", "python"]) == ["python", "python3"]

    def test_star_means_any_and_absorbs_the_rest(self):
        assert plans.validate_programs(["bash", "*"]) == ["*"]

    @pytest.mark.parametrize("bad", [
        [], "python", None, 5, ["/usr/bin/python"], ["py thon"], ["python;rm"], [""], [1], ["a" * 65],
        [f"p{i}" for i in range(plans.MAX_PROGRAMS + 1)],
    ])
    def test_bad_program_lists_are_rejected(self, bad):
        with pytest.raises(ValueError):
            plans.validate_programs(bad)


class TestStorage:
    def test_defaults_when_nothing_stored_and_when_there_is_no_settings_table(self, settings):
        assert plans.load_plan_limits(settings) == plans.DEFAULT_PLAN_LIMITS
        assert plans.load_plan_limits(None) == plans.DEFAULT_PLAN_LIMITS

    def test_defaults_are_returned_as_copies(self):
        table = plans.load_plan_limits(None)
        table["Standard"]["requests_per_minute"] = 1
        assert plans.DEFAULT_PLAN_LIMITS["Standard"]["requests_per_minute"] == 600

    def test_save_changes_one_plan_and_keeps_the_others(self, settings):
        saved = plans.save_plan_limits(settings, "pro", 100, 7, ["python"], 90, updated_by="ann")
        assert saved == {"requests_per_minute": 100, "max_concurrent_jobs": 7,
                         "allowed_programs": ["python"], "max_runtime_seconds": 90, "max_workers": 25, "max_leased_workers": 4, "verification": True, "free_allowance_cents": 0}
        table = plans.load_plan_limits(settings)
        assert table["Pro"] == saved
        assert table["Standard"] == plans.DEFAULT_PLAN_LIMITS["Standard"]
        assert table["Enterprise"] == plans.DEFAULT_PLAN_LIMITS["Enterprise"]

    def test_leaving_programs_and_runtime_out_keeps_the_current_ones(self, settings):
        plans.save_plan_limits(settings, "Pro", 100, 7, ["python"], 90)
        saved = plans.save_plan_limits(settings, "Pro", 200, 8)
        assert saved["allowed_programs"] == ["python"] and saved["max_runtime_seconds"] == 90
        assert saved["requests_per_minute"] == 200

    def test_a_value_stored_before_programs_and_runtime_existed_keeps_its_numbers(self, settings):
        settings.set(plans.SETTINGS_KEY, json.dumps({
            "Pro": {"requests_per_minute": 42, "max_concurrent_jobs": 9}}))
        pro = plans.load_plan_limits(settings)["Pro"]
        assert (pro["requests_per_minute"], pro["max_concurrent_jobs"]) == (42, 9)
        assert pro["allowed_programs"] == plans.DEFAULT_PLAN_LIMITS["Pro"]["allowed_programs"]
        assert pro["max_runtime_seconds"] == plans.DEFAULT_PLAN_LIMITS["Pro"]["max_runtime_seconds"]

    def test_save_rejects_bad_input_and_stores_nothing(self, settings):
        with pytest.raises(ValueError):
            plans.save_plan_limits(settings, "Gold", 1, 1)
        with pytest.raises(ValueError):
            plans.save_plan_limits(settings, "Pro", -5, 1)
        assert settings.get(plans.SETTINGS_KEY) is None

    @pytest.mark.parametrize("garbage", ["not json", "[1, 2]", "null", '"text"'])
    def test_unreadable_stored_value_falls_back_to_defaults(self, settings, garbage, caplog):
        settings.set(plans.SETTINGS_KEY, garbage)
        assert plans.load_plan_limits(settings) == plans.DEFAULT_PLAN_LIMITS

    def test_one_bad_entry_only_resets_that_plan(self, settings):
        settings.set(plans.SETTINGS_KEY, json.dumps({
            "Standard": {"requests_per_minute": "lots", "max_concurrent_jobs": 5},
            "Pro": {"requests_per_minute": 42, "max_concurrent_jobs": 9},
            "Enterprise": "oops",
        }))
        table = plans.load_plan_limits(settings)
        assert table["Standard"] == plans.DEFAULT_PLAN_LIMITS["Standard"]
        assert (table["Pro"]["requests_per_minute"], table["Pro"]["max_concurrent_jobs"]) == (42, 9)
        assert table["Enterprise"] == plans.DEFAULT_PLAN_LIMITS["Enterprise"]


class TestEnvironmentOverride:
    def test_request_rate(self):
        assert plans.effective_requests_per_minute(600, None) == 600     # nothing set: the plan
        assert plans.effective_requests_per_minute(600, 25) == 25        # one number for everyone
        assert plans.effective_requests_per_minute(600, 0) == 0          # off for everyone
        assert plans.effective_requests_per_minute(0, None) == 0         # plan says no limit

    def test_concurrent_jobs(self):
        assert plans.effective_max_concurrent_jobs(50, 0) == 50          # env unset: the plan
        assert plans.effective_max_concurrent_jobs(50, 3) == 3           # env wins, even if lower...
        assert plans.effective_max_concurrent_jobs(50, 500) == 500       # ...or higher, like pricing
        assert plans.effective_max_concurrent_jobs(0, 0) == 0
