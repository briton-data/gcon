"""
Customer-facing accounts: signup, login, and self-service API key
management (management/customers.py, auth.py's Customer* classes,
and ManagementLayer's signup_customer/customer_login/
list_customer_api_keys/create_customer_api_key/revoke_customer_api_key).

Real ManagementLayer + real Database (:memory:) throughout, same
convention as the rest of this suite -- no mocking the layer being
tested.
"""
import pytest

from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def mgmt():
    return ManagementLayer(coordinator=None, db_path=":memory:")


class TestSignup:
    def test_signup_creates_org_customer_and_a_working_key(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        assert result["organization"]["name"] == "Platform Z"
        assert result["customer_user"]["email"] == "zara@platformz.com"
        assert result["customer_user"]["org_id"] == result["organization"]["org_id"]
        assert result["api_key"]["secret"].startswith("gcon_")

        # The returned key is immediately real and usable.
        key, owner = mgmt.authenticate_api_key(result["api_key"]["secret"])
        assert owner.organization_id == result["organization"]["org_id"]

    def test_duplicate_email_signup_rejected(self, mgmt):
        mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        with pytest.raises(ValueError):
            mgmt.signup_customer("Different Org", "Someone Else", "zara@platformz.com", "pw")

    def test_signup_and_login_returns_a_working_session(self, mgmt):
        result = mgmt.customer_signup_and_login("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        assert mgmt.get_customer_session_user(result["session_token"]) is not None

    def test_two_signups_get_two_different_orgs(self, mgmt):
        r1 = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "pw1")
        r2 = mgmt.signup_customer("Globex", "Gary", "gary@globex.com", "pw2")
        assert r1["organization"]["org_id"] != r2["organization"]["org_id"]


class TestLogin:
    def test_correct_credentials_succeed(self, mgmt):
        mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        token, customer = mgmt.customer_login("zara@platformz.com", "hunter2")
        assert customer["email"] == "zara@platformz.com"
        assert token

    def test_wrong_password_rejected(self, mgmt):
        mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        with pytest.raises(ValueError):
            mgmt.customer_login("zara@platformz.com", "wrong")

    def test_unknown_email_rejected_with_same_message_as_wrong_password(self, mgmt):
        # Anti-enumeration: a login form can't distinguish "no such
        # account" from "wrong password" from the exception alone.
        mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        try:
            mgmt.customer_login("zara@platformz.com", "wrong")
        except ValueError as e:
            wrong_password_msg = str(e)
        try:
            mgmt.customer_login("nobody@nowhere.com", "whatever")
        except ValueError as e:
            unknown_email_msg = str(e)
        assert wrong_password_msg == unknown_email_msg

    def test_session_token_resolves_to_the_right_customer(self, mgmt):
        mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        token, customer = mgmt.customer_login("zara@platformz.com", "hunter2")
        session_user = mgmt.get_customer_session_user(token)
        assert session_user["customer_user_id"] == customer["customer_user_id"]

    def test_garbage_token_returns_none_not_a_crash(self, mgmt):
        assert mgmt.get_customer_session_user("not-a-real-token") is None
        assert mgmt.get_customer_session_user(None) is None


class TestAuthenticateApiKeyCustomerFallback:
    def test_customer_key_scopes_correctly_via_the_real_production_path(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        key, owner = mgmt.authenticate_api_key(result["api_key"]["secret"])
        assert owner.organization_id == result["organization"]["org_id"]
        assert owner.status == "Active"

    def test_required_scope_enforced_for_a_customer_key_too(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        with pytest.raises(ValueError):
            mgmt.authenticate_api_key(result["api_key"]["secret"], required_scope="Manage organizations")

    def test_disabled_customer_account_key_is_rejected(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        mgmt.customer_registry.update_status(result["customer_user"]["customer_user_id"], "Disabled")
        with pytest.raises(ValueError):
            mgmt.authenticate_api_key(result["api_key"]["secret"])

    def test_staff_and_customer_keys_both_work_independently(self, mgmt):
        # The fallback must not break the existing staff path.
        staff_key = mgmt.create_api_key("staff key", mgmt.user_registry.get_user_by_email("owner@example.com").user_id)
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")

        _, staff_owner = mgmt.authenticate_api_key(staff_key["secret"])
        _, customer_owner = mgmt.authenticate_api_key(result["api_key"]["secret"])
        assert staff_owner.organization_id is None  # bootstrap owner has no org
        assert customer_owner.organization_id == result["organization"]["org_id"]


class TestCustomerApiKeySelfService:
    def test_list_shows_the_signup_key(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        org_id = result["organization"]["org_id"]
        keys = mgmt.list_customer_api_keys(org_id)
        assert len(keys) == 1
        assert keys[0]["key_id"] == result["api_key"]["key_id"]

    def test_create_adds_a_second_working_key(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        org_id = result["organization"]["org_id"]
        cust_id = result["customer_user"]["customer_user_id"]

        new_key = mgmt.create_customer_api_key(org_id, cust_id, "CI key")
        assert len(mgmt.list_customer_api_keys(org_id)) == 2
        _, owner = mgmt.authenticate_api_key(new_key["secret"])
        assert owner.organization_id == org_id

    def test_revoke_actually_invalidates_the_key(self, mgmt):
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        org_id = result["organization"]["org_id"]
        mgmt.revoke_customer_api_key(org_id, result["api_key"]["key_id"])
        with pytest.raises(ValueError):
            mgmt.authenticate_api_key(result["api_key"]["secret"])

    def test_a_teammate_in_the_same_org_can_manage_the_other_users_key(self, mgmt):
        # Keys are an org-level resource, not private to whoever
        # created them -- see list_customer_api_keys's own docstring.
        result = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        org_id = result["organization"]["org_id"]
        teammate = mgmt.customer_registry.add_user("Zach", "zach@platformz.com", org_id, "pw")

        new_key = mgmt.create_customer_api_key(org_id, teammate.customer_user_id, "Zach's key")
        # Zara (not Zach) can still see and revoke it.
        assert any(k["key_id"] == new_key["key_id"] for k in mgmt.list_customer_api_keys(org_id))
        mgmt.revoke_customer_api_key(org_id, new_key["key_id"])
        with pytest.raises(ValueError):
            mgmt.authenticate_api_key(new_key["secret"])

    def test_cross_org_revoke_is_rejected(self, mgmt):
        result1 = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        result2 = mgmt.signup_customer("Globex", "Gary", "gary@globex.com", "pw12345")
        with pytest.raises(ValueError):
            mgmt.revoke_customer_api_key(result2["organization"]["org_id"], result1["api_key"]["key_id"])
        # And the key is still perfectly valid -- the rejected
        # cross-org attempt must not have revoked it as a side effect.
        key, owner = mgmt.authenticate_api_key(result1["api_key"]["secret"])
        assert owner.organization_id == result1["organization"]["org_id"]

    def test_cross_org_list_never_shows_another_orgs_keys(self, mgmt):
        result1 = mgmt.signup_customer("Platform Z", "Zara", "zara@platformz.com", "hunter2")
        result2 = mgmt.signup_customer("Globex", "Gary", "gary@globex.com", "pw12345")
        globex_keys = mgmt.list_customer_api_keys(result2["organization"]["org_id"])
        assert result1["api_key"]["key_id"] not in {k["key_id"] for k in globex_keys}
