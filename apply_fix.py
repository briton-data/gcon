"""
GCON fix-in-place patcher.

Run this FROM INSIDE your gcon project folder (the one that has
api_keys.py and web_server.py directly in it):

    python apply_fix.py

It reads the two files that are ALREADY on your disk, fixes the two
known bugs in them if it finds the broken pattern, and prints exactly
what it changed (or tells you plainly if a file is missing / already
fixed / has an unexpected shape it can't safely patch).

This does not rely on you manually replacing files — it edits
whatever is actually at ./api_keys.py and ./web_server.py right now.
"""

import os
import re
import sys

CHANGED = []
SKIPPED = []
ERRORS = []


def patch_file(path, patches):
    """
    patches: list of (description, old, new) tuples. Applies each
    if `old` is found; reports if not found (already patched, or
    file has a different shape than expected).
    """
    if not os.path.exists(path):
        ERRORS.append(f"{path}: FILE NOT FOUND in current directory ({os.getcwd()})")
        return

    with open(path, "r", encoding="utf-8") as f:
        original = f.read()
    text = original

    for desc, old, new in patches:
        if old in text:
            text = text.replace(old, new, 1)
            CHANGED.append(f"{path}: {desc}")
        elif new in text:
            SKIPPED.append(f"{path}: {desc} (already correct)")
        else:
            ERRORS.append(
                f"{path}: {desc} — could not find the expected old OR new pattern. "
                f"This file may have a different shape than expected; needs manual review."
            )

    if text != original:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)


def main():
    print(f"Running in: {os.getcwd()}\n")

    # -----------------------------------------------------------
    # api_keys.py — move find_by_secret/is_valid into the class
    # -----------------------------------------------------------
    api_keys_path = "api_keys.py"
    if os.path.exists(api_keys_path):
        with open(api_keys_path, "r", encoding="utf-8") as f:
            text = f.read()

        # Detect the broken (unindented, module-level) versions
        broken_find_by_secret = re.search(
            r"^def find_by_secret\(self, secret\):", text, re.MULTILINE
        )
        broken_is_valid = re.search(
            r"^def is_valid\(self, key\):", text, re.MULTILINE
        )

        if broken_find_by_secret or broken_is_valid:
            # Remove the broken module-level defs entirely (they're
            # never valid at module scope — 'self' as a plain arg).
            text = re.sub(
                r"^def find_by_secret\(self, secret\):\n"
                r"(?:.*\n)*?"
                r"    return None\n\n?",
                "",
                text,
                count=1,
                flags=re.MULTILINE,
            )
            text = re.sub(
                r"^def is_valid\(self, key\):\n"
                r"(?:.*\n)*?"
                r"    return True\n\n?",
                "",
                text,
                count=1,
                flags=re.MULTILINE,
            )

            # Insert them as real class methods, right after list_keys
            if "    def find_by_secret(self, secret):" not in text:
                anchor = "    def list_keys(self):\n        return list(self.keys.values())\n"
                if anchor in text:
                    replacement = anchor + '''
    def find_by_secret(self, secret):
        """
        Look up an active key by its raw secret, for use by the
        public API's authentication layer. Uses a constant-time
        comparison to avoid leaking timing information.
        """
        if not secret:
            return None
        for key in self.keys.values():
            if hmac.compare_digest(key.secret, secret):
                return key
        return None

    def is_valid(self, key):
        """
        Return True if a key is active and not expired.
        """
        if key.status != "Active":
            return False
        if key.expires_at and datetime.now(UTC) > key.expires_at:
            key.status = "Expired"
            return False
        return True
'''
                    text = text.replace(anchor, replacement, 1)
                    CHANGED.append(
                        f"{api_keys_path}: moved find_by_secret/is_valid into APIKeyManager as real methods"
                    )
                else:
                    ERRORS.append(
                        f"{api_keys_path}: found the broken module-level functions but couldn't find "
                        f"'def list_keys' to anchor the fix after. Needs manual review."
                    )

            with open(api_keys_path, "w", encoding="utf-8") as f:
                f.write(text)
        else:
            if "    def find_by_secret(self, secret):" in text and "    def is_valid(self, key):" in text:
                SKIPPED.append(f"{api_keys_path}: find_by_secret/is_valid already correctly indented as methods")
            else:
                ERRORS.append(
                    f"{api_keys_path}: could not find find_by_secret/is_valid at all (broken or fixed form). "
                    f"Needs manual review."
                )
    else:
        ERRORS.append(f"api_keys.py: FILE NOT FOUND in current directory ({os.getcwd()})")

    # -----------------------------------------------------------
    # web_server.py — targeted route/permission fixes
    # -----------------------------------------------------------
    patch_file("web_server.py", [
        (
            "GET /management/users (was PUT)",
            '@self.app.put("/management/users")\n        def mgmt_users(',
            '@self.app.get("/management/users")\n        def mgmt_users(',
        ),
        (
            '"Manage api_keys" -> "Manage API keys" (revoke)',
            'def mgmt_revoke_api_key(key_id: str, user=Depends(self.      require_permission("Manage api_keys"))):',
            'def mgmt_revoke_api_key(key_id: str, user=Depends(self.require_permission("Manage API keys"))):',
        ),
        (
            '"Manage api_keys" -> "Manage API keys" (revoke, alt spacing)',
            'def mgmt_revoke_api_key(key_id: str, user=Depends(self.require_permission("Manage api_keys"))):',
            'def mgmt_revoke_api_key(key_id: str, user=Depends(self.require_permission("Manage API keys"))):',
        ),
        (
            '"Manage api_keys" -> "Manage API keys" (regenerate)',
            'def mgmt_regenerate_api_key(key_id: str, user=Depends(self.require_permission("Manage api_keys"))):',
            'def mgmt_regenerate_api_key(key_id: str, user=Depends(self.require_permission("Manage API keys"))):',
        ),
    ])

    # rediscover-nodes: add if missing
    if os.path.exists("web_server.py"):
        with open("web_server.py", "r", encoding="utf-8") as f:
            text = f.read()
        if '"/admin/rediscover-nodes"' not in text:
            anchor = re.search(
                r'(        @self\.app\.post\("/admin/scale-down"\)\n(?:.*\n)*?            return self\.presentation\.scale_down\(\)\n)',
                text,
            )
            if anchor:
                insertion = anchor.group(1) + (
                    '\n        @self.app.post("/admin/rediscover-nodes")\n'
                    '        def admin_rediscover_nodes(user=Depends(self.require_permission("Manage cluster"))):\n'
                    '            return self.presentation.rediscover_nodes()\n'
                )
                text = text.replace(anchor.group(1), insertion, 1)
                with open("web_server.py", "w", encoding="utf-8") as f:
                    f.write(text)
                CHANGED.append("web_server.py: re-added missing POST /admin/rediscover-nodes route")
            else:
                ERRORS.append(
                    "web_server.py: /admin/rediscover-nodes is missing and couldn't find an anchor "
                    "(/admin/scale-down) to insert after. Needs manual review."
                )
        else:
            SKIPPED.append("web_server.py: /admin/rediscover-nodes already present")

    # ---------------------------------------------------------------
    print("=" * 70)
    print(f"CHANGED ({len(CHANGED)}):")
    for c in CHANGED:
        print(f"  + {c}")
    print(f"\nALREADY OK ({len(SKIPPED)}):")
    for s in SKIPPED:
        print(f"  = {s}")
    print(f"\nNEEDS MANUAL REVIEW ({len(ERRORS)}):")
    for e in ERRORS:
        print(f"  ! {e}")
    print("=" * 70)

    if ERRORS:
        print("\nSome things need manual review — see above. Paste this whole")
        print("output back and I'll tell you exactly what to do next.")
        sys.exit(1)
    else:
        print("\nAll patches applied (or already present). Now run:")
        print("  find . -name __pycache__ -type d -exec rm -rf {} +")
        print("  python smoke_test.py")
        sys.exit(0)


if __name__ == "__main__":
    main()
