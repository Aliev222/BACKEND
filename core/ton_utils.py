"""
TON wallet pure helpers extracted from routers/legacy.py (Patch 7.6).
No side effects, no DB/Redis calls.
"""

import re
from typing import Any

TON_WALLET_ALLOWED_CHARS = set(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-:"
)


def is_valid_ton_wallet_address(address: str | None) -> bool:
    """Check if a TON wallet address looks valid."""
    if not address:
        return False
    if not re.match(r"^[A-Za-z0-9_-]{48}$", address):
        return False
    return True


def mask_ton_wallet(address: str | None) -> str | None:
    """Mask a TON wallet address for display."""
    if not address:
        return None
    if len(address) <= 10:
        return address[:4] + "..." + address[-4:]
    return address[:6] + "..." + address[-6:]


def ton_addresses_match(a: str | None, b: str | None) -> bool:
    """Compare two TON addresses case-insensitively."""
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()


def ton_wallet_normalized_variants(address: str | None) -> set[str]:
    """Return normalized variants of a TON address for matching."""
    if not address:
        return set()
    addr = address.strip()
    variants = {addr.lower()}
    # UQ/EQ prefix variants
    if addr.startswith("UQ") or addr.startswith("EQ"):
        variants.add("UQ" + addr[2:])
        variants.add("EQ" + addr[2:])
    return variants


def ton_wallets_equal(a: str | None, b: str | None) -> bool:
    """Check if two TON addresses are equal (normalized)."""
    if not a or not b:
        return False
    return ton_addresses_match(a, b)


def get_ton_wallet_from_user(user: dict | None) -> dict:
    """Extract TON wallet info from a user dict."""
    if not user:
        return {
            "connected": False,
            "verified": False,
            "address": None,
            "masked_address": None,
            "provider": None,
        }
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            import json

            extra = json.loads(extra)
        except Exception:
            extra = {}
    wallet = (
        extra.get("ton_wallet") if isinstance(extra.get("ton_wallet"), dict) else {}
    )
    address = wallet.get("address")
    return {
        "connected": bool(wallet.get("address")),
        "verified": bool(wallet.get("verified")),
        "address": address,
        "masked_address": mask_ton_wallet(address),
        "provider": wallet.get("provider"),
    }


def ton_proof_allowed_domains(request: Any | None) -> list[str]:
    """Return allowed domains for TON proof verification."""
    return ["spirix.vercel.app", "web.telegram.org"]
