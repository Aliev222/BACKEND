"""
Skin constants and pure helpers extracted from routers/legacy.py (Patch 7.6).
"""

DEFAULT_SKIN_ID = "default.pngSP"

SOCIAL_SUB_TASK_SKINS = {
    "telegram_sub": "telega.pngSP",
    "tiktok_sub": "tiktok.pngSP",
    "instagram_sub": "insta.pngSP",
}


def normalize_owned_skins(raw: list | None) -> list[str]:
    """Normalize owned skins to a list of strings."""
    if not raw:
        return [DEFAULT_SKIN_ID]
    result = []
    for item in raw:
        s = str(item).strip()
        if s and s not in result:
            result.append(s)
    return result or [DEFAULT_SKIN_ID]


def normalize_selected_skin(selected: str | int | None, owned: list[str]) -> str:
    """Normalize selected skin ID, falling back to default if not owned."""
    if selected is None:
        return DEFAULT_SKIN_ID
    s = str(selected).strip()
    if s in owned:
        return s
    return DEFAULT_SKIN_ID
