STARS_SKIN_PRICES = {
    "stars1.pngSP": 149,
    "stars2.pngSP": 149,
    "stars3.pngSP": 149,
    "stars4.pngSP": 249,
    "stars5.pngSP": 249,
    "stars6.pngSP": 249,
    "stars7.pngSP": 500,
    "stars8.pngSP": 500,
}


def get_stars_skin_price(skin_id: str) -> int | None:
    return STARS_SKIN_PRICES.get(skin_id)


def is_stars_skin(skin_id: str) -> bool:
    return skin_id in STARS_SKIN_PRICES
