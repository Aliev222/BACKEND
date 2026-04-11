MAX_REWARD_PER_VIDEO = 5000
MAX_BET = 1000000
MIN_BET = 10
BASE_MAX_ENERGY = 500
ENERGY_REGEN_SECONDS = 0.2  # 5 energy per second (matches frontend: 200ms per energy)
TOURNAMENT_KEY = "tournament:leaderboard"
TOURNAMENT_PRIZE_POOL = 100000

CLICK_BUFFER_KEY = "clicks:buffer"
CLICK_FLUSH_INTERVAL = 5
USER_CACHE_PREFIX = "user:cache:"
USER_CACHE_TTL = 300
USER_CACHE_MISS_MARKER = "__miss__"
USER_CACHE_MISS_TTL = 20

MAX_REAL_CLICKS_PER_SECOND = 30
CLICK_BURST_ALLOWANCE = 25
MAX_CLICK_BATCH_SIZE = 600
INITIAL_CLICK_BATCH_ALLOWANCE = 20
CLICK_TIME_ACCUMULATION_CAP_SECONDS = 60
CLICK_SUSPICIOUS_OVERSHOOT = 60
CLICK_SUSPICION_SOFT_LIMIT = 6

GHOST_BOOST_MULTIPLIER = 5

RATE_LIMITS = {
    "reward_video": (5, 60),
    "activate_mega_boost": (10, 60),
    "update_energy": (10, 60),
    "complete_task": (20, 60),
    "game_action": (30, 60),
}

MAX_UPGRADE_LEVEL = 100


def _build_multitap_prices():
    prices = []
    for level in range(MAX_UPGRADE_LEVEL):
        step = level + 1
        price = 120 + (step**2.15) * 34
        if level >= 15:
            price *= 1.18
        if level >= 30:
            price *= 1.32
        if level >= 45:
            price *= 1.45
        if level >= 60:
            price *= 1.65
        if level >= 80:
            price *= 1.95

        prices.append(max(50, int(round(price / 50.0) * 50)))
    return prices


UPGRADE_PRICES = {
    "multitap": _build_multitap_prices(),
    "profit": [
        40,
        60,
        90,
        130,
        180,
        240,
        310,
        390,
        480,
        580,
        690,
        810,
        940,
        1080,
        1230,
        1390,
        1560,
        1740,
        1930,
        2130,
        2340,
        2560,
        2790,
        3030,
        3280,
        3540,
        3810,
        4090,
        4380,
        4680,
        4990,
        5310,
        5640,
        5980,
        6330,
        6690,
        7060,
        7440,
        7830,
        8230,
        8640,
        9060,
        9490,
        9930,
        10380,
        10840,
        11310,
        11790,
        12280,
        12780,
        13300,
        13850,
        14420,
        15010,
        15620,
        16250,
        16900,
        17570,
        18260,
        18970,
        19700,
        20450,
        21220,
        22010,
        22820,
        23650,
        24500,
        25370,
        26260,
        27170,
        28100,
        29050,
        30020,
        31010,
        32020,
        33050,
        34100,
        35170,
        36260,
        37370,
        38500,
        39650,
        40820,
        42010,
        43220,
        44450,
        45700,
        46970,
        48260,
        49570,
        50900,
        52250,
        53620,
        55010,
        56420,
        57850,
        59300,
        60770,
        62260,
        63770,
    ],
    "energy": [
        30,
        45,
        65,
        90,
        120,
        155,
        195,
        240,
        290,
        345,
        405,
        470,
        540,
        615,
        695,
        780,
        870,
        965,
        1065,
        1170,
        1280,
        1395,
        1515,
        1640,
        1770,
        1905,
        2045,
        2190,
        2340,
        2495,
        2655,
        2820,
        2990,
        3165,
        3345,
        3530,
        3720,
        3915,
        4115,
        4320,
        4530,
        4745,
        4965,
        5190,
        5420,
        5655,
        5895,
        6140,
        6390,
        6645,
        6905,
        7170,
        7440,
        7715,
        7995,
        8280,
        8570,
        8865,
        9165,
        9470,
        9780,
        10095,
        10415,
        10740,
        11070,
        11405,
        11745,
        12090,
        12440,
        12795,
        13155,
        13520,
        13890,
        14265,
        14645,
        15030,
        15420,
        15815,
        16215,
        16620,
        17030,
        17445,
        17865,
        18290,
        18720,
        19155,
        19595,
        20040,
        20490,
        20945,
        21405,
        21870,
        22340,
        22815,
        23295,
        23780,
        24270,
        24765,
        25265,
        25770,
    ],
}


def _build_global_upgrade_prices():
    # Harder "tournament" progression:
    # lvl 1  ~= 300
    # lvl 10 ~= 12,000
    # lvl 20 ~= 70,000
    # lvl 50 ~= 1,400,000
    # lvl 100 ~= 22,000,000
    #
    # We interpolate smoothly between anchor points in log space so the curve
    # stays readable and does not spike awkwardly in the middle levels.
    import math

    anchors = [
        (0, 300),
        (9, 12000),
        (19, 70000),
        (49, 1400000),
        (99, 22000000),
    ]

    prices = [0] * MAX_UPGRADE_LEVEL
    for index in range(len(anchors) - 1):
        start_level, start_price = anchors[index]
        end_level, end_price = anchors[index + 1]
        span = end_level - start_level
        for level in range(start_level, end_level + 1):
            progress = 0 if span == 0 else (level - start_level) / span
            log_price = (
                math.log(start_price)
                + (math.log(end_price) - math.log(start_price)) * progress
            )
            rounded_price = max(50, int(round(math.exp(log_price) / 50.0) * 50))
            prices[level] = rounded_price

    # Guarantee monotonic growth even after rounding.
    for level in range(1, len(prices)):
        if prices[level] <= prices[level - 1]:
            prices[level] = prices[level - 1] + 50

    return prices


GLOBAL_UPGRADE_PRICES = _build_global_upgrade_prices()

HOUR_VALUES = [
    10,
    15,
    22,
    32,
    45,
    62,
    83,
    108,
    138,
    173,
    215,
    265,
    324,
    393,
    473,
    565,
    670,
    789,
    923,
    1073,
    1240,
    1425,
    1629,
    1853,
    2098,
    2365,
    2655,
    2969,
    3308,
    3673,
    4065,
    4485,
    4934,
    5413,
    5923,
    6465,
    7040,
    7649,
    8293,
    8973,
    9690,
    10445,
    11239,
    12073,
    12948,
    13865,
    14825,
    15829,
    16878,
    17973,
    19115,
    20305,
    21544,
    22833,
    24173,
    25565,
    27010,
    28509,
    30063,
    31673,
    33340,
    35065,
    36849,
    38693,
    40598,
    42565,
    44595,
    46689,
    48848,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
    51073,
]
