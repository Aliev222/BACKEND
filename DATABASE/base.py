from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, BigInteger, select, DateTime, Index, UniqueConstraint, Boolean, desc, func
import json
from datetime import datetime, timedelta
import logging
from CONFIG.settings import DATABASE_URL
from core.game_config import BASE_MAX_ENERGY
from core.game_logic import get_hour_value, get_max_energy, get_tap_value

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

REFERRAL_SIGNUP_BONUS = 25000
REFERRAL_SPECIAL_SKIN_ID = "refferal.pngSP"
WEEKLY_LEAGUE_ORDER = ("diamond", "gold", "silver", "bronze")
WEEKLY_LEAGUE_FUND_SPLITS = {
    "diamond": 0.50,
    "gold": 0.30,
    "silver": 0.15,
    "bronze": 0.05,
}
WEEKLY_TOP_PAYOUT_SPLITS = {
    1: 0.30,
    2: 0.20,
    3: 0.15,
}


# Модель пользователя
class User(Base):
    __tablename__ = 'users'
    __table_args__ = (
        Index("ix_users_referrer_id", "referrer_id"),
        Index("ix_users_created_at", "created_at"),
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, index=True)
    username = Column(String, nullable=True)
    coins = Column(BigInteger, default=0)

    profit_per_hour = Column(BigInteger, default=100)
    profit_per_tap = Column(Integer, default=1)
    energy = Column(Integer, default=BASE_MAX_ENERGY)
    max_energy = Column(Integer, default=BASE_MAX_ENERGY)
    level = Column(Integer, default=0)

    multitap_level = Column(Integer, default=0)
    profit_level = Column(Integer, default=0)
    energy_level = Column(Integer, default=0)
    boost_level = Column(Integer, default=0)

    last_passive_income = Column(DateTime, default=datetime.utcnow)
    last_energy_update = Column(DateTime, default=datetime.utcnow)

    referrer_id = Column(BigInteger, nullable=True)
    referral_count = Column(Integer, default=0)
    referral_earnings = Column(BigInteger, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow)

    extra_data = Column(String, default="{}")
    
    luck_level = Column(Integer, default=0)


# ==================== МОДЕЛЬ ЗАДАНИЙ ====================
class UserTask(Base):
    __tablename__ = 'user_tasks'
    __table_args__ = (
        UniqueConstraint("user_id", "task_id", name="uq_user_tasks_user_id_task_id"),
        Index("ix_user_tasks_user_id_completed_at", "user_id", "completed_at"),
        {'extend_existing': True}
    )
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, index=True)
    task_id = Column(String)
    completed_at = Column(DateTime, default=datetime.utcnow)


class WeeklyTournamentSeason(Base):
    __tablename__ = 'weekly_tournament_seasons'
    __table_args__ = (
        UniqueConstraint("season_key", name="uq_weekly_tournament_seasons_season_key"),
        Index("ix_weekly_tournament_seasons_starts_at", "starts_at"),
        Index("ix_weekly_tournament_seasons_status", "status"),
    )

    id = Column(Integer, primary_key=True)
    season_key = Column(String, nullable=False)
    starts_at = Column(DateTime, nullable=False)
    ends_at = Column(DateTime, nullable=False)
    status = Column(String, default="active", nullable=False)
    payout_fund_cents = Column(BigInteger, default=0)
    gross_ad_revenue_cents = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    settled_at = Column(DateTime, nullable=True)


class WeeklyTournamentEntry(Base):
    __tablename__ = 'weekly_tournament_entries'
    __table_args__ = (
        UniqueConstraint("season_key", "user_id", name="uq_weekly_tournament_entries_season_user"),
        Index("ix_weekly_tournament_entries_season_league_score", "season_key", "league", "score"),
        Index("ix_weekly_tournament_entries_user_id", "user_id"),
    )

    id = Column(Integer, primary_key=True)
    season_key = Column(String, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    username = Column(String, nullable=True)
    display_level = Column(Integer, default=1, nullable=False)
    league = Column(String, default="bronze", nullable=False)
    score = Column(BigInteger, default=0, nullable=False)
    eligible_for_payout = Column(Boolean, default=True, nullable=False)
    fraud_flag = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)


class WeeklyTournamentWinner(Base):
    __tablename__ = 'weekly_tournament_winners'
    __table_args__ = (
        Index("ix_weekly_tournament_winners_season_key", "season_key"),
        Index("ix_weekly_tournament_winners_user_id", "user_id"),
        UniqueConstraint("season_key", "league", "rank", name="uq_weekly_tournament_winners_season_league_rank"),
    )

    id = Column(Integer, primary_key=True)
    season_key = Column(String, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    username = Column(String, nullable=True)
    league = Column(String, nullable=False)
    rank = Column(Integer, nullable=False)
    display_level = Column(Integer, default=1, nullable=False)
    score = Column(BigInteger, default=0, nullable=False)
    stars_reward = Column(BigInteger, default=0, nullable=False)
    payout_cents = Column(BigInteger, default=0, nullable=False)
    eligible_for_payout = Column(Boolean, default=True, nullable=False)
    fraud_flag = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# ==================== ФУНКЦИИ ====================

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def get_weekly_tournament_season_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    now = now or datetime.utcnow()
    start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start, end


def get_weekly_tournament_season_key(now: datetime | None = None) -> str:
    start, _ = get_weekly_tournament_season_window(now)
    return start.strftime("%Y-%m-%d")


def get_weekly_tournament_league(display_level: int) -> str:
    level = max(1, int(display_level or 1))
    if level >= 100:
        return "diamond"
    if level >= 66:
        return "gold"
    if level >= 33:
        return "silver"
    return "bronze"


async def ensure_weekly_tournament_season(session: AsyncSession, season_key: str, starts_at: datetime, ends_at: datetime):
    result = await session.execute(
        select(WeeklyTournamentSeason).where(WeeklyTournamentSeason.season_key == season_key)
    )
    season = result.scalar_one_or_none()
    if season:
        return season

    season = WeeklyTournamentSeason(
        season_key=season_key,
        starts_at=starts_at,
        ends_at=ends_at,
        status="active",
    )
    session.add(season)
    await session.flush()
    return season


def _serialize_user(user: User) -> dict:
    extra_data = {}
    if user.extra_data:
        try:
            extra_data = json.loads(user.extra_data)
        except json.JSONDecodeError:
            logging.error("Failed to decode extra_data for user_id=%s", user.user_id)

    return {
        "user_id": user.user_id,
        "username": user.username,
        "coins": user.coins,
        "profit_per_hour": user.profit_per_hour,
        "profit_per_tap": user.profit_per_tap,
        "energy": user.energy,
        "max_energy": user.max_energy,
        "level": user.level,
        "multitap_level": user.multitap_level,
        "profit_level": user.profit_level,
        "energy_level": user.energy_level,
        "boost_level": user.boost_level,
        "last_passive_income": user.last_passive_income,
        "last_energy_update": user.last_energy_update,
        "luck_level": user.luck_level,
        "referrer_id": user.referrer_id,
        "referral_count": user.referral_count,
        "referral_earnings": user.referral_earnings,
        "created_at": user.created_at,
        "extra_data": extra_data,
    }


async def get_user(user_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()

        if user:
            return _serialize_user(user)
        return None


# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================

async def add_referral_bonus(referrer_id: int, referral_id: int):
    """Начисление бонуса рефереру за нового реферала"""
    try:
        async with AsyncSessionLocal() as session:
            # Получаем реферера
            result = await session.execute(
                select(User).where(User.user_id == referrer_id)
            )
            referrer = result.scalar_one_or_none()
            
            if not referrer or referrer_id == referral_id:
                logging.error(f"❌ Реферер {referrer_id} не найден при попытке начисления бонуса за реферала {referral_id}")
                return False

            referral_result = await session.execute(
                select(User).where(User.user_id == referral_id)
            )
            referral = referral_result.scalar_one_or_none()
            if referral and referral.referrer_id == referrer_id and referrer.referrer_id == referral_id:
                logging.error(f"вќЊ РћС‚РєР»РѕРЅРµРЅ РІР·Р°РёРјРЅС‹Р№ СЂРµС„РµСЂР°Р»СЊРЅС‹Р№ С†РёРєР»: {referrer_id} <-> {referral_id}")
                return False

            extra_data = {}
            if referrer.extra_data:
                try:
                    extra_data = json.loads(referrer.extra_data)
                except json.JSONDecodeError:
                    extra_data = {}

            owned_skins = extra_data.get("owned_skins", ["default.pngSP"])
            if not isinstance(owned_skins, list):
                owned_skins = ["default.pngSP"]
            owned_skins = [
                "refferal.pngSP" if skin_id == "referral-special.pngSP" else skin_id
                for skin_id in owned_skins
            ]
            if REFERRAL_SPECIAL_SKIN_ID not in owned_skins:
                owned_skins.append(REFERRAL_SPECIAL_SKIN_ID)
            extra_data["owned_skins"] = owned_skins

            referrer.coins += REFERRAL_SIGNUP_BONUS
            referrer.referral_count += 1
            referrer.referral_earnings += REFERRAL_SIGNUP_BONUS
            referrer.extra_data = json.dumps(extra_data)
            
            await session.commit()
            logging.info(f"✅ Реферер {referrer_id} получил +{REFERRAL_SIGNUP_BONUS} монет за реферала {referral_id}")
            logging.info(f"📊 Теперь у реферера {referrer_id}: coins={referrer.coins}, count={referrer.referral_count}")
            
            return True
            
    except Exception as e:
        logging.error(f"❌ Ошибка начисления бонуса рефереру {referrer_id} за реферала {referral_id}: {e}")
        return False


async def get_referral_stats(user_id: int):
    """Получение реферальной статистики пользователя"""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(User).where(User.user_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                return {"count": 0, "earnings": 0}
            
            return {
                "count": user.referral_count or 0,
                "earnings": user.referral_earnings or 0
            }
            
    except Exception as e:
        logging.error(f"❌ Ошибка получения реферальной статистики для {user_id}: {e}")
        return {"count": 0, "earnings": 0}


async def get_referrals_list(user_id: int):
    """Получение списка рефералов пользователя"""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(User).where(User.referrer_id == user_id)
            )
            referrals = result.scalars().all()
            
            return [
                {
                    "user_id": ref.user_id,
                    "username": ref.username,
                    "joined_at": ref.created_at.isoformat() if ref.created_at else None,
                    "earned": REFERRAL_SIGNUP_BONUS
                }
                for ref in referrals
            ]
            
    except Exception as e:
        logging.error(f"❌ Ошибка получения списка рефералов для {user_id}: {e}")
        return []


async def get_completed_tasks(user_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserTask).where(UserTask.user_id == user_id)
        )
        tasks = result.scalars().all()
        return [task.task_id for task in tasks]


async def add_completed_task(user_id: int, task_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserTask).where(
                UserTask.user_id == user_id,
                UserTask.task_id == task_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            return False
        
        new_task = UserTask(
            user_id=user_id,
            task_id=task_id
        )
        session.add(new_task)
        await session.commit()
        return True


async def add_user(user_id: int, username: str = None, referrer_id: int = None):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing
        
        new_user = User(
            user_id=user_id,
            username=username or f"user_{user_id}",
            coins=0,
            profit_per_hour=get_hour_value(0),
            profit_per_tap=get_tap_value(0),
            energy=get_max_energy(0),
            max_energy=get_max_energy(0),
            level=0,
            multitap_level=0,
            profit_level=0,
            energy_level=0,
            luck_level=0,
            last_passive_income=datetime.utcnow(),
            last_energy_update=datetime.utcnow(),
            referrer_id=referrer_id,
            referral_count=0,
            referral_earnings=0,
            extra_data=json.dumps({"owned_skins": ["default.pngSP"], "selected_skin": "default.pngSP", "ads_watched": 0})
        )
        
        session.add(new_user)
        await session.commit()
        logging.info(f"✅ Пользователь {user_id} создан, referrer_id={referrer_id}")
        
        if referrer_id:
            logging.info(f"🎯 Попытка начисления бонуса: реферер {referrer_id} за реферала {user_id}")
            await add_referral_bonus(referrer_id, user_id)
        
        return new_user


async def update_user(user_id: int, data: dict):
    allowed_fields = {
        'username', 'coins', 'profit_per_hour', 'profit_per_tap', 'energy',
        'max_energy', 'level', 'multitap_level', 'profit_level', 'energy_level',
        'boost_level', 'last_passive_income', 'last_energy_update', 'referrer_id',
        'referral_count', 'referral_earnings', 'extra_data', 'luck_level'
    }
    unknown_fields = set(data) - allowed_fields
    if unknown_fields:
        raise ValueError(f"Unsupported update_user fields: {sorted(unknown_fields)}")

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None

        if 'coins' in data:
            user.coins = data['coins']
        if 'username' in data:
            user.username = data['username']
        if 'energy' in data:
            user.energy = data['energy']
        if 'profit_per_hour' in data:
            user.profit_per_hour = data['profit_per_hour']
        if 'profit_per_tap' in data:
            user.profit_per_tap = data['profit_per_tap']
        if 'max_energy' in data:
            user.max_energy = data['max_energy']
        if 'level' in data:
            user.level = data['level']
        if 'multitap_level' in data:
            user.multitap_level = data['multitap_level']
        if 'profit_level' in data:
            user.profit_level = data['profit_level']
        if 'energy_level' in data:
            user.energy_level = data['energy_level']
        if 'boost_level' in data:
            user.boost_level = data['boost_level']
        if 'last_passive_income' in data:
            user.last_passive_income = data['last_passive_income']
        if 'last_energy_update' in data:
            user.last_energy_update = data['last_energy_update']
        if 'referrer_id' in data:
            user.referrer_id = data['referrer_id']
        if 'referral_count' in data:
            user.referral_count = data['referral_count']
        if 'referral_earnings' in data:
            user.referral_earnings = data['referral_earnings']
        if 'luck_level' in data:
            user.luck_level = data['luck_level']
        if 'extra_data' in data:
            user.extra_data = json.dumps(data['extra_data'])

        await session.commit()

        return await get_user(user_id)


async def add_weekly_tournament_score(user_id: int, username: str | None, display_level: int, gained: int):
    if int(gained or 0) <= 0:
        return None

    starts_at, ends_at = get_weekly_tournament_season_window()
    season_key = get_weekly_tournament_season_key(starts_at)
    league = get_weekly_tournament_league(display_level)
    now = datetime.utcnow()

    async with AsyncSessionLocal() as session:
        await ensure_weekly_tournament_season(session, season_key, starts_at, ends_at)

        result = await session.execute(
            select(WeeklyTournamentEntry).where(
                WeeklyTournamentEntry.season_key == season_key,
                WeeklyTournamentEntry.user_id == user_id
            )
        )
        entry = result.scalar_one_or_none()

        if not entry:
            entry = WeeklyTournamentEntry(
                season_key=season_key,
                user_id=user_id,
                username=username,
                display_level=max(1, int(display_level or 1)),
                league=league,
                score=int(gained),
                updated_at=now,
            )
            session.add(entry)
        else:
            entry.username = username or entry.username
            entry.display_level = max(1, int(display_level or 1))
            entry.league = league
            entry.score = int(entry.score or 0) + int(gained)
            entry.updated_at = now

        await session.commit()
        return {
            "season_key": season_key,
            "league": league,
            "score": int(entry.score or 0),
        }


async def get_weekly_tournament_leaderboard(*, season_key: str | None = None, league: str | None = None, limit: int = 50):
    season_key = season_key or get_weekly_tournament_season_key()
    limit = max(1, min(200, int(limit or 50)))

    async with AsyncSessionLocal() as session:
        query = select(WeeklyTournamentEntry).where(WeeklyTournamentEntry.season_key == season_key)
        if league:
            query = query.where(WeeklyTournamentEntry.league == league)
        query = query.order_by(desc(WeeklyTournamentEntry.score), WeeklyTournamentEntry.updated_at.asc()).limit(limit)
        result = await session.execute(query)
        entries = result.scalars().all()

        rows = []
        for idx, entry in enumerate(entries, start=1):
            rows.append({
                "rank": idx,
                "user_id": entry.user_id,
                "username": entry.username,
                "display_level": int(entry.display_level or 1),
                "league": entry.league,
                "score": int(entry.score or 0),
                "eligible_for_payout": bool(entry.eligible_for_payout),
                "fraud_flag": bool(entry.fraud_flag),
                "updated_at": entry.updated_at.isoformat() if entry.updated_at else None,
            })
        return rows


async def get_weekly_tournament_player_entry(user_id: int, season_key: str | None = None):
    season_key = season_key or get_weekly_tournament_season_key()

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WeeklyTournamentEntry).where(
                WeeklyTournamentEntry.season_key == season_key,
                WeeklyTournamentEntry.user_id == user_id
            )
        )
        entry = result.scalar_one_or_none()
        if not entry:
            return None

        rank_query = await session.execute(
            select(func.count()).where(
                WeeklyTournamentEntry.season_key == season_key,
                WeeklyTournamentEntry.league == entry.league,
                WeeklyTournamentEntry.score > entry.score
            )
        )
        rank = int(rank_query.scalar() or 0) + 1

        return {
            "user_id": entry.user_id,
            "username": entry.username,
            "display_level": int(entry.display_level or 1),
            "league": entry.league,
            "score": int(entry.score or 0),
            "rank": rank,
            "eligible_for_payout": bool(entry.eligible_for_payout),
            "fraud_flag": bool(entry.fraud_flag),
            "updated_at": entry.updated_at.isoformat() if entry.updated_at else None,
        }


async def finalize_weekly_tournament_season(season_key: str):
    async with AsyncSessionLocal() as session:
        season_result = await session.execute(
            select(WeeklyTournamentSeason).where(WeeklyTournamentSeason.season_key == season_key)
        )
        season = season_result.scalar_one_or_none()
        if not season or season.status == "finalized":
            return False

        total_fund_cents = max(0, int(season.payout_fund_cents or 0))

        for league in WEEKLY_LEAGUE_ORDER:
            result = await session.execute(
                select(WeeklyTournamentEntry).where(
                    WeeklyTournamentEntry.season_key == season_key,
                    WeeklyTournamentEntry.league == league
                ).order_by(desc(WeeklyTournamentEntry.score), WeeklyTournamentEntry.updated_at.asc()).limit(50)
            )
            entries = result.scalars().all()
            league_fund_cents = int(total_fund_cents * WEEKLY_LEAGUE_FUND_SPLITS.get(league, 0))

            top_payouts = {
                rank: int(league_fund_cents * share)
                for rank, share in WEEKLY_TOP_PAYOUT_SPLITS.items()
            }
            rest_pool_cents = max(0, league_fund_cents - sum(top_payouts.values()))
            eligible_rest_entries = [
                entry for index, entry in enumerate(entries, start=1)
                if index >= 4 and bool(entry.eligible_for_payout) and not bool(entry.fraud_flag)
            ]
            rest_share_cents = 0
            rest_remainder_cents = 0
            if eligible_rest_entries:
                rest_share_cents = rest_pool_cents // len(eligible_rest_entries)
                rest_remainder_cents = rest_pool_cents % len(eligible_rest_entries)

            for idx, entry in enumerate(entries, start=1):
                payout_cents = 0
                if bool(entry.eligible_for_payout) and not bool(entry.fraud_flag):
                    if idx in top_payouts:
                        payout_cents = top_payouts[idx]
                    elif idx >= 4 and eligible_rest_entries:
                        payout_cents = rest_share_cents
                        if rest_remainder_cents > 0:
                            payout_cents += 1
                            rest_remainder_cents -= 1

                winner = WeeklyTournamentWinner(
                    season_key=season_key,
                    user_id=entry.user_id,
                    username=entry.username,
                    league=league,
                    rank=idx,
                    display_level=int(entry.display_level or 1),
                    score=int(entry.score or 0),
                    payout_cents=payout_cents,
                    eligible_for_payout=bool(entry.eligible_for_payout),
                    fraud_flag=bool(entry.fraud_flag),
                )
                session.add(winner)

        season.status = "finalized"
        season.settled_at = datetime.utcnow()
        await session.commit()
        return True


async def list_weekly_tournament_seasons(limit: int = 12):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WeeklyTournamentSeason)
            .order_by(desc(WeeklyTournamentSeason.starts_at))
            .limit(max(1, min(52, int(limit or 12))))
        )
        seasons = result.scalars().all()
        return [
            {
                "season_key": season.season_key,
                "starts_at": season.starts_at.isoformat() if season.starts_at else None,
                "ends_at": season.ends_at.isoformat() if season.ends_at else None,
                "status": season.status,
                "gross_ad_revenue_cents": int(season.gross_ad_revenue_cents or 0),
                "payout_fund_cents": int(season.payout_fund_cents or 0),
                "settled_at": season.settled_at.isoformat() if season.settled_at else None,
            }
            for season in seasons
        ]


async def get_weekly_tournament_winners(season_key: str, league: str | None = None):
    async with AsyncSessionLocal() as session:
        query = select(WeeklyTournamentWinner).where(WeeklyTournamentWinner.season_key == season_key)
        if league:
            query = query.where(WeeklyTournamentWinner.league == league)
        query = query.order_by(WeeklyTournamentWinner.league.asc(), WeeklyTournamentWinner.rank.asc())
        result = await session.execute(query)
        winners = result.scalars().all()
        return [
            {
                "season_key": winner.season_key,
                "user_id": winner.user_id,
                "username": winner.username,
                "league": winner.league,
                "rank": int(winner.rank or 0),
                "display_level": int(winner.display_level or 1),
                "score": int(winner.score or 0),
                "stars_reward": int(winner.stars_reward or 0),
                "payout_cents": int(winner.payout_cents or 0),
                "eligible_for_payout": bool(winner.eligible_for_payout),
                "fraud_flag": bool(winner.fraud_flag),
                "created_at": winner.created_at.isoformat() if winner.created_at else None,
            }
            for winner in winners
        ]
