from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import asyncio
import uvicorn
import random
import time
import hashlib
import hmac
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import os
import logging
from contextlib import asynccontextmanager

from DATABASE.base import (
    get_user, add_user as create_user, update_user, 
    init_db, get_completed_tasks, add_completed_task
)

# ==================== CONFIGURATION ====================

# Anti-cheat constants
MAX_CLICKS_PER_SECOND = 100
MAX_ENERGY_RECOVERY_PER_SECOND = 10
MIN_TASK_COMPLETION_INTERVAL = 60  # seconds
MAX_REWARD_PER_VIDEO = 5000
MAX_BET = 1000000
MIN_BET = 10

UPGRADE_PRICES = {
    "multitap": [100, 200, 600, 900, 1500, 2000, 4000, 7000, 10000, 20000, 30000, 40000, 50000, 100000],
    "profit":   [100, 200, 600, 900, 1500, 2000, 4000, 7000, 10000, 20000, 30000, 40000, 50000, 100000],
    "energy":   [100, 200, 600, 900, 1500, 2000, 4000, 7000, 10000, 20000, 30000, 40000, 50000, 100000],
}

HOUR_VALUES = [100, 150, 250, 500, 1000, 1250, 1500, 1800, 2000, 2500, 3000]
ENERGY_VALUES = [100, 200, 300, 500, 650, 700, 850, 1000, 1150, 1200, 1400]

SKIN_BONUSES = {
    'default_cat': {'type': 'multiplier', 'value': 1.0},
    'black_cat': {'type': 'multiplier', 'value': 1.1},
    'white_cat': {'type': 'multiplier', 'value': 1.15},
    'gold_cat': {'type': 'multiplier', 'value': 1.5},
    'space_cat': {'type': 'interval', 'value': 8},
    'ninja_cat': {'type': 'multiplier', 'value': 2.0},
    'wizard_cat': {'type': 'both', 'multiplier': 1.8, 'interval': 7},
    'rainbow_cat': {'type': 'multiplier', 'value': 3.0},
    'alien_cat': {'type': 'interval', 'value': 5}
}

# In-memory rate limiting storage (use Redis in production)
_rate_limit_store: Dict[str, List[float]] = {}
_click_spam_store: Dict[str, List[float]] = {}

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    logger.info("🚀 Starting Ryoho Clicker API")
    try:
        await init_db()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
    yield
    logger.info("🛑 Shutting down Ryoho Clicker API")

# ==================== INITIALIZATION ====================

app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)

# ==================== MIDDLEWARE ====================

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Basic rate limiting middleware"""
    client_ip = request.client.host if request.client else "unknown"
    path = request.url.path
    
    # Skip rate limiting for health checks
    if path in ["/health", "/"]:
        return await call_next(request)
    
    # Rate limit by IP and endpoint
    key = f"{client_ip}:{path}"
    now = time.time()
    
    if key not in _rate_limit_store:
        _rate_limit_store[key] = []
    
    # Clean old requests (older than 60 seconds)
    _rate_limit_store[key] = [t for t in _rate_limit_store[key] if now - t < 60]
    
    # Check rate (max 60 requests per minute per endpoint)
    if len(_rate_limit_store[key]) >= 60:
        logger.warning(f"Rate limit exceeded for {key}")
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please slow down."}
        )
    
    _rate_limit_store[key].append(now)
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ryoho-eta.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== MODELS ====================

class ClickRequest(BaseModel):
    user_id: int
    clicks: int = Field(1, ge=1, le=100)
    energy_cost: int = Field(1, ge=1, le=10)
    
    @validator('user_id')
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError('Invalid user_id')
        return v

class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str
    
    @validator('boost_type')
    def validate_boost_type(cls, v):
        if v not in UPGRADE_PRICES:
            raise ValueError('Invalid boost type')
        return v

class UserIdRequest(BaseModel):
    user_id: int
    
    @validator('user_id')
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError('Invalid user_id')
        return v

class GameRequest(BaseModel):
    user_id: int
    bet: int = Field(..., ge=MIN_BET, le=MAX_BET)
    color: Optional[str] = None
    bet_type: Optional[str] = None
    bet_value: Optional[int] = Field(None, ge=0, le=36)
    prediction: Optional[str] = None

class TaskCompleteRequest(BaseModel):
    user_id: int
    task_id: str

class RegisterRequest(BaseModel):
    user_id: int
    username: Optional[str] = None
    referrer_id: Optional[int] = None
    
    @validator('user_id')
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError('Invalid user_id')
        return v

class BoostActivateRequest(BaseModel):
    user_id: int

class PassiveIncomeRequest(BaseModel):
    user_id: int
    skin_bonus: Optional[Dict[str, Any]] = None

class RewardVideoRequest(BaseModel):
    user_id: int
    reward: int = Field(MAX_REWARD_PER_VIDEO, le=MAX_REWARD_PER_VIDEO)

# ==================== HEALTH ENDPOINTS ====================

@app.get("/health")
@app.get("/")
async def root():
    return {
        "status": "ok", 
        "message": "Ryoho Clicker API is running",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/health/db")
async def check_db_endpoint():
    """Separate endpoint for DB health check"""
    try:
        await get_user(0)
        return {"database": "connected"}
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        return {"database": "disconnected", "error": str(e)}

# ==================== HELPER FUNCTIONS ====================

def get_tap_value(level: int) -> int:
    """Calculate tap value based on level"""
    return 1 + level

def get_hour_value(level: int) -> int:
    """Calculate hourly profit based on level"""
    if level >= len(HOUR_VALUES):
        return HOUR_VALUES[-1] * (2 ** (level - len(HOUR_VALUES) + 1))
    return HOUR_VALUES[level]

def get_max_energy(level: int) -> int:
    """Calculate max energy based on level"""
    if level >= len(ENERGY_VALUES):
        return ENERGY_VALUES[-1] * (1.5 ** (level - len(ENERGY_VALUES) + 1))
    return ENERGY_VALUES[level]

def check_click_spam(user_id: int) -> bool:
    """Check if user is clicking too fast"""
    key = f"click_{user_id}"
    now = time.time()
    
    if key not in _click_spam_store:
        _click_spam_store[key] = []
    
    # Clean old clicks (older than 1 second)
    _click_spam_store[key] = [t for t in _click_spam_store[key] if now - t < 1]
    
    if len(_click_spam_store[key]) >= MAX_CLICKS_PER_SECOND:
        logger.warning(f"Click spam detected for user {user_id}")
        return False
    
    _click_spam_store[key].append(now)
    return True

def calculate_passive_income(user: Dict, minutes_passed: int, bonus_multiplier: float = 1.0) -> int:
    """Calculate passive income based on time passed"""
    hour_value = get_hour_value(user.get("profit_level", 0))
    base_income_per_10min = hour_value // 6
    cycles = max(1, minutes_passed // 10)
    return int(base_income_per_10min * cycles * bonus_multiplier)

# ==================== API ENDPOINTS ====================

@app.get("/api/check-referral/{user_id}")
async def check_referral(user_id: int):
    """Check if user has a referrer"""
    try:
        user = await get_user(user_id)
        if not user:
            return {"has_referrer": False}
        
        return {
            "has_referrer": user.get("referrer_id") is not None,
            "referrer_id": user.get("referrer_id")
        }
    except Exception as e:
        logger.error(f"Error in check_referral: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    """Get user data"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found. Please register first.")
        
        return {
            "coins": user.get("coins", 0),
            "energy": user.get("energy", 0),
            "max_energy": user.get("max_energy", 1000),
            "profit_per_tap": get_tap_value(user.get("multitap_level", 0)),
            "profit_per_hour": get_hour_value(user.get("profit_level", 0)),
            "multitap_level": user.get("multitap_level", 0),
            "profit_level": user.get("profit_level", 0),
            "energy_level": user.get("energy_level", 0),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/click")
async def process_click(request: ClickRequest):
    """Process a click with anti-cheat protection"""
    try:
        # Anti-spam check
        if not check_click_spam(request.user_id):
            raise HTTPException(status_code=429, detail="Too many clicks. Please slow down.")
        
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Check mega boost
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        mega_boost_active = False
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now <= expires:
                    mega_boost_active = True
                else:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(request.user_id, {"extra_data": extra})
            except (ValueError, KeyError):
                # Invalid boost data, remove it
                del active_boosts["mega_boost"]
                extra["active_boosts"] = active_boosts
                await update_user(request.user_id, {"extra_data": extra})
        
        # Calculate gain
        base_tap = get_tap_value(user.get("multitap_level", 0))
        actual_gain = base_tap * (2 if mega_boost_active else 1)
        
        # Validate energy
        if not mega_boost_active:
            if user.get("energy", 0) < 1:
                raise HTTPException(status_code=400, detail="Not enough energy")
            user["energy"] = user.get("energy", 0) - 1
        
        # Update coins
        user["coins"] = user.get("coins", 0) + actual_gain
        
        # Save to DB
        await update_user(request.user_id, {
            "coins": user["coins"],
            "energy": user["energy"]
        })
        
        return {
            "coins": user["coins"],
            "energy": user["energy"],
            "tap_value": base_tap,
            "multiplier": 2 if mega_boost_active else 1,
            "actual_gain": actual_gain,
            "mega_boost_active": mega_boost_active
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_click: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/upgrade")
async def process_upgrade(request: UpgradeRequest):
    """Process an upgrade with validation"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        boost_type = request.boost_type
        current_level = user.get(f"{boost_type}_level", 0)
        
        if current_level >= len(UPGRADE_PRICES[boost_type]):
            raise HTTPException(status_code=400, detail="Max level reached")
        
        price = UPGRADE_PRICES[boost_type][current_level]
        if user.get("coins", 0) < price:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        # Atomic update simulation (use DB transaction in production)
        user["coins"] -= price
        user[f"{boost_type}_level"] = current_level + 1
        
        updates = {
            "coins": user["coins"],
            f"{boost_type}_level": current_level + 1
        }
        
        if boost_type == "profit":
            updates["profit_per_hour"] = get_hour_value(current_level + 1)
        elif boost_type == "energy":
            new_max = get_max_energy(current_level + 1)
            updates["max_energy"] = new_max
            updates["energy"] = new_max
        
        await update_user(request.user_id, updates)
        
        # Get fresh user data
        updated_user = await get_user(request.user_id)
        
        return {
            "coins": updated_user["coins"],
            "new_level": updated_user[f"{boost_type}_level"],
            "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
            "profit_per_tap": get_tap_value(updated_user["multitap_level"]),
            "profit_per_hour": get_hour_value(updated_user["profit_level"]),
            "max_energy": updated_user["max_energy"],
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/recover-energy")
async def recover_energy(data: UserIdRequest):
    """Recover energy at a fixed rate"""
    try:
        user = await get_user(data.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("energy", 0) >= user.get("max_energy", 1000):
            return {"energy": user["energy"]}
        
        # Always +1 per second
        new_energy = min(user.get("max_energy", 1000), user.get("energy", 0) + MAX_ENERGY_RECOVERY_PER_SECOND)
        await update_user(data.user_id, {"energy": new_energy})
        
        return {"energy": new_energy}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in recover_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    """Get current upgrade prices for user"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        prices = {}
        for boost in UPGRADE_PRICES:
            level = user.get(f"{boost}_level", 0)
            prices[boost] = UPGRADE_PRICES[boost][level] if level < len(UPGRADE_PRICES[boost]) else 0
        
        return prices
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_upgrade_prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/register")
async def register_user(request: RegisterRequest):
    """Register new user with referral"""
    try:
        # Check if user exists
        existing = await get_user(request.user_id)
        if existing:
            return {"status": "exists", "user": existing}
        
        # Create new user
        await create_user(
            user_id=request.user_id, 
            username=request.username,
            referrer_id=request.referrer_id
        )
        
        user = await get_user(request.user_id)
        
        if request.referrer_id:
            return {
                "status": "created_with_referral",
                "user": user,
                "message": f"Welcome! You were invited by {request.referrer_id}"
            }
        
        return {"status": "created", "user": user}
        
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video")
async def reward_video(request: RewardVideoRequest):
    """Process video reward with fixed amount"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Fixed reward amount (not from client)
        reward = MAX_REWARD_PER_VIDEO
        
        user["coins"] = user.get("coins", 0) + reward
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {"success": True, "coins": user["coins"]}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reward_video: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== BOOSTS ====================

@app.post("/api/activate-boost")
@app.post("/api/activate-mega-boost")
async def activate_mega_boost(request: BoostActivateRequest):
    """Activate mega boost (x2 coins + infinite energy for 2 minutes)"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        # Check if boost already active
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now < expires:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "success": False,
                        "message": f"Boost already active! {remaining // 60}:{remaining % 60:02d} remaining",
                        "already_active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"]
                    }
            except (ValueError, KeyError):
                # Invalid data, remove it
                del active_boosts["mega_boost"]
        
        # Activate boost for 2 minutes
        expires_at = (now + timedelta(minutes=2)).isoformat()
        active_boosts["mega_boost"] = {
            "active": True,
            "expires_at": expires_at
        }
        
        extra["active_boosts"] = active_boosts
        await update_user(request.user_id, {"extra_data": extra})
        
        return {
            "success": True,
            "message": "🔥⚡ MEGA BOOST activated for 2 minutes! x2 coins + infinite energy",
            "expires_at": expires_at
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/boosts/{user_id}")
@app.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int):
    """Get mega boost status"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        # Check and clean expired boost
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now > expires:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
                    return {"active": False}
                else:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"],
                        "remaining_seconds": remaining
                    }
            except (ValueError, KeyError):
                # Invalid data, remove it
                if "mega_boost" in active_boosts:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
        
        return {"active": False}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== REFERRALS ====================

@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int):
    """Get referral statistics"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "count": user.get("referral_count", 0),
            "earnings": user.get("referral_earnings", 0)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/migrate-referrals")
async def migrate_referrals():
    """Migration endpoint (safe to call multiple times)"""
    try:
        from sqlalchemy import create_engine, inspect, text
        
        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        sync_engine = create_engine(db_url.replace("+asyncpg", ""))
        
        with sync_engine.connect() as conn:
            inspector = inspect(sync_engine)
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            added = []
            if 'referrer_id' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN referrer_id BIGINT"))
                added.append('referrer_id')
            if 'referral_count' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN referral_count INTEGER DEFAULT 0"))
                added.append('referral_count')
            if 'referral_earnings' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN referral_earnings BIGINT DEFAULT 0"))
                added.append('referral_earnings')
            if 'created_at' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
                added.append('created_at')
            
            conn.commit()
            
            return {
                "status": "success",
                "message": f"Columns added: {added}",
                "columns": columns + added
            }
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return {"status": "error", "error": str(e)}

# ==================== TASKS ====================

# In-memory task completion tracking (use Redis in production)
_task_completion_store: Dict[str, Dict[str, float]] = {}

@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int):
    """Get available tasks"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        completed_tasks = await get_completed_tasks(user_id) or []
        
        tasks = [
            {
                "id": "daily_bonus",
                "title": "📅 Daily Bonus",
                "description": "Come back every day for rewards",
                "reward": "25000 coins",
                "icon": "📅",
                "completed": "daily_bonus" in completed_tasks,
                "progress": 0,
                "total": 1
            },
            {
                "id": "energy_refill",
                "title": "⚡ Infinite Energy",
                "description": "5 minutes of unlimited energy",
                "reward": "⚡ 5 minutes",
                "icon": "⚡",
                "completed": "energy_refill" in completed_tasks,
                "progress": 0,
                "total": 1
            },
            {
                "id": "link_click",
                "title": "🔗 Follow Link",
                "description": "Click the link and get reward",
                "reward": "25000 coins",
                "icon": "🔗",
                "completed": False,  # Always available
                "progress": 0,
                "total": 1
            },
            {
                "id": "invite_5_friends",
                "title": "👥 Invite 5 Friends",
                "description": "Invite 5 friends to the game",
                "reward": "20000 coins",
                "icon": "👥",
                "completed": "invite_5_friends" in completed_tasks,
                "progress": min(user.get("referral_count", 0), 5),
                "total": 5
            }
        ]
        
        return tasks
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/complete-task")
async def complete_task(request: TaskCompleteRequest):
    """Complete a task with anti-cheat protection"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        task_id = request.task_id
        now = datetime.utcnow()
        now_timestamp = time.time()
        
        # Anti-spam for link_click
        if task_id == "link_click":
            spam_key = f"link_click:{request.user_id}"
            last_click = _task_completion_store.get(spam_key, {}).get("last", 0)
            
            if now_timestamp - last_click < MIN_TASK_COMPLETION_INTERVAL:
                logger.warning(f"Link click spam detected for user {request.user_id}")
                raise HTTPException(status_code=429, detail="Please wait before clicking again")
            
            _task_completion_store[spam_key] = {"last": now_timestamp}
            
            # Award coins
            user["coins"] = user.get("coins", 0) + 25000
            await update_user(request.user_id, {"coins": user["coins"]})
            
            return {
                "success": True, 
                "message": "🔗 +25000 coins for clicking!",
                "coins": user["coins"]
            }
        
        # One-time tasks
        completed = await get_completed_tasks(request.user_id) or []
        
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")
        
        message = ""
        updates = {}
        
        if task_id == "daily_bonus":
            user["coins"] = user.get("coins", 0) + 25000
            message = "🎁 +25000 coins (daily bonus)"
            updates = {"coins": user["coins"]}
            await add_completed_task(request.user_id, task_id)
            
        elif task_id == "energy_refill":
            message = "⚡ Infinite energy activated for 5 minutes!"
            await add_completed_task(request.user_id, task_id)
            
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] = user.get("coins", 0) + 20000
                message = "👥 +20000 coins for 5 friends!"
                updates = {"coins": user["coins"]}
                await add_completed_task(request.user_id, task_id)
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")
        
        if updates:
            await update_user(request.user_id, updates)
        
        return {"success": True, "message": message, "coins": user["coins"]}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== MINI-GAMES ====================

@app.post("/api/game/coinflip")
async def play_coinflip(request: GameRequest):
    """Play coinflip game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        win = random.choice([True, False])
        
        if win:
            user["coins"] = user.get("coins", 0) + request.bet
            message = f"🎉 You won +{request.bet} coins!"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "result": "win" if win else "lose",
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_coinflip: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/slots")
async def play_slots(request: GameRequest):
    """Play slots game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        symbols = ["🍒", "🍋", "🍊", "7️⃣", "💎"]
        slots = [random.choice(symbols) for _ in range(3)]
        win = len(set(slots)) == 1
        multiplier = 10 if "7️⃣" in slots and win else 5 if "💎" in slots and win else 3
        
        if win:
            win_amount = request.bet * multiplier
            user["coins"] = user.get("coins", 0) + win_amount
            message = f"🎰 JACKPOT! +{win_amount} coins (x{multiplier})"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "slots": slots,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_slots: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/dice")
async def play_dice(request: GameRequest):
    """Play dice game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        dice1 = random.randint(1, 6)
        dice2 = random.randint(1, 6)
        total = dice1 + dice2
        
        win = False
        multiplier = 1
        
        if request.prediction == "7" and total == 7:
            win = True
            multiplier = 5
        elif request.prediction == "even" and total % 2 == 0:
            win = True
            multiplier = 2
        elif request.prediction == "odd" and total % 2 == 1:
            win = True
            multiplier = 2
        
        if win:
            win_amount = request.bet * multiplier
            user["coins"] = user.get("coins", 0) + win_amount
            message = f"🎲 You won +{win_amount} coins (x{multiplier})"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "dice1": dice1,
            "dice2": dice2,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/roulette")
async def play_roulette(request: GameRequest):
    """Play roulette game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        red_numbers = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]
        
        result = random.randint(0, 36)
        
        if result == 0:
            result_color = 'green'
            result_symbol = '🟢'
        elif result in red_numbers:
            result_color = 'red'
            result_symbol = '🔴'
        else:
            result_color = 'black'
            result_symbol = '⚫'
        
        win = False
        multiplier = 0
        
        if request.bet_type == 'number' and request.bet_value == result:
            win = True
            multiplier = 35
        elif request.bet_type == 'green' and result_color == 'green':
            win = True
            multiplier = 35
        elif request.bet_type == result_color:
            win = True
            multiplier = 2
        
        if win:
            win_amount = request.bet * multiplier
            user["coins"] = user.get("coins", 0) + win_amount
            message = f"🎉 {result_symbol} {result} - You won +{win_amount} coins! (x{multiplier})"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 {result_symbol} {result} - You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "result_number": result,
            "result_color": result_color,
            "result_symbol": result_symbol,
            "win": win,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_roulette: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== PASSIVE INCOME ====================

@app.post("/api/passive-income")
async def passive_income(request: PassiveIncomeRequest):
    """Collect passive income with skin bonuses"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        last_income = user.get('last_passive_income')
        now = datetime.utcnow()
        
        # Base interval 10 minutes
        base_interval = timedelta(minutes=10)
        
        # Apply skin bonus
        multiplier = 1.0
        interval = base_interval
        
        if request.skin_bonus:
            bonus_type = request.skin_bonus.get('type')
            
            if bonus_type == 'multiplier':
                multiplier = request.skin_bonus.get('value', 1.0)
            elif bonus_type == 'interval':
                interval = timedelta(minutes=request.skin_bonus.get('value', 10))
            elif bonus_type == 'both':
                multiplier = request.skin_bonus.get('multiplier', 1.0)
                interval = timedelta(minutes=request.skin_bonus.get('interval', 10))
        
        # Calculate income
        if not last_income or (now - last_income) >= interval:
            minutes_passed = int((now - last_income).total_seconds() / 60) if last_income else 0
            cycles = max(1, minutes_passed // 10) if last_income else 1
            
            total_income = calculate_passive_income(user, minutes_passed if last_income else 10, multiplier)
            
            if total_income > 0:
                user["coins"] = user.get("coins", 0) + total_income
                await update_user(request.user_id, {
                    "coins": user["coins"],
                    "last_passive_income": now
                })
                
                return {
                    "coins": user["coins"],
                    "income": total_income,
                    "message": f"💰 +{total_income} coins (skin bonus x{multiplier})"
                }
        
        return {"coins": user["coins"], "income": 0}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== LAUNCH ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "api:app", 
        host="0.0.0.0", 
        port=port, 
        reload=False,  # Disable reload in production
        workers=4      # Multiple workers for concurrency
    )