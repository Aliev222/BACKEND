from typing import Optional

from pydantic import BaseModel, Field


class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str


class UserIdRequest(BaseModel):
    user_id: int


class RegisterRequest(BaseModel):
    user_id: int
    username: Optional[str] = None
    referrer_id: Optional[int] = None


class SkinRequest(BaseModel):
    user_id: int
    skin_id: str


class GameRequest(BaseModel):
    user_id: int
    bet: int = Field(..., ge=10, le=1000000)
    prediction: Optional[str] = None
    bet_type: Optional[str] = None
    bet_value: Optional[int] = None


class LuckyBoxRequest(BaseModel):
    user_id: int
    bet: int = Field(..., ge=10, le=1000000)
    box_index: int = Field(..., ge=0, le=3)


class CrashGameStartRequest(BaseModel):
    user_id: int
    bet: int = Field(..., ge=10, le=1000000)


class CrashGameCashoutRequest(BaseModel):
    user_id: int
    session_id: str


class TaskCompleteRequest(BaseModel):
    user_id: int
    task_id: str


class PassiveIncomeRequest(BaseModel):
    user_id: int


class BoostActivateRequest(BaseModel):
    user_id: int


class EnergySyncRequest(BaseModel):
    user_id: int


class ClicksBatchRequest(BaseModel):
    user_id: int
    clicks: int = Field(..., ge=1, le=500)
    batch_id: str


class RewardVideoStartRequest(BaseModel):
    user_id: int


class RewardVideoClaimRequest(BaseModel):
    user_id: int
    ad_session_id: str


class AdActionStartRequest(BaseModel):
    user_id: int
    action: str


class AdActionClaimRequest(BaseModel):
    user_id: int
    ad_session_id: str
    skin_id: Optional[str] = None


class VideoTaskClaimRequest(BaseModel):
    user_id: int
    ad_session_id: str
    task_id: str


class TournamentData(BaseModel):
    user_id: int
    score: int


class WeeklyTournamentFundRequest(BaseModel):
    gross_ad_revenue_cents: int = Field(..., ge=0)
    payout_fund_cents: int = Field(..., ge=0)


class AdminFraudUpdateRequest(BaseModel):
    status: str = Field(..., min_length=2, max_length=16)
    reason: Optional[str] = None
    disqualify_from_payout: bool = False
    season_key: Optional[str] = None


class AdminWinnerStarsUpdateRequest(BaseModel):
    user_id: int
    stars_reward: int = Field(..., ge=0)
