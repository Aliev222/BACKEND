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


class TournamentData(BaseModel):
    user_id: int
    score: int
