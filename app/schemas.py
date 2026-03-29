from typing import Optional, List
from pydantic import BaseModel


class MessageOut(BaseModel):
    id: int
    discord_message_id: str
    channel_id: str
    channel_name: Optional[str]
    author_name: Optional[str]
    content: str
    parse_status: str
    deal_type: Optional[str]
    amount: Optional[float]
    payment_method: Optional[str]
    category: Optional[str]
    notes: Optional[str]
    confidence: Optional[float]
    needs_review: bool


class RetryRequest(BaseModel):
    ids: List[int]


class HealthOut(BaseModel):
    ok: bool