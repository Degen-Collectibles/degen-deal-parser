from functools import lru_cache
from pathlib import Path
from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB = BASE_DIR / "data" / "degen_live.db"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Degen Live Deal Parser"
    database_url: str = f"sqlite:///{DEFAULT_DB.as_posix()}"

    discord_bot_token: str = Field(alias="DISCORD_BOT_TOKEN")
    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    discord_channel_ids: str = Field(default="", alias="DISCORD_CHANNEL_IDS")

    parser_poll_seconds: float = 2.0
    parser_batch_size: int = 10
    parser_max_attempts: int = 3

    startup_backfill_enabled: bool = True
    startup_backfill_limit_per_channel: int = 500
    startup_backfill_oldest_first: bool = True

    stitch_enabled: bool = True
    stitch_window_seconds: int = 30
    stitch_max_messages: int = 2

    @property
    def channel_ids(self) -> List[int]:
        return [int(x.strip()) for x in self.discord_channel_ids.split(",") if x.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()