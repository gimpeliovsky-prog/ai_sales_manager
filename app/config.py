from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    openai_api_key: str
    openai_model: str = "gpt-5.4-mini"
    license_server_url: str
    ai_agent_token: str
    redis_url: str = "redis://localhost:6379"
    session_ttl_seconds: int = 86400
    public_base_url: str = ""

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
