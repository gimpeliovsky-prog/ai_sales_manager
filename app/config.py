from functools import lru_cache

from pydantic_settings import SettingsConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    openai_api_key: str
    openai_model: str = "gpt-5.4-mini"
    license_server_url: str
    ai_agent_token: str
    redis_url: str = "redis://localhost:6379"
    session_ttl_seconds: int = 86400
    public_base_url: str = ""
    session_secret: str = "ai-sales-manager-session-secret"
    lead_followup_worker_enabled: bool = True
    lead_followup_scan_interval_seconds: int = 300
    lead_followup_scan_batch_size: int = 500
    lead_stalled_after_minutes: int = 60
    sales_lead_persistence_enabled: bool = True
    sales_lead_repository_backend: str = "redis"
    sales_lead_postgres_dsn: str = ""
    sales_lead_postgres_db: str = "ai_sales"
    sales_lead_postgres_user: str = "ai_sales"
    sales_lead_postgres_password: str = "ai_sales"
    sales_lead_retention_days: int = 180
    sales_lead_timeline_limit: int = 100
    sales_lead_max_per_company: int = 50000
    sales_crm_sync_enabled: bool = False
    sales_crm_sync_webhook_url: str = ""
    sales_crm_sync_worker_enabled: bool = True
    sales_crm_sync_scan_interval_seconds: int = 60
    sales_crm_sync_batch_size: int = 100
    sales_crm_sync_max_attempts: int = 8
    sales_crm_sync_outbox_retention_days: int = 30
    sales_dashboard_read_token: str = ""
    sales_dashboard_manager_token: str = ""
    sales_dashboard_admin_token: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
