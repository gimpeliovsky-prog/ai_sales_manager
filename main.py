import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import sales_dashboard, telegram, webchat, whatsapp
from app.lead_followup_worker import get_lead_followup_worker
from app.sales_crm_sync_worker import get_sales_crm_sync_worker
from app.sales_lead_repository import close_sales_lead_repository, init_sales_lead_repository
from app.session_store import init_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_redis()
    await init_sales_lead_repository()
    worker = get_lead_followup_worker()
    crm_sync_worker = get_sales_crm_sync_worker()
    worker.start()
    crm_sync_worker.start()
    try:
        yield
    finally:
        await worker.stop()
        await crm_sync_worker.stop()
        await close_sales_lead_repository()


app = FastAPI(title="AI Sales Manager", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

app.include_router(telegram.router, prefix="/webhook/telegram")
app.include_router(whatsapp.router, prefix="/webhook/whatsapp")
app.include_router(webchat.router, prefix="/webhook/webchat")
app.include_router(sales_dashboard.router, prefix="/sales")


@app.get("/health")
def health():
    return {"status": "ok"}
