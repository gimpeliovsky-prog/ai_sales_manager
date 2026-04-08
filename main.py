import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
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


@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>AI Sales Manager</title>
        <style>
          body { font-family: Arial, sans-serif; max-width: 760px; margin: 48px auto; padding: 0 20px; color: #1f2937; }
          h1 { margin-bottom: 8px; }
          p { line-height: 1.5; }
          code { background: #f3f4f6; padding: 2px 5px; border-radius: 4px; }
          a { color: #2563eb; }
          .card { border: 1px solid #e5e7eb; border-radius: 12px; padding: 18px; margin: 16px 0; }
        </style>
      </head>
      <body>
        <h1>AI Sales Manager</h1>
        <p>Backend API is running. This service does not include a full dashboard frontend yet.</p>
        <div class="card">
          <p><a href="/health">Health check</a></p>
          <p><a href="/docs">API docs</a></p>
          <p><code>GET /sales/summary?company_code=...</code> requires <code>X-AI-Agent-Token</code>.</p>
          <p><code>GET /sales/leads?company_code=...</code> requires <code>X-AI-Agent-Token</code>.</p>
          <p><code>GET /sales/dashboard/contract</code> returns the dashboard API contract.</p>
        </div>
      </body>
    </html>
    """


@app.get("/health")
def health():
    return {"status": "ok"}
