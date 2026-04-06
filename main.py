import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import telegram, webchat, whatsapp
from app.session_store import init_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_redis()
    yield


app = FastAPI(title="AI Sales Manager", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

app.include_router(telegram.router, prefix="/webhook/telegram")
app.include_router(whatsapp.router, prefix="/webhook/whatsapp")
app.include_router(webchat.router, prefix="/webhook/webchat")


@app.get("/health")
def health():
    return {"status": "ok"}
