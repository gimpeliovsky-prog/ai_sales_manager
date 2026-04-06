# AI Sales Manager

Standalone AI sales runtime for ERPNext tenants.

Structure:
- `app/`: runtime orchestration, tools, channel adapters
- `evals/`: regression checks for conversation flow and tool policy
- `main.py`: FastAPI entrypoint

Runtime depends on `license_server` as the control plane for tenant routing, buyer resolution, policies, handoffs, and transcript ingestion.
