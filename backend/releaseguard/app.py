from __future__ import annotations

import asyncio
import hmac
import json
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncIterator

from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .kafka_gateway import KafkaGateway
from .models import CanaryRequest, RegressionRequest, ReleaseDecision
from .simulator import TelemetrySimulator
from .state import DemoController


settings = Settings()
controller = DemoController(settings)
gateway = KafkaGateway(settings, controller)
simulator = TelemetrySimulator(settings, controller, gateway)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await gateway.start()
    simulator_task = asyncio.create_task(simulator.run(), name="releaseguard-telemetry-simulator")
    try:
        yield
    finally:
        simulator.stop()
        simulator_task.cancel()
        with suppress(asyncio.CancelledError):
            await simulator_task
        await gateway.close()


app = FastAPI(
    title="ReleaseGuard API",
    version="1.0.0",
    description="Real-time canary safety demo powered by Confluent Cloud.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Last-Event-ID"],
)


@app.middleware("http")
async def limit_quick_tunnel_surface(request: Request, call_next):
    """A disposable public tunnel exposes only the connector webhook and health probe."""
    host = request.headers.get("host", "").split(":", 1)[0].lower()
    if host.endswith(".trycloudflare.com") and not (
        request.url.path == "/healthz"
        or request.url.path.startswith("/api/v1/release-decisions/")
    ):
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    return await call_next(request)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "kafka": "configured" if settings.kafka_enabled else "local-preview",
    }


@app.get("/api/demo/state")
async def demo_state() -> dict[str, Any]:
    return controller.snapshot()


@app.post("/api/demo/reset")
async def reset_demo() -> dict[str, Any]:
    snapshot = await controller.reset()
    simulator.reset_seed()
    event = snapshot["timeline"][-1]
    await gateway.publish_release_event(event)
    return snapshot


@app.post("/api/demo/canary")
async def launch_canary(request: CanaryRequest) -> dict[str, Any]:
    try:
        snapshot, event = await controller.launch_canary(request.version, request.traffic_percent)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await gateway.publish_release_event(event)
    return snapshot


@app.post("/api/demo/regression")
async def set_regression(request: RegressionRequest) -> dict[str, Any]:
    try:
        snapshot, event = await controller.set_regression(request.enabled)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await gateway.publish_release_event(event)
    return snapshot


@app.get("/api/events")
async def events(request: Request) -> StreamingResponse:
    queue = await controller.hub.subscribe()

    async def stream() -> AsyncIterator[str]:
        try:
            initial = json.dumps(controller.snapshot(), separators=(",", ":"))
            yield f"event: snapshot\ndata: {initial}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    envelope = await asyncio.wait_for(queue.get(), timeout=12)
                    payload = json.dumps(envelope["data"], separators=(",", ":"), default=str)
                    yield f"event: {envelope['type']}\ndata: {payload}\n\n"
                except TimeoutError:
                    yield f"event: heartbeat\ndata: {json.dumps({'at': __import__('time').time()})}\n\n"
        finally:
            controller.hub.unsubscribe(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/v1/release-decisions/{decision_id}")
async def receive_release_decision(
    decision_id: str,
    payload: dict[str, Any] | None = Body(default=None),
    authorization: str | None = Header(default=None),
) -> JSONResponse:
    expected = f"Bearer {settings.webhook_secret}"
    if authorization is None or not hmac.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook bearer token")

    if decision_id not in controller._known_decisions and payload:  # webhook can win the consumer race.
        candidate = payload.get("value", payload)
        candidate["decision_id"] = decision_id
        try:
            decision = ReleaseDecision.model_validate(candidate)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Invalid release decision: {exc}") from exc
        await controller.register_external_decision(decision)

    try:
        result = await controller.apply_rollback(decision_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Decision has not reached ReleaseGuard") from exc

    await gateway.publish_action(result.model_dump())
    return JSONResponse(status_code=200, content=result.model_dump())


def _mount_frontend() -> None:
    dist = settings.frontend_dist
    if not dist.exists():
        return
    assets = dist / "_next"
    if assets.exists():
        app.mount("/_next", StaticFiles(directory=assets), name="next-assets")
    for static_name in ("favicon.svg", "favicon.ico"):
        file_path = dist / static_name
        if file_path.exists():
            app.add_api_route(
                f"/{static_name}",
                lambda path=file_path: FileResponse(path),
                include_in_schema=False,
            )

    @app.get("/{full_path:path}", include_in_schema=False)
    async def frontend(full_path: str) -> FileResponse:
        candidate = dist / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        nested_index = candidate / "index.html"
        if nested_index.is_file():
            return FileResponse(nested_index)
        return FileResponse(dist / "index.html")


_mount_frontend()
