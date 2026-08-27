from __future__ import annotations

import asyncio
import hmac
import json
import logging
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from urllib.parse import unquote

from fastapi import APIRouter, Body, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .kafka_gateway import KafkaGateway
from .models import CanaryRequest, RegressionRequest, ReleaseDecision
from .simulator import TelemetrySimulator
from .state import DemoController


logger = logging.getLogger("releaseguard.webhook")
router = APIRouter()


class GatewayDependency(Protocol):
    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def publish_release_event(self, event: dict[str, Any]) -> None: ...

    async def publish_metric(self, metric: Any) -> None: ...

    async def publish_decision(self, decision: ReleaseDecision) -> None: ...

    async def publish_action(self, action: dict[str, Any]) -> None: ...


class SimulatorDependency(Protocol):
    async def run(self) -> None: ...

    def stop(self) -> None: ...

    def reset_seed(self) -> None: ...


@asynccontextmanager
async def _lifespan(application: FastAPI) -> AsyncIterator[None]:
    gateway: GatewayDependency = application.state.gateway
    simulator: SimulatorDependency = application.state.simulator
    await gateway.start()
    simulator_task = asyncio.create_task(
        simulator.run(),
        name="releaseguard-telemetry-simulator",
    )
    try:
        yield
    finally:
        simulator.stop()
        simulator_task.cancel()
        with suppress(asyncio.CancelledError):
            await simulator_task
        await gateway.close()


@router.get("/healthz")
async def healthz(request: Request) -> dict[str, Any]:
    app_settings: Settings = request.app.state.settings
    return {
        "status": "ok",
        "service": app_settings.app_name,
        "kafka": "configured" if app_settings.kafka_enabled else "local-preview",
    }


@router.get("/api/demo/state")
async def demo_state(request: Request) -> dict[str, Any]:
    controller: DemoController = request.app.state.controller
    return controller.snapshot()


@router.post("/api/demo/reset")
async def reset_demo(request: Request) -> dict[str, Any]:
    controller: DemoController = request.app.state.controller
    gateway: GatewayDependency = request.app.state.gateway
    simulator: SimulatorDependency = request.app.state.simulator
    snapshot = await controller.reset()
    simulator.reset_seed()
    event = snapshot["timeline"][-1]
    await gateway.publish_release_event(event)
    return snapshot


@router.post("/api/demo/canary")
async def launch_canary(payload: CanaryRequest, request: Request) -> dict[str, Any]:
    controller: DemoController = request.app.state.controller
    gateway: GatewayDependency = request.app.state.gateway
    try:
        snapshot, event = await controller.launch_canary(payload.version, payload.traffic_percent)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await gateway.publish_release_event(event)
    return snapshot


@router.post("/api/demo/regression")
async def set_regression(payload: RegressionRequest, request: Request) -> dict[str, Any]:
    controller: DemoController = request.app.state.controller
    gateway: GatewayDependency = request.app.state.gateway
    try:
        snapshot, event = await controller.set_regression(payload.enabled)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await gateway.publish_release_event(event)
    return snapshot


@router.get("/api/events")
async def events(request: Request) -> StreamingResponse:
    controller: DemoController = request.app.state.controller
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


@router.post("/api/v1/release-decisions/{decision_id}")
async def receive_release_decision(
    decision_id: str,
    request: Request,
    payload: Any = Body(default=None),
    authorization: str | None = Header(default=None),
) -> JSONResponse:
    app_settings: Settings = request.app.state.settings
    controller: DemoController = request.app.state.controller
    gateway: GatewayDependency = request.app.state.gateway
    expected = f"Bearer {app_settings.webhook_secret}"
    if authorization is None or not hmac.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook bearer token")

    if payload is not None:  # The webhook can win the dashboard consumer race.
        try:
            candidate = _connector_decision_payload(payload, decision_id)
            decision = ReleaseDecision.model_validate(candidate)
        except Exception as exc:
            logger.warning(
                "Rejected connector decision %s (%s): %s",
                decision_id,
                type(payload).__name__,
                exc,
            )
            raise HTTPException(status_code=422, detail=f"Invalid release decision: {exc}") from exc
        if not await controller.register_external_decision(decision):
            raise HTTPException(
                status_code=409,
                detail="Decision is not for the current active canary",
            )

    try:
        result = await controller.apply_rollback(decision_id, via_connector=True)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Decision has not reached ReleaseGuard") from exc

    await gateway.publish_action(result.model_dump())
    return JSONResponse(status_code=200, content=result.model_dump())


def _connector_decision_payload(payload: Any, decision_id: str) -> dict[str, Any]:
    candidate = payload
    for _ in range(4):
        if isinstance(candidate, (bytes, bytearray)):
            candidate = candidate.decode("utf-8")
            continue
        if isinstance(candidate, str):
            candidate = json.loads(candidate)
            continue
        if isinstance(candidate, list) and len(candidate) == 1:
            candidate = candidate[0]
            continue
        if isinstance(candidate, dict) and "value" in candidate:
            candidate = candidate["value"]
            continue
        if isinstance(candidate, dict) and "payload" in candidate:
            candidate = candidate["payload"]
            continue
        break
    if not isinstance(candidate, dict):
        raise ValueError("Connector body must contain one decision object")

    normalized = dict(candidate)
    body_decision_id = normalized.get("decision_id")
    if body_decision_id is not None and body_decision_id != decision_id:
        raise ValueError("URL decision_id does not match the connector body")
    normalized.setdefault("decision_id", decision_id)
    decided_at = normalized.get("decided_at")
    if isinstance(decided_at, (int, float)):
        normalized["decided_at"] = (
            datetime.fromtimestamp(decided_at / 1000, UTC).isoformat().replace("+00:00", "Z")
        )
    return normalized


def _resolve_frontend_file(dist: Path, relative_path: str) -> Path | None:
    """Resolve a frontend file without allowing URL or symlink traversal."""
    decoded = relative_path
    for _ in range(12):
        next_value = unquote(decoded)
        if next_value == decoded:
            break
        decoded = next_value
    normalized = decoded.replace("\\", "/")
    if "\x00" in normalized or normalized.startswith("/") or ".." in normalized.split("/"):
        raise ValueError("Frontend path leaves the export directory")

    root = dist.resolve()
    candidate = (root / relative_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("Frontend path leaves the export directory") from exc
    return candidate if candidate.is_file() else None


def _mount_frontend(target_app: FastAPI, dist: Path) -> None:
    if not dist.exists():
        return
    assets = dist / "_next"
    if assets.exists():
        assets_root = assets.resolve()
        try:
            assets_root.relative_to(dist.resolve())
        except ValueError:
            pass
        else:
            if assets_root.is_dir():
                target_app.mount(
                    "/_next",
                    StaticFiles(directory=assets_root),
                    name="next-assets",
                )
    for static_name in ("favicon.svg", "favicon.ico"):
        try:
            file_path = _resolve_frontend_file(dist, static_name)
        except ValueError:
            continue
        if file_path is not None:
            target_app.add_api_route(
                f"/{static_name}",
                lambda path=file_path: FileResponse(path),
                include_in_schema=False,
            )

    @target_app.get("/{full_path:path}", include_in_schema=False)
    async def frontend(full_path: str) -> FileResponse:
        if full_path == "api" or full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        try:
            candidate = _resolve_frontend_file(dist, full_path)
            if candidate is not None:
                return FileResponse(candidate)
            nested_path = "index.html" if not full_path else f"{full_path.rstrip('/')}/index.html"
            nested_index = _resolve_frontend_file(dist, nested_path)
            if nested_index is not None:
                return FileResponse(nested_index)
            index = _resolve_frontend_file(dist, "index.html")
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="Not found") from exc
        if index is None:
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(index)


def create_app(
    app_settings: Settings | None = None,
    *,
    controller: DemoController | None = None,
    gateway: GatewayDependency | None = None,
    simulator: SimulatorDependency | None = None,
    serve_frontend: bool = True,
) -> FastAPI:
    """Build an isolated application with optional runtime dependencies."""
    resolved_settings = app_settings or Settings()
    resolved_controller = controller or DemoController(resolved_settings)
    resolved_gateway = gateway or KafkaGateway(resolved_settings, resolved_controller)
    resolved_simulator = simulator or TelemetrySimulator(
        resolved_settings,
        resolved_controller,
        resolved_gateway,
    )

    application = FastAPI(
        title="ReleaseGuard API",
        version="1.0.0",
        description="Real-time canary release safety demonstration.",
        lifespan=_lifespan,
    )
    application.state.settings = resolved_settings
    application.state.controller = resolved_controller
    application.state.gateway = resolved_gateway
    application.state.simulator = resolved_simulator
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Last-Event-ID"],
    )

    @application.middleware("http")
    async def limit_quick_tunnel_surface(request: Request, call_next):
        """Expose only the connector webhook and health probe through Quick Tunnels."""
        host = request.headers.get("host", "").split(":", 1)[0].lower()
        if host.endswith(".trycloudflare.com") and not (
            request.url.path == "/healthz"
            or request.url.path.startswith("/api/v1/release-decisions/")
        ):
            return JSONResponse(status_code=404, content={"detail": "Not found"})
        return await call_next(request)

    application.include_router(router)
    if serve_frontend:
        _mount_frontend(application, resolved_settings.frontend_dist)
    return application


# Uvicorn and the container keep using `releaseguard.app:app`.
settings = Settings()
controller = DemoController(settings)
gateway = KafkaGateway(settings, controller)
simulator = TelemetrySimulator(settings, controller, gateway)
app = create_app(
    settings,
    controller=controller,
    gateway=gateway,
    simulator=simulator,
)
