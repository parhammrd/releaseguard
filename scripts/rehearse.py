from __future__ import annotations

import asyncio
import time

from releaseguard.config import Settings
from releaseguard.kafka_gateway import KafkaGateway
from releaseguard.simulator import TelemetrySimulator
from releaseguard.state import DemoController


async def one_run(index: int) -> float:
    settings = Settings()
    controller = DemoController(settings)
    gateway = KafkaGateway(settings, controller)
    simulator = TelemetrySimulator(settings, controller, gateway)
    task = asyncio.create_task(simulator.run())
    try:
        await controller.launch_canary("v2.4.0", 10)
        await asyncio.sleep(2.2)
        assert controller.phase.value == "CANARY_RUNNING", "false rollback during healthy canary"
        injected = time.monotonic()
        await controller.set_regression(True)
        while controller.phase.value != "ROLLED_BACK":
            if time.monotonic() - injected > 12:
                raise TimeoutError("rollback exceeded 12 seconds")
            await asyncio.sleep(0.05)
        duration = time.monotonic() - injected
        print(f"run {index}: rollback in {duration:.2f}s")
        return duration
    finally:
        simulator.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def main() -> None:
    durations = [await one_run(index) for index in range(1, 6)]
    print(f"five-run max: {max(durations):.2f}s; false rollbacks: 0")


if __name__ == "__main__":
    asyncio.run(main())
