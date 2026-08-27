import { defineConfig, devices } from "@playwright/test";

const localEnvironment = [
  "LOCAL_DECISIONS=true",
  "KAFKA_BOOTSTRAP_SERVERS=",
  "KAFKA_API_KEY=",
  "KAFKA_API_SECRET=",
  "SCHEMA_REGISTRY_URL=",
  "SCHEMA_REGISTRY_API_KEY=",
  "SCHEMA_REGISTRY_API_SECRET=",
  "WEBHOOK_SECRET=releaseguard-browser-test-secret",
  "EVENTS_PER_SECOND=500",
  "SIMULATOR_INTERVAL_SECONDS=0.02",
  "HEALTH_WINDOW_SECONDS=1",
  "HEALTH_EMIT_SECONDS=1",
].join(" ");

export default defineConfig({
  testDir: "./tests/browser",
  fullyParallel: false,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "line",
  use: {
    baseURL: "http://127.0.0.1:8010",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  webServer: {
    command: `${localEnvironment} PYTHONPATH=backend .venv/bin/python -m uvicorn releaseguard.app:app --host 127.0.0.1 --port 8010`,
    url: "http://127.0.0.1:8010/healthz",
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
