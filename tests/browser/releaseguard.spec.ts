import { expect, test } from "@playwright/test";

test("local mode updates through SSE and completes a rollback", async ({
  page,
  request,
}) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));

  await request.post("/api/demo/reset");
  await page.goto("/");

  await expect(page.getByText("Dashboard events · CONNECTED")).toBeVisible();
  await expect(
    page.getByText("Local twin · no cloud resources in use"),
  ).toBeVisible();
  await expect(page.getByText(/Confluent Cloud · LIVE/i)).toHaveCount(0);

  await page.getByRole("button", { name: "Launch canary" }).click();
  await expect(page.getByText("CANARY RUNNING", { exact: true })).toBeVisible();
  await expect(page.locator(".canary-lane b")).toHaveText("10% traffic");

  await page.reload();
  await expect(page.getByText("Dashboard events · CONNECTED")).toBeVisible();
  await expect(page.getByText("CANARY RUNNING", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Inject regression" }).click();
  await expect(page.getByText("LOCAL REGRESSION", { exact: true })).toBeVisible();
  await expect(page.getByText("Local rollback completed")).toBeVisible({
    timeout: 12_000,
  });
  await expect(page.locator(".canary-lane b")).toHaveText("0% traffic");
  await expect(page.getByText("estimated sessions spared · demo")).toBeVisible();

  expect(consoleErrors).toEqual([]);
});

test("cloud labels require cloud evidence and fit a narrow viewport", async ({
  page,
  request,
}) => {
  const stateResponse = await request.get("/api/demo/state");
  const snapshot = await stateResponse.json();
  snapshot.pipeline = {
    mode: "confluent",
    configured_mode: "confluent",
    health_source: "not_observed",
    flink_health_observed: false,
    flink_decision_observed: false,
    connector_delivery: false,
  };

  await page.route("**/api/events", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      headers: { "Cache-Control": "no-cache" },
      body: `event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n`,
    });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");

  await expect(page.getByText("Confluent cloud mode · configured")).toBeVisible();
  await expect(
    page.getByText(/Request traffic and impact figures are simulated\./),
  ).toBeVisible();
  await expect(page.getByText(/HTTP delivery has not been observed yet/)).toBeVisible();
  await expect(page.getByText("Local twin · no cloud resources in use")).toHaveCount(0);

  const hasHorizontalOverflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth,
  );
  expect(hasHorizontalOverflow).toBe(false);
});
