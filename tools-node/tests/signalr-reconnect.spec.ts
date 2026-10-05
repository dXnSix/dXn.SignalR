import { expect, test } from "@playwright/test";
import { openFixture, protocols } from "./fixture";

for (const { protocol, title } of protocols) {
  test(`ordinary reconnect ${title}`, async ({ page }) => {
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error(`[browser:pageerror] ${error.message}`));
    let activeSocket: import("@playwright/test").WebSocket | undefined;
    let negotiateRequests = 0;
    page.on("request", (request) => {
      if (request.url().includes("/test/negotiate")) negotiateRequests += 1;
    });
    await page.routeWebSocket(/\/test\?/, (socket) => {
      activeSocket = socket;
      socket.connectToServer();
    });

    await openFixture(page);
    const initial = await page.evaluate(
      (selected) => window.rayxSignalRFixture.connectAndProbe(selected),
      protocol,
    );
    expect(initial).toBe("initial-ok");
    expect(activeSocket, "Playwright must intercept the physical SignalR WebSocket").toBeDefined();
    const initialNegotiateRequests = negotiateRequests;

    await activeSocket!.close({ code: 1011, reason: "Playwright transport fault" });

    await expect.poll(
      () => negotiateRequests,
      { message: "fresh reconnect negotiation", timeout: 15_000 },
    ).toBeGreaterThan(initialNegotiateRequests);

    await expect.poll(async () => {
      try {
        return await page.evaluate(() => window.rayxSignalRFixture.probeAfterReconnect());
      } catch (error) {
        return `reconnect failed: ${String(error)}`;
      }
    }, { timeout: 15_000 }).toBe("reconnected");

    await page.evaluate(() => window.rayxSignalRFixture.triggerCallbackAfterReconnect());
    await expect.poll(
      () => page.evaluate(() => window.rayxSignalRFixture.callbackCount()),
      { message: "callback registration survives reconnect", timeout: 5_000 },
    ).toBe(1);
  });
}

for (const { protocol, title } of protocols) {
  test(`stateful reconnect ${title}`, async ({ page }) => {
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error(`[browser:pageerror] ${error.message}`));
    let activeSocket: import("@playwright/test").WebSocketRoute | undefined;
    let socketGeneration = 0;
    let negotiateRequests = 0;
    page.on("request", (request) => {
      if (request.url().includes("/test-stateful/negotiate")) negotiateRequests += 1;
    });
    page.on("websocket", (socket) => {
      if (!socket.url().includes("/test-stateful?")) return;
      activeSocket = socket;
      socketGeneration += 1;
    });

    await openFixture(page);
    const connectionId = await page.evaluate(
      (selected) => window.rayxSignalRFixture.connectStateful(selected),
      protocol,
    );
    expect(activeSocket, "Playwright must intercept the stateful SignalR WebSocket").toBeDefined();
    const initialNegotiateRequests = negotiateRequests;
    expect(initialNegotiateRequests).toBe(1);

    await page.evaluate(() => window.rayxSignalRFixture.startStatefulStream());
    for (const threshold of [4, 80]) {
      await expect.poll(
        () => page.evaluate(() => window.rayxSignalRFixture.statefulStreamCount()),
        { message: `stream reaches ${threshold} before transport drop`, timeout: 15_000 },
      ).toBeGreaterThanOrEqual(threshold);
      const previousGeneration = socketGeneration;
      await page.evaluate(() => window.rayxSignalRFixture.abortPhysicalTransport());
      await expect.poll(() => socketGeneration, { timeout: 15_000 }).toBeGreaterThan(previousGeneration);
    }

    await expect.poll(
      () => page.evaluate(() => window.rayxSignalRFixture.statefulStreamCount()),
      { message: "stream reaches third-drop boundary", timeout: 15_000 },
    ).toBeGreaterThanOrEqual(160);
    await page.evaluate(() => window.rayxSignalRFixture.startPendingInvocation());
    await expect.poll(
      () => page.evaluate(() => window.rayxSignalRFixture.pendingInvocationStarted()),
      { timeout: 5_000 },
    ).toBe(true);
    const previousGeneration = socketGeneration;
    await page.evaluate(() => window.rayxSignalRFixture.abortPhysicalTransport());
    await expect.poll(() => socketGeneration, { timeout: 15_000 }).toBeGreaterThan(previousGeneration);

    await expect.poll(
      () => page.evaluate(() => window.rayxSignalRFixture.statefulStreamDone()),
      { message: "stateful stream completes", timeout: 15_000 },
    ).toBe(true);
    await expect.poll(
      () => page.evaluate(() => window.rayxSignalRFixture.pendingInvocationResult()),
      { message: "pending invocation completes after resume", timeout: 15_000 },
    ).toBe(connectionId);
    expect(await page.evaluate(() => window.rayxSignalRFixture.scenarioError())).toBeUndefined();

    const streamValues = await page.evaluate(() =>
      Array.from(
        { length: window.rayxSignalRFixture.statefulStreamCount() },
        (_, index) => window.rayxSignalRFixture.statefulStreamValue(index),
      ),
    );
    expect(streamValues).toEqual(Array.from({ length: 240 }, (_, index) => index + 1));
    expect(await page.evaluate(() => window.rayxSignalRFixture.probeStatefulIdentity())).toBe(connectionId);

    await page.evaluate(() => window.rayxSignalRFixture.triggerStatefulCallbacks(12));
    await expect.poll(
      () => page.evaluate(() => window.rayxSignalRFixture.statefulCallbackCount()),
      { message: "ordered callbacks complete", timeout: 10_000 },
    ).toBe(12);
    const callbackValues = await page.evaluate(() =>
      Array.from(
        { length: window.rayxSignalRFixture.statefulCallbackCount() },
        (_, index) => window.rayxSignalRFixture.statefulCallbackValue(index),
      ),
    );
    expect(callbackValues).toEqual(Array.from({ length: 12 }, (_, index) => index + 1));
    expect(negotiateRequests, "stateful resume must not negotiate again").toBe(initialNegotiateRequests);
  });
}
