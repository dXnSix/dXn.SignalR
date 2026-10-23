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
      (selected) => window.dxnSignalRFixture.connectAndProbe(selected),
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
        return await page.evaluate(() => window.dxnSignalRFixture.probeAfterReconnect());
      } catch (error) {
        return `reconnect failed: ${String(error)}`;
      }
    }, { timeout: 15_000 }).toBe("reconnected");

    await page.evaluate(() => window.dxnSignalRFixture.triggerCallbackAfterReconnect());
    await expect.poll(
      () => page.evaluate(() => window.dxnSignalRFixture.callbackCount()),
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
      (selected) => window.dxnSignalRFixture.connectStateful(selected),
      protocol,
    );
    expect(activeSocket, "Playwright must intercept the stateful SignalR WebSocket").toBeDefined();
    const initialNegotiateRequests = negotiateRequests;
    expect(initialNegotiateRequests).toBe(1);

    await page.evaluate(() => window.dxnSignalRFixture.startStatefulStream());
    for (const threshold of [4, 80]) {
      await expect.poll(
        () => page.evaluate(() => window.dxnSignalRFixture.statefulStreamCount()),
        { message: `stream reaches ${threshold} before transport drop`, timeout: 15_000 },
      ).toBeGreaterThanOrEqual(threshold);
      const previousGeneration = socketGeneration;
      await page.evaluate(() => window.dxnSignalRFixture.abortPhysicalTransport());
      await expect.poll(() => socketGeneration, { timeout: 15_000 }).toBeGreaterThan(previousGeneration);
    }

    await expect.poll(
      () => page.evaluate(() => window.dxnSignalRFixture.statefulStreamCount()),
      { message: "stream reaches third-drop boundary", timeout: 15_000 },
    ).toBeGreaterThanOrEqual(160);
    await page.evaluate(() => window.dxnSignalRFixture.startPendingInvocation());
    await expect.poll(
      () => page.evaluate(() => window.dxnSignalRFixture.pendingInvocationStarted()),
      { timeout: 5_000 },
    ).toBe(true);
    const previousGeneration = socketGeneration;
    await page.evaluate(() => window.dxnSignalRFixture.abortPhysicalTransport());
    await expect.poll(() => socketGeneration, { timeout: 15_000 }).toBeGreaterThan(previousGeneration);

    await expect.poll(
      () => page.evaluate(() => window.dxnSignalRFixture.statefulStreamDone()),
      { message: "stateful stream completes", timeout: 15_000 },
    ).toBe(true);
    await expect.poll(
      () => page.evaluate(() => window.dxnSignalRFixture.pendingInvocationResult()),
      { message: "pending invocation completes after resume", timeout: 15_000 },
    ).toBe(connectionId);
    expect(await page.evaluate(() => window.dxnSignalRFixture.scenarioError())).toBeUndefined();

    const streamValues = await page.evaluate(() =>
      Array.from(
        { length: window.dxnSignalRFixture.statefulStreamCount() },
        (_, index) => window.dxnSignalRFixture.statefulStreamValue(index),
      ),
    );
    expect(streamValues).toEqual(Array.from({ length: 240 }, (_, index) => index + 1));
    expect(await page.evaluate(() => window.dxnSignalRFixture.probeStatefulIdentity())).toBe(connectionId);

    await page.evaluate(() => window.dxnSignalRFixture.triggerStatefulCallbacks(12));
    await expect.poll(
      () => page.evaluate(() => window.dxnSignalRFixture.statefulCallbackCount()),
      { message: "ordered callbacks complete", timeout: 10_000 },
    ).toBe(12);
    const callbackValues = await page.evaluate(() =>
      Array.from(
        { length: window.dxnSignalRFixture.statefulCallbackCount() },
        (_, index) => window.dxnSignalRFixture.statefulCallbackValue(index),
      ),
    );
    expect(callbackValues).toEqual(Array.from({ length: 12 }, (_, index) => index + 1));
    expect(negotiateRequests, "stateful resume must not negotiate again").toBe(initialNegotiateRequests);
  });
}
