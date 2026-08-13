import { expect, test } from "@playwright/test";

declare global {
  interface Window {
    rayxSignalRFixture: {
      abortPhysicalTransport(): Promise<void>;
      cancelLongStreams(rounds: number): Promise<number>;
      connectAndProbe(protocol: "json" | "messagepack"): Promise<string>;
      connectStateful(protocol: "json" | "messagepack"): Promise<string>;
      consumeShortStreams(rounds: number, itemsPerRound: number): Promise<number>;
      pendingInvocationResult(): string | undefined;
      pendingInvocationStarted(): boolean;
      probeAfterReconnect(): Promise<string>;
      probeStatefulIdentity(): Promise<string>;
      scenarioError(): string | undefined;
      startPendingInvocation(): void;
      startStatefulStream(): void;
      statefulCallbackCount(): number;
      statefulCallbackValue(index: number): number;
      statefulStreamCount(): number;
      statefulStreamDone(): boolean;
      statefulStreamValue(index: number): number;
      triggerCallbackAfterReconnect(): Promise<void>;
      triggerStatefulCallbacks(count: number): Promise<void>;
      callbackCount(): number;
    };
  }
}

for (const protocol of ["json", "messagepack"] as const) {
  test(`short stream chaining ${protocol === "json" ? "JSON" : "MessagePack"}`, async ({ page }) => {
    const pageErrors: string[] = [];
    let cancellationFrames = 0;
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("websocket", (socket) => {
      socket.on("framesent", ({ payload }) => cancellationFrames += countCancelInvocations(payload));
    });
    await page.goto("http://127.0.0.1:7878/");
    await expect(page.locator("body")).toHaveAttribute("data-rayx-signalr-wasm", "ready");
    await page.evaluate(
      (selected) => window.rayxSignalRFixture.connectAndProbe(selected),
      protocol,
    );

    const received = await page.evaluate(
      () => window.rayxSignalRFixture.consumeShortStreams(40, 3),
    );

    expect(received).toBe(120);
    expect(cancellationFrames, "naturally completed streams must not be canceled").toBe(0);
    expect(pageErrors).toEqual([]);
  });
}

for (const protocol of ["json", "messagepack"] as const) {
  test(`stream cancellation ${protocol === "json" ? "JSON" : "MessagePack"}`, async ({ page }) => {
    const pageErrors: string[] = [];
    let cancellationFrames = 0;
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("websocket", (socket) => {
      socket.on("framesent", ({ payload }) => cancellationFrames += countCancelInvocations(payload));
    });

    await page.goto("http://127.0.0.1:7878/");
    await expect(page.locator("body")).toHaveAttribute("data-rayx-signalr-wasm", "ready");
    await page.evaluate(
      (selected) => window.rayxSignalRFixture.connectAndProbe(selected),
      protocol,
    );

    const canceled = await page.evaluate(
      () => window.rayxSignalRFixture.cancelLongStreams(64),
    );

    expect(canceled).toBe(64);
    expect(cancellationFrames, "each dropped stream emits exactly one type-5 frame").toBe(64);
    expect(await page.evaluate(() => window.rayxSignalRFixture.probeAfterReconnect())).toBe("reconnected");
    expect(pageErrors).toEqual([]);
  });
}

for (const protocol of ["json", "messagepack"] as const) {
  test(`ordinary reconnect ${protocol === "json" ? "JSON" : "MessagePack"}`, async ({ page }) => {
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

    await page.goto("http://127.0.0.1:7878/");
    await expect(page.locator("body")).toHaveAttribute("data-rayx-signalr-wasm", "ready");
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

for (const protocol of ["json", "messagepack"] as const) {
  test(`stateful reconnect ${protocol === "json" ? "JSON" : "MessagePack"}`, async ({ page }) => {
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

    await page.goto("http://127.0.0.1:7878/");
    await expect(page.locator("body")).toHaveAttribute("data-rayx-signalr-wasm", "ready");
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

function countCancelInvocations(payload: string | Buffer) {
  if (typeof payload === "string") {
    return payload
      .split("\u001e")
      .filter(Boolean)
      .reduce((count, message) => {
        try {
          return count + (JSON.parse(message).type === 5 ? 1 : 0);
        } catch {
          return count;
        }
      }, 0);
  }

  let count = 0;
  let offset = 0;
  while (offset < payload.length) {
    let length = 0;
    let shift = 0;
    let byte = 0;
    do {
      byte = payload[offset++];
      length |= (byte & 0x7f) << shift;
      shift += 7;
    } while ((byte & 0x80) !== 0 && offset < payload.length);
    const messageOffset = offset;
    if (length >= 2 && payload[messageOffset] === 0x93 && payload[messageOffset + 1] === 0x05) {
      count += 1;
    }
    offset += length;
  }
  return count;
}

export {};
