import { expect, test } from "@playwright/test";

declare global {
  interface Window {
    rayxSignalRFixture: {
      connectAndProbe(protocol: "json" | "messagepack"): Promise<string>;
      probeAfterReconnect(): Promise<string>;
      triggerCallbackAfterReconnect(): Promise<void>;
      callbackCount(): number;
    };
  }
}

for (const protocol of ["json", "messagepack"] as const) {
  test(`ordinary reconnect ${protocol === "json" ? "JSON" : "MessagePack"}`, async ({ page }) => {
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("pageerror", (error) => console.error(`[browser:pageerror] ${error.message}`));
    let activeSocket: import("@playwright/test").WebSocketRoute | undefined;
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

export {};
