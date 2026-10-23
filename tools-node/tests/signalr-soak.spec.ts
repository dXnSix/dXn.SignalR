import { expect, test } from "@playwright/test";
import { openFixture, protocols } from "./fixture";

// The soak under chaos (`tests/wasm-fixture/src/soak.rs`, mirroring `tests/e2e/soak_chaos.rs`):
// it runs for DXN_SIGNALR_SOAK_SECONDS (the Aspire runner forwards it; 120 by default) and its
// cell bound is that duration plus 60 seconds.
const soakSeconds = Number(process.env.DXN_SIGNALR_SOAK_SECONDS ?? "120");

for (const { protocol, title } of protocols) {
  test(`soak chaos ${title}`, async ({ page }) => {
    test.setTimeout((soakSeconds + 55) * 1_000);
    const pageErrors: string[] = [];
    const sockets: { isClosed(): boolean; url(): string }[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("websocket", (socket) => sockets.push(socket));
    await openFixture(page);

    const outcome = await page.evaluate(
      ([selectedProtocol, seconds]) =>
        window.dxnSignalRFixture
          .runSoakScenario(selectedProtocol, seconds)
          .catch((error: unknown) => `failed: ${String(error)}`),
      [protocol, soakSeconds] as const,
    );

    expect(outcome).toBe("ok");
    expect(sockets.length, "the soak opened WebSockets").toBeGreaterThan(0);
    // After `disconnect` no socket of either client stays open.
    await expect
      .poll(() => sockets.filter((socket) => !socket.isClosed()).map((socket) => socket.url()), {
        message: "every WebSocket is closed after disconnect",
        timeout: 15_000,
      })
      .toEqual([]);
    expect(pageErrors).toEqual([]);
  });
}
