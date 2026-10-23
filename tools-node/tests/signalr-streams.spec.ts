import { expect, test } from "@playwright/test";
import { countCancelInvocations, openFixture, protocols } from "./fixture";

for (const { protocol, title } of protocols) {
  test(`short stream chaining ${title}`, async ({ page }) => {
    const pageErrors: string[] = [];
    let cancellationFrames = 0;
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("websocket", (socket) => {
      socket.on("framesent", ({ payload }) => cancellationFrames += countCancelInvocations(payload));
    });
    await openFixture(page);
    await page.evaluate(
      (selected) => window.dxnSignalRFixture.connectAndProbe(selected),
      protocol,
    );

    const received = await page.evaluate(
      () => window.dxnSignalRFixture.consumeShortStreams(40, 3),
    );

    expect(received).toBe(120);
    expect(cancellationFrames, "naturally completed streams must not be canceled").toBe(0);
    expect(pageErrors).toEqual([]);
  });
}

for (const { protocol, title } of protocols) {
  test(`stream cancellation ${title}`, async ({ page }) => {
    const pageErrors: string[] = [];
    let cancellationFrames = 0;
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("websocket", (socket) => {
      socket.on("framesent", ({ payload }) => cancellationFrames += countCancelInvocations(payload));
    });

    await openFixture(page);
    await page.evaluate(
      (selected) => window.dxnSignalRFixture.connectAndProbe(selected),
      protocol,
    );

    const canceled = await page.evaluate(
      () => window.dxnSignalRFixture.cancelLongStreams(64),
    );

    expect(canceled).toBe(64);
    expect(cancellationFrames, "each dropped stream emits exactly one type-5 frame").toBe(64);
    expect(await page.evaluate(() => window.dxnSignalRFixture.probeAfterReconnect())).toBe("reconnected");
    expect(pageErrors).toEqual([]);
  });
}

// The scenario logic lives in the fixture (`tests/wasm-fixture/src/functional.rs`) and mirrors the
// native `tests/e2e/unsent_stream.rs`; only the live stream it drops may emit a type-5 frame.
for (const { protocol, title } of protocols) {
  test(`unsent stream ${title}`, async ({ page }) => {
    const pageErrors: string[] = [];
    let cancellationFrames = 0;
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    page.on("websocket", (socket) => {
      socket.on("framesent", ({ payload }) => cancellationFrames += countCancelInvocations(payload));
    });
    await openFixture(page);

    const outcome = await page.evaluate(
      (selected) =>
        window.dxnSignalRFixture
          .runFunctionalScenario("unsent-stream", selected)
          .catch((error: unknown) => `failed: ${String(error)}`),
      protocol,
    );

    expect(outcome).toBe("ok");
    expect(cancellationFrames, "streams whose invocation was never sent must not be canceled").toBe(1);
    expect(pageErrors).toEqual([]);
  });
}
