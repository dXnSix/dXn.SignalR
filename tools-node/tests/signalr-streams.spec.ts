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
