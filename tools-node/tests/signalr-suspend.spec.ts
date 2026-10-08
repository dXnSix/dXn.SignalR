import { expect, test } from "@playwright/test";
import { openFixture, protocols } from "./fixture";

// The suspend-resume scenario (`tests/wasm-fixture/src/functional.rs`, mirroring
// `tests/e2e/suspend_resume.rs`): for each readiness line the fixture logs, the spec stops the
// page's JavaScript through CDP `Debugger.pause` for the named duration, then resumes it: no
// timers or events run while the clocks advance, as in a frozen page. (Playwright keeps every
// page visible, and Chromium's `Page.setWebLifecycleState` frozen has no effect on a visible
// page.) Playwright offers CDP only for Chromium, so the Firefox and WebKit projects ignore this
// spec (the catalog's exclusions).
const readiness = /RAYX_SIGNALR_SUSPEND_READY seconds=(\d+)$/;

for (const { protocol, title } of protocols) {
  test(`suspend resume ${title}`, async ({ page }) => {
    test.setTimeout(115_000);
    const session = await page.context().newCDPSession(page);
    await session.send("Debugger.enable");
    const pageErrors: string[] = [];
    const freezes: Promise<void>[] = [];
    let previous = Promise.resolve();
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => {
      const text = message.text();
      console.log(`[browser:${message.type()}] ${text}`);
      const match = readiness.exec(text);
      if (match) {
        const seconds = Number(match[1]);
        previous = previous.then(async () => {
          await session.send("Debugger.pause");
          console.log(`[runner] froze the page for ${seconds} s`);
          await new Promise((resolve) => setTimeout(resolve, seconds * 1_000));
          await session.send("Debugger.resume");
          console.log(`[runner] the page runs again`);
        });
        freezes.push(previous);
      }
    });
    await openFixture(page);

    const outcome = await page.evaluate(
      ([selectedProtocol]) =>
        window.rayxSignalRFixture
          .runFunctionalScenario("suspend-resume", selectedProtocol)
          .catch((error: unknown) => `failed: ${String(error)}`),
      [protocol] as const,
    );
    await Promise.all(freezes);

    expect(outcome).toBe("ok");
    expect(freezes, "the page was frozen twice").toHaveLength(2);
    expect(pageErrors).toEqual([]);
  });
}
