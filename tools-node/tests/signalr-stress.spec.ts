import { expect, test } from "@playwright/test";
import { openFixture, protocols } from "./fixture";

// 500 server-side transport aborts across five stateful connections
// (`tests/wasm-fixture/src/stress.rs`, mirroring `tests/e2e/stateful_resume_stress.rs`); the cell
// bound is 600 seconds. A failure carries the tail of the stateful sequence trace.
for (const { protocol, title } of protocols) {
  test(`stateful resume stress ${title}`, async ({ page }) => {
    test.setTimeout(590_000);
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    await openFixture(page);

    const outcome = await page.evaluate(
      (selectedProtocol) =>
        window.dxnSignalRFixture
          .runStressScenario(selectedProtocol)
          .catch((error: unknown) => `failed: ${String(error)}`),
      protocol,
    );

    expect(outcome).toBe("ok");
    expect(pageErrors).toEqual([]);
  });
}
