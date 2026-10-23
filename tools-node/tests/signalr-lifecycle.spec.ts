import { expect, test } from "@playwright/test";
import { openFixture, protocols } from "./fixture";

// Browser runs of the lifecycle-control scenarios (pause, resume, status and disconnect); the scenario logic
// lives in the fixture (`tests/wasm-fixture/src/functional.rs`) and mirrors the native
// `tests/e2e` scenarios.
const scenarios = ["pause-resume", "pause-resume-stateful", "disconnect-every-handle"];

for (const scenario of scenarios) {
  for (const { protocol, title } of protocols) {
    test(`${scenario.replaceAll("-", " ")} ${title}`, async ({ page }) => {
      const pageErrors: string[] = [];
      page.on("pageerror", (error) => pageErrors.push(error.message));
      page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
      await openFixture(page);

      const outcome = await page.evaluate(
        ([selected, selectedProtocol]) =>
          window.dxnSignalRFixture
            .runFunctionalScenario(selected, selectedProtocol)
            .catch((error: unknown) => `failed: ${String(error)}`),
        [scenario, protocol] as const,
      );

      expect(outcome).toBe("ok");
      expect(pageErrors).toEqual([]);
    });
  }
}
