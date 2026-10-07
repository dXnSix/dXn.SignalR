import { expect, test } from "@playwright/test";
import { openFixture, protocols } from "./fixture";

// Browser runs of the functional baseline; the scenario logic lives in the fixture
// (`tests/wasm-fixture/src/functional.rs`) and mirrors the native `tests/e2e` scenarios.
const scenarios = [
  "connect-negotiate",
  "authentication",
  "invoke",
  "send",
  "server-stream",
  "callbacks",
  "concurrency-and-clones",
  "large-payload",
  "server-close",
  "reconnect-exhaustion",
  "negotiate-redirect",
  "skip-negotiation",
  "message-burst",
  "error-kinds",
];

for (const scenario of scenarios) {
  for (const { protocol, title } of protocols) {
    test(`${scenario.replaceAll("-", " ")} ${title}`, async ({ page }) => {
      const pageErrors: string[] = [];
      page.on("pageerror", (error) => pageErrors.push(error.message));
      page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
      await openFixture(page);

      const outcome = await page.evaluate(
        ([selected, selectedProtocol]) =>
          window.rayxSignalRFixture
            .runFunctionalScenario(selected, selectedProtocol)
            .catch((error: unknown) => `failed: ${String(error)}`),
        [scenario, protocol] as const,
      );

      expect(outcome).toBe("ok");
      expect(pageErrors).toEqual([]);
    });
  }
}
