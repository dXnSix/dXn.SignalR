import { expect, test } from "@playwright/test";
import { openFixture, protocols, transportBaseline, transports } from "./fixture";

// The transport baseline on the transports other than WebSockets: the fixture's functional
// scenarios with every connection restricted to the cell's transport (the page's `transport`
// parameter). Server-Sent Events carry text only, so they never run with MessagePack.
const cells = transports.flatMap(({ transport, title: transportTitle }) =>
  transportBaseline.flatMap((scenario) =>
    protocols
      .filter(({ protocol }) => !(transport === "server-sent-events" && protocol === "messagepack"))
      .map(({ protocol, title }) => ({ scenario, protocol, transport, title, transportTitle })),
  ),
);

for (const { scenario, protocol, transport, title, transportTitle } of cells) {
  test(`${scenario.replaceAll("-", " ")} (${title}, ${transportTitle})`, async ({ page }) => {
    // Over Long Polling the liveness scenarios idle for 45 s (the catalog bounds them at 120 s).
    if (transport === "long-polling" && ["keepalive-idle", "server-timeout"].includes(scenario)) {
      test.setTimeout(110_000);
    }
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    await openFixture(page, "/", undefined, transport);

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
