import { expect, test } from "@playwright/test";
import { openFixture, protocols, tlsServiceUrl } from "./fixture";

// The HTTPS endpoint uses a per-run test CA the browser does not trust; the context ignores
// certificate errors so the test proves the client's https negotiate and wss transport paths.
test.use({ ignoreHTTPSErrors: true });

for (const { protocol, title } of protocols) {
  test(`tls ${title}`, async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    const secureSockets: string[] = [];
    page.on("websocket", (socket) => secureSockets.push(socket.url()));
    await openFixture(page, "/", tlsServiceUrl());

    const outcome = await page.evaluate(
      (selected) =>
        window.rayxSignalRFixture
          .runFunctionalScenario("tls", selected)
          .catch((error: unknown) => `failed: ${String(error)}`),
      protocol,
    );

    expect(outcome).toBe("ok");
    expect(secureSockets.some((url) => url.startsWith("wss://"))).toBe(true);
    expect(pageErrors).toEqual([]);
  });
}
