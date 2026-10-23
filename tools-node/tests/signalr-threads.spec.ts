import { expect, test } from "@playwright/test";
import { protocols, serviceUrl } from "./fixture";

/** The fixture host's cross-origin-isolated endpoint serving the threaded build. */
function isolatedUrl(): string {
  const url = process.env.DXN_SIGNALR_E2E_ISOLATED_URL;
  if (!url) throw new Error("DXN_SIGNALR_E2E_ISOLATED_URL is not set; run through the Aspire runner");
  return url;
}

for (const { protocol, title } of protocols) {
  test(`threaded wasm ${title}`, async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    await page.goto(`${isolatedUrl()}/?service=${encodeURIComponent(serviceUrl())}`);
    await expect(page.locator("body")).toHaveAttribute("data-dxn-signalr-wasm", "ready");
    expect(await page.evaluate(() => globalThis.crossOriginIsolated)).toBe(true);

    const outcome = await page.evaluate(
      (selected) =>
        window.dxnSignalRFixture
          .runFunctionalScenario("threaded-wasm", selected)
          .catch((error: unknown) => `failed: ${String(error)}`),
      protocol,
    );

    expect(outcome).toBe("ok");
    expect(pageErrors).toEqual([]);
  });
}
