import { expect, test } from "@playwright/test";
import { protocols, serviceUrl } from "./fixture";

// The fixture runs inside a dedicated module Worker (no `window`); the page only relays the outcome.
for (const { protocol, title } of protocols) {
  test(`worker transport ${title}`, async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
    const query = new URLSearchParams({ service: serviceUrl(), scenario: "worker-transport", protocol });
    await page.goto(`/worker.html?${query}`);

    await expect(page.locator("body")).toHaveAttribute("data-rayx-signalr-worker", "done", { timeout: 45_000 });
    expect(await page.locator("body").getAttribute("data-rayx-signalr-worker-outcome")).toBe("ok");
    expect(pageErrors).toEqual([]);
  });
}
