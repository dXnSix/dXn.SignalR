import { defineConfig, devices } from "@playwright/test";

process.env.PLAYWRIGHT_BROWSERS_PATH ??= "0";

// Run only through the Aspire runner (`dotnet test dotnet/RayX.SignalR.E2E.Tests`), which starts
// `fixture-host` and `signalr-test-service` and passes their dynamically allocated URLs.
export default defineConfig({
  testDir: "./tests",
  testMatch: ["signalr-*.spec.ts"],
  timeout: 55_000,
  expect: { timeout: 15_000 },
  outputDir: "../artifacts-temp/e2e/playwright-results",
  reporter: [["list"]],
  use: {
    baseURL: process.env.RAYX_SIGNALR_E2E_FIXTURE_URL,
    trace: "retain-on-failure",
    video: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
    {
      name: "firefox",
      use: { ...devices["Desktop Firefox"] },
      // The soak and the stress run in one engine, and the suspension needs CDP (see the
      // catalog's exclusions).
      testIgnore: [
        "signalr-soak.spec.ts",
        "signalr-stress.spec.ts",
        "signalr-suspend.spec.ts",
      ],
    },
    {
      name: "webkit",
      use: { ...devices["Desktop Safari"] },
      testIgnore: [
        "signalr-soak.spec.ts",
        "signalr-stress.spec.ts",
        "signalr-suspend.spec.ts",
      ],
    },
  ],
});
