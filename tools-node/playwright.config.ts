import { defineConfig, devices } from "@playwright/test";

process.env.PLAYWRIGHT_BROWSERS_PATH ??= "0";

export default defineConfig({
  testDir: "./tests",
  testMatch: ["signalr-wasm.spec.ts"],
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [["list"], ["html", { outputFolder: "playwright-report", open: "never" }]],
  use: {
    browserName: "chromium",
    trace: "retain-on-failure",
    video: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: [
    {
      command: "pnpm run build:wasm && pnpm run serve:wasm",
      url: "http://127.0.0.1:7878/",
      timeout: 120_000,
      reuseExistingServer: false,
    },
    {
      command: "dotnet run --no-launch-profile --project ../dotnet/SignalRTestService/SignalRTestService.csproj --urls http://127.0.0.1:5220",
      url: "http://127.0.0.1:5220/",
      timeout: 120_000,
      reuseExistingServer: true,
    },
  ],
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
