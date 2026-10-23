// Dedicated module Worker host for the browser fixture: runs one scenario without `window` and
// reports the outcome to the page with postMessage.
import init, { configure_service, run_functional_scenario } from "./dxn_signalr_fixture.js";

self.onmessage = async ({ data }) => {
  try {
    await init();
    configure_service(data.service);
    const outcome = await run_functional_scenario(data.scenario, data.protocol);
    self.postMessage({ outcome });
  } catch (error) {
    self.postMessage({ outcome: `failed: ${String(error)}` });
  }
};
