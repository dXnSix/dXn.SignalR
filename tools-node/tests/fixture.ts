import { expect, type Page } from "@playwright/test";

export type Protocol = "json" | "messagepack";

/** Every protocol with the title suffix the Aspire scenario catalog expects. */
export const protocols = [
  { protocol: "json", title: "JSON" },
  { protocol: "messagepack", title: "MessagePack" },
] as const;

/** The SignalR test service URL passed by the Aspire runner. */
export function serviceUrl(): string {
  const url = process.env.RAYX_SIGNALR_E2E_SERVICE_URL;
  if (!url) {
    throw new Error(
      "RAYX_SIGNALR_E2E_SERVICE_URL is not set; run browser scenarios through `dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E`",
    );
  }
  return url;
}

/** The SignalR test service's HTTPS URL passed by the Aspire runner. */
export function tlsServiceUrl(): string {
  const url = process.env.RAYX_SIGNALR_E2E_TLS_URL;
  if (!url) throw new Error("RAYX_SIGNALR_E2E_TLS_URL is not set; run through the Aspire runner");
  return url;
}

/** Every transport with the title the Aspire scenario catalog uses in transport-suffixed cells. */
export const transports = [
  { transport: "long-polling", title: "Long Polling" },
  { transport: "server-sent-events", title: "Server-Sent Events" },
] as const;

/**
 * The transport-baseline scenarios (`ScenarioCatalog.Baseline`): they run on every transport,
 * every other scenario on WebSockets only.
 */
export const transportBaseline = [
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
  "ordinary-reconnect",
  "keepalive-idle",
  "server-timeout",
  "pause-resume",
  "token-factory",
] as const;

/**
 * Opens a fixture host page configured for the test service (and a transport other than
 * WebSockets) and waits for the WASM module.
 */
export async function openFixture(
  page: Page,
  path = "/",
  service = serviceUrl(),
  transport = "websockets",
): Promise<void> {
  await page.goto(
    `${path}?service=${encodeURIComponent(service)}&transport=${encodeURIComponent(transport)}`,
  );
  await expect(page.locator("body")).toHaveAttribute("data-rayx-signalr-wasm", "ready");
}

/** Counts SignalR CancelInvocation (type 5) messages in a sent WebSocket frame. */
export function countCancelInvocations(payload: string | Buffer) {
  if (typeof payload === "string") {
    return payload
      .split("\u001e")
      .filter(Boolean)
      .reduce((count, message) => {
        try {
          return count + (JSON.parse(message).type === 5 ? 1 : 0);
        } catch {
          return count;
        }
      }, 0);
  }

  let count = 0;
  let offset = 0;
  while (offset < payload.length) {
    let length = 0;
    let shift = 0;
    let byte = 0;
    do {
      byte = payload[offset++];
      length |= (byte & 0x7f) << shift;
      shift += 7;
    } while ((byte & 0x80) !== 0 && offset < payload.length);
    const messageOffset = offset;
    if (length >= 2 && payload[messageOffset] === 0x93 && payload[messageOffset + 1] === 0x05) {
      count += 1;
    }
    offset += length;
  }
  return count;
}

declare global {
  interface Window {
    rayxSignalRFixture: {
      abortPhysicalTransport(): Promise<void>;
      cancelLongStreams(rounds: number): Promise<number>;
      connectAndProbe(protocol: "json" | "messagepack"): Promise<string>;
      connectStateful(protocol: "json" | "messagepack"): Promise<string>;
      consumeShortStreams(rounds: number, itemsPerRound: number): Promise<number>;
      pendingInvocationResult(): string | undefined;
      pendingInvocationStarted(): boolean;
      probeAfterReconnect(): Promise<string>;
      probeStatefulIdentity(): Promise<string>;
      scenarioError(): string | undefined;
      startPendingInvocation(): void;
      startStatefulStream(): void;
      statefulCallbackCount(): number;
      statefulCallbackValue(index: number): number;
      statefulStreamCount(): number;
      statefulStreamDone(): boolean;
      statefulStreamValue(index: number): number;
      triggerCallbackAfterReconnect(): Promise<void>;
      triggerStatefulCallbacks(count: number): Promise<void>;
      callbackCount(): number;
      configureService(url: string): void;
      runFunctionalScenario(scenario: string, protocol: "json" | "messagepack"): Promise<string>;
      runSoakScenario(protocol: "json" | "messagepack", seconds: number): Promise<string>;
      runStressScenario(protocol: "json" | "messagepack"): Promise<string>;
    };
  }
}

