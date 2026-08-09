import { copyFileSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const toolsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rootDir = path.resolve(toolsDir, "..");
const outputDir = path.join(rootDir, "artifacts-temp", "wasm-fixture");
const targetDir = path.join(rootDir, "artifacts-temp", "wasm-fixture-target");
const wasmPath = path.join(
  targetDir,
  "wasm32-unknown-unknown",
  "debug",
  "rayx_signalr_wasm_fixture.wasm",
);

rmSync(outputDir, { recursive: true, force: true });
mkdirSync(outputDir, { recursive: true });

run("cargo", [
  "build",
  "--manifest-path",
  path.join(rootDir, "tests", "wasm-fixture", "Cargo.toml"),
  "--target",
  "wasm32-unknown-unknown",
  "--target-dir",
  targetDir,
]);
run("wasm-bindgen", [
  wasmPath,
  "--target",
  "web",
  "--out-dir",
  outputDir,
  "--out-name",
  "rayx_signalr_fixture",
]);
copyFileSync(path.join(toolsDir, "wasm-host", "index.html"), path.join(outputDir, "index.html"));

function run(executable, args) {
  const result = spawnSync(executable, args, { cwd: rootDir, stdio: "inherit" });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
