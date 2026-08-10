import { createReadStream, statSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const toolsDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const root = path.resolve(toolsDir, "..", "artifacts-temp", "wasm-fixture");
const mime = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".wasm", "application/wasm"],
]);

http.createServer((request, response) => {
  const url = new URL(request.url ?? "/", "http://127.0.0.1:7878");
  const relative = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  const candidate = path.resolve(root, relative);
  if (!candidate.startsWith(`${root}${path.sep}`) && candidate !== path.join(root, "index.html")) {
    response.writeHead(403).end();
    return;
  }
  try {
    if (!statSync(candidate).isFile()) throw new Error("not a file");
    response.writeHead(200, { "Content-Type": mime.get(path.extname(candidate)) ?? "application/octet-stream" });
    createReadStream(candidate).pipe(response);
  } catch {
    response.writeHead(404).end();
  }
}).listen(7878, "127.0.0.1", () => {
  console.log("Serving RayX.SignalR WASM fixture at http://127.0.0.1:7878/");
});
