import { readFileSync } from "node:fs";
import { Agent } from "node:https";
import { join } from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import type { ProxyOptions } from "vite";
import { defineConfig } from "vitest/config";

// Frontend development against `tracekeep dev --public-url https://localhost:5173` (api.md §10.3 *Development*):
// TRACEKEEP_DEV_DATA names dev's --data directory, whose certificate this server serves and the proxy trusts,
// and TRACEKEEP_DEV_API the API's address, https://127.0.0.1:8080 by default (dev's --api, which localhost may not resolve to first).
const data = process.env.TRACEKEEP_DEV_DATA;
const cert = data ? readFileSync(join(data, "tls/cert.pem")) : undefined;
const key = data ? readFileSync(join(data, "tls/key.pem")) : undefined;
const target = process.env.TRACEKEEP_DEV_API ?? "https://127.0.0.1:8080";
const proxy: ProxyOptions = { target, secure: true, agent: cert ? new Agent({ ca: cert }) : undefined };

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": join(import.meta.dirname, "src") } },
  build: { outDir: "dist", emptyOutDir: true },
  server: {
    host: "localhost",
    port: 5173,
    strictPort: true,
    https: cert && key ? { cert, key } : undefined,
    proxy: { "/auth/": proxy, "/download/": proxy, "/tracekeep.v1.": proxy },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    restoreMocks: true,
  },
});
