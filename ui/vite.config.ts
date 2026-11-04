/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During UI work: `npm run dev` against a running `moksha dev`
// (MOKSHA_SERVER, default http://127.0.0.1:7070); API calls are proxied.
const server = process.env.MOKSHA_SERVER ?? "http://127.0.0.1:7070";

export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/moksha.v1.": { target: server, changeOrigin: true } } },
  build: { outDir: "dist", emptyOutDir: true },
  test: { environment: "jsdom", globals: true, setupFiles: ["src/test-setup.ts"] },
});
