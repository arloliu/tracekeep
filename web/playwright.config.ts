import { defineConfig, devices } from "@playwright/test";

// The packaging tests' browser (architecture.md §3.3 *Packaging tests*), run by scripts/packaging/, never by make web-check.
// TRACEKEEP_URL is api.public_url; TRACEKEEP_HOST_RULES maps the tests' host names without touching /etc/hosts;
// TRACEKEEP_BROWSER_HOME is a home whose NSS database trusts the tests' CA, so no certificate error is ignored.
const home = process.env.TRACEKEEP_BROWSER_HOME;
const rules = process.env.TRACEKEEP_HOST_RULES;

export default defineConfig({
  testDir: "e2e",
  testMatch: "*.e2e.ts",
  timeout: 180_000,
  expect: { timeout: 30_000 },
  workers: 1,
  reporter: [["list"]],
  use: {
    ...devices["Desktop Chrome"],
    baseURL: process.env.TRACEKEEP_URL ?? "https://tracekeep.test:8443",
    trace: "retain-on-failure",
    launchOptions: {
      args: rules ? [`--host-resolver-rules=${rules}`] : [],
      env: home ? { ...process.env, HOME: home } : undefined,
    },
  },
});
