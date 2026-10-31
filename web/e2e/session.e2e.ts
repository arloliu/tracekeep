import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

// A troubleshooting session end to end against the packaged service (the gate's case, architecture.md §3.3 *Packaging tests*):
// sign in at Dex, browse the simulator's tool, read its records and a transaction, export them and read the bundle's manifest.

const tool = process.env.TRACEKEEP_TOOL ?? "SIM-TOOL";
const device = process.env.TRACEKEEP_DEVICE ?? "eqp";

// signIn signs the mock connector's person in: Dex lists its connectors, and the mock one signs in with no form.
async function signIn(page: Page) {
  await page.goto("/");
  await page.getByRole("link", { name: "Sign in" }).click();
  await page.getByText("Log in with Example").click();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
}

// tarFile is the content of the file named name in a tar archive.
function tarFile(tar: Buffer, name: string): Buffer | undefined {
  for (let at = 0; at + 512 <= tar.length; ) {
    const header = tar.subarray(at, at + 512);
    const entry = header.subarray(0, 100).toString("utf8").replace(/\0.*$/s, "");
    if (entry === "") return undefined;
    const size = parseInt(header.subarray(124, 136).toString("utf8").replace(/\0.*$/s, "").trim() || "0", 8);
    if (entry === name || entry.endsWith(`/${name}`)) return tar.subarray(at + 512, at + 512 + size);
    at += 512 + Math.ceil(size / 512) * 512;
  }
  return undefined;
}

test("a person browses a tool, reads its records and a transaction, and exports them", async ({ page }) => {
  await signIn(page);

  await page.getByLabel("Tool id").fill(tool);
  await page.getByRole("button", { name: "Open" }).click();
  await page.getByRole("checkbox", { name: `Select ${device}` }).check();
  const records = page.getByRole("link", { name: /^Records of / }).first();
  await expect(records).toBeVisible();
  await expect(page.getByRole("table", { name: "Hours" })).toBeVisible();

  await records.click();
  await expect(page.getByText(/^\d+ records? in the list;/)).toBeVisible({ timeout: 60_000 });
  // A primary of the simulator's (S1F3 W), whose transaction the lookup matches with its reply.
  await page.getByRole("button", { name: /^Seq \d+$/ }).filter({ hasText: "S1F3 W" }).first().click();
  const panel = page.getByRole("complementary", { name: "Selected" });
  await expect(panel.getByText("matched")).toBeVisible();

  await page.getByRole("link", { name: "Export this selection…" }).click();
  await page.getByRole("button", { name: "Start export" }).click();
  const download = page.getByRole("button", { name: "Download" });
  await expect(download).toBeVisible({ timeout: 120_000 });
  const [bundle] = await Promise.all([page.waitForEvent("download"), download.click()]);
  const path = await bundle.path();
  const manifest = tarFile(readFileSync(path), "manifest.json");
  expect(manifest, "the bundle holds manifest.json").toBeDefined();
  const m = JSON.parse(manifest?.toString("utf8") ?? "{}") as { kind?: string; captures?: unknown[] };
  expect(m.kind).toBe("canonical");
  expect(m.captures?.length ?? 0).toBeGreaterThan(0);
});

test("a person no group maps is signed in and admitted to nothing", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Sign in" }).click();
  await page.getByText("Log in with Email").click();
  await page.getByRole("textbox", { name: /email/i }).fill(process.env.TRACEKEEP_UNMAPPED_EMAIL ?? "nobody@example.test");
  await page.getByRole("textbox", { name: /password/i }).fill(process.env.TRACEKEEP_UNMAPPED_PASSWORD ?? "password");
  await page.getByRole("button", { name: "Login" }).click();
  await expect(page.getByText("Your roles do not admit this page; it needs viewer.")).toBeVisible();
});

// A login one api replica starts, the other finishes (the cluster's leg), the first restarted before the callback.
// The browser stays on the public URL, as a person's does; the leg's commands make the Service send it to the first replica for the login
// (TRACEKEEP_BEFORE_LOGIN) and, with the request held, restart that replica and send the callback to the second (TRACEKEEP_BEFORE_CALLBACK).
test("a login started on one api replica is finished by another", async ({ page, baseURL }) => {
  const beforeLogin = process.env.TRACEKEEP_BEFORE_LOGIN;
  const beforeCallback = process.env.TRACEKEEP_BEFORE_CALLBACK;
  test.skip(!beforeLogin || !beforeCallback, "needs a cluster's two api replicas");
  const pub = new URL(baseURL ?? "https://tracekeep.test:8443");
  // The callback request is held, unchanged, while the replicas are switched.
  const cdp = await page.context().newCDPSession(page);
  cdp.on("Fetch.requestPaused", (e) => {
    execSync(beforeCallback ?? "", { stdio: "inherit" });
    void cdp.send("Fetch.continueRequest", { requestId: e.requestId });
  });
  await cdp.send("Fetch.enable", { patterns: [{ urlPattern: `${pub.origin}/auth/callback*`, requestStage: "Request" }] });

  execSync(beforeLogin ?? "", { stdio: "inherit" });
  await page.goto(`${pub.origin}/auth/login?return=%2F`);
  await page.getByText("Log in with Example").click();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
});
