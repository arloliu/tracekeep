import { execSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";

// A troubleshooting session end to end against the packaged service (the gate's case, architecture.md §3.3 *Packaging tests*):
// sign in at Dex, browse the simulator's tool, read its records and a transaction, export them and read the bundle's manifest.

const tool = process.env.TRACEKEEP_TOOL ?? "SIM-TOOL";
const device = process.env.TRACEKEEP_DEVICE ?? "eqp";
// The simulator sends this as a process program's body (S7F3), which the baseline policy masks: no page or bundle may show it.
const processProgram = process.env.TRACEKEEP_PROCESS_PROGRAM ?? "SIM-PROCESS-PROGRAM-BODY";

// signIn signs the mock connector's person in: Dex lists its connectors, and the mock one signs in with no form.
async function signIn(page: Page) {
  await page.goto("/");
  await page.getByRole("link", { name: "Sign in" }).click();
  await page.getByText("Log in with Example").click();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
}

// tarFiles are a tar archive's regular files by name: a long name is a ustar prefix and name, or a PAX path record.
function tarFiles(tar: Buffer): Map<string, Buffer> {
  const files = new Map<string, Buffer>();
  const field = (h: Buffer, from: number, len: number) => h.subarray(from, from + len).toString("utf8").replace(/\0.*$/s, "");
  let paxPath: string | undefined;
  for (let at = 0; at + 512 <= tar.length; ) {
    const header = tar.subarray(at, at + 512);
    const name = field(header, 0, 100);
    if (name === "") break;
    const size = parseInt(field(header, 124, 12).trim() || "0", 8);
    const body = tar.subarray(at + 512, at + 512 + size);
    const type = field(header, 156, 1) || "0";
    const prefix = field(header, 257, 6).startsWith("ustar") ? field(header, 345, 155) : "";
    if (type === "x") {
      paxPath = /(?:^|\n)\d+ path=([^\n]*)\n/.exec(body.toString("utf8"))?.[1];
    } else {
      if (type === "0") files.set(paxPath ?? (prefix === "" ? name : `${prefix}/${name}`), body);
      paxPath = undefined;
    }
    at += 512 + Math.ceil(size / 512) * 512;
  }
  return files;
}

// holds tells whether s holds the process program, as it is or in a base64 string, as a record's payload is.
function holds(s: string): boolean {
  if (s.includes(processProgram)) return true;
  return /^[A-Za-z0-9+/]+={0,2}$/.test(s) && s.length % 4 === 0 && Buffer.from(s, "base64").includes(processProgram);
}

// shows tells whether a page's text shows the process program, as it is or in any base64 word of it.
function shows(text: string): boolean {
  return holds(text) || text.split(/[^A-Za-z0-9+/=]+/).some(holds);
}

// strings are every string a JSON value holds, its keys included.
function strings(v: unknown): string[] {
  if (typeof v === "string") return [v];
  if (Array.isArray(v)) return v.flatMap(strings);
  if (v !== null && typeof v === "object") return Object.entries(v).flatMap(([k, x]) => [k, ...strings(x)]);
  return [];
}

// isS7F3 tells whether a JSONL record's base64 payload, an HSMS frame, is an S7F3.
function isS7F3(payload: unknown): boolean {
  if (typeof payload !== "string") return false;
  const frame = Buffer.from(payload, "base64");
  return frame.length >= 14 && (frame[6] & 0x7f) === 7 && frame[7] === 3;
}

interface Manifest {
  kind?: string;
  selector?: { tool_id?: string; devices?: string[]; capture_id?: string; from?: number; to?: number };
  captures?: { capture?: { device?: string }; hours?: { hour?: string; packs?: { file?: string }[]; left_out?: unknown[] }[] }[];
}

// evidence is where a run before an upgrade keeps what it read (TRACEKEEP_EVIDENCE), for the run after it to read again.
const evidence = process.env.TRACEKEEP_EVIDENCE;
interface Evidence {
  url: string;
  end: string;
  first: string;
}

// openRecords opens the tool's device, sees an hour of its capture with its scope's state and packs, and opens the capture's records,
// returning the list's end once it is read.
async function openRecords(page: Page) {
  await page.getByLabel("Tool id").fill(tool);
  await page.getByRole("button", { name: "Open" }).click();
  await page.getByRole("checkbox", { name: `Select ${device}` }).check();
  const records = page.getByRole("link", { name: /^Records of / }).first();
  await expect(records).toBeVisible();
  await expect(page.getByRole("table", { name: "Hours" }).getByRole("row").filter({ hasText: /\b(open|settled|unmergeable) \d+ packs?\b/ }).first()).toBeVisible();
  await records.click();
  const end = page.getByText(/^\d+ records? in the list;/);
  await expect(end).toBeVisible({ timeout: 60_000 });
  return end;
}

// expectMatched opens a primary of the simulator's (S1F3 W) and sees its SML and its transaction matched:
// the primary, and its reply a valid candidate.
async function expectMatched(page: Page) {
  await page.getByRole("button", { name: /^Seq \d+$/ }).filter({ hasText: "S1F3 W" }).first().click();
  const panel = page.getByRole("complementary", { name: "Selected" });
  await expect(panel.locator("pre")).not.toBeEmpty();
  await expect(panel.getByText("matched")).toBeVisible().catch(async (e: unknown) => {
    throw new Error(`the transaction is not matched; the panel reads:\n${await panel.innerText()}`, { cause: e });
  });
  const tx = panel.getByRole("list", { name: "Transaction records" }).getByRole("listitem");
  await expect(tx.filter({ hasText: /— primary$/ })).toHaveCount(1);
  await expect(tx.filter({ hasText: /candidate.*✓/ }).first()).toBeVisible();
  return panel;
}

// While the leg's simulator still sends (TRACEKEEP_LIVE), a primary's transaction is looked up and matched, as on a running tool.
test("a transaction is matched while the simulator sends", async ({ page }) => {
  test.skip(!process.env.TRACEKEEP_LIVE, "runs while the leg's simulator sends");
  await signIn(page);
  await openRecords(page);
  await expectMatched(page);
});

test("a person browses a tool, reads its records and a transaction, and exports them", async ({ page }) => {
  // The checks below can fail: the process program is found as it is and inside a base64 word.
  expect(shows(`SML ${Buffer.from(`ab${processProgram}`).toString("base64")}.`) && shows(processProgram)).toBe(true);
  await signIn(page);
  const end = await openRecords(page);
  await expect(end).toHaveText(/; complete\.$/);
  const listed = Number(/^(\d+)/.exec(await end.innerText())?.[1] ?? "0");
  const selected = new URL(page.url()).searchParams;
  // The evidence is written before an upgrade only: a run after one never writes what it is to be compared with.
  if (evidence && !process.env.TRACEKEEP_AFTER_UPGRADE && !existsSync(evidence)) {
    const first = (await page.getByRole("button", { name: /^Seq \d+$/ }).first().getAttribute("aria-label")) ?? "";
    writeFileSync(evidence, JSON.stringify({ url: page.url(), end: await end.innerText(), first } satisfies Evidence));
  }
  const panel = await expectMatched(page);

  // The process program the simulator sent, masked: its record renders, and its page never shows the program.
  const pp = page.getByRole("button", { name: /^Seq \d+$/ }).filter({ hasText: "S7F3 W" }).first();
  const ppSeq = (await pp.getAttribute("aria-label")) ?? "";
  await pp.click();
  await expect(panel.getByRole("heading", { name: ppSeq, exact: true })).toBeVisible();
  await expect(panel.locator("pre")).not.toBeEmpty();
  expect(shows(await panel.innerText()), "the masked record's page shows the process program").toBe(false);

  await page.getByRole("link", { name: "Export this selection…" }).click();
  await page.getByRole("button", { name: "Start export" }).click();
  const download = page.getByRole("button", { name: "Download" });
  await expect(download).toBeVisible({ timeout: 120_000 });
  const [bundle] = await Promise.all([page.waitForEvent("download"), download.click()]);
  expect(new URL(bundle.url()).pathname).toMatch(/^\/download\//);
  const files = tarFiles(readFileSync(await bundle.path()));
  const manifest = [...files].find(([name]) => name.endsWith("manifest.json"))?.[1];
  expect(manifest, "the bundle holds manifest.json").toBeDefined();
  const m = JSON.parse(manifest?.toString("utf8") ?? "{}") as Manifest;
  expect(m.kind).toBe("canonical");
  // The selection exported: the tool, the device and the records' window, to the millisecond (the manifest's nanoseconds parse as a double).
  expect(m.selector?.tool_id).toBe(tool);
  expect(m.selector?.devices).toContain(device);
  expect(Math.abs((m.selector?.from ?? 0) / 1e6 - Date.parse(selected.get("from") ?? ""))).toBeLessThan(1);
  expect(Math.abs((m.selector?.to ?? 0) / 1e6 - Date.parse(selected.get("to") ?? ""))).toBeLessThan(1);
  expect(m.captures?.length ?? 0).toBeGreaterThan(0);
  // Every capture is the device's, every hour has its packs, each pack's file is in the bundle, and none was left out.
  const hours = m.captures?.flatMap((c) => c.hours ?? []) ?? [];
  expect(m.captures?.map((c) => c.capture?.device)).toEqual(m.captures?.map(() => device));
  expect(hours.length).toBeGreaterThan(0);
  for (const h of hours) {
    expect(h.packs?.length ?? 0, `hour ${h.hour ?? "?"} has its packs`).toBeGreaterThan(0);
    for (const p of h.packs ?? []) expect(files.has(p.file ?? ""), `the bundle holds ${p.file ?? "?"}`).toBe(true);
  }
  expect(hours.flatMap((h) => h.left_out ?? [])).toEqual([]);
  // The records are in the bundle, every one the list read and the process program's among them,
  // and nothing in a JSONL line or the manifest holds the program, as it is or in base64.
  expect(strings(m).some(holds), "manifest.json holds the process program").toBe(false);
  let records = 0;
  let programs = 0;
  for (const [name, body] of files) {
    if (!name.endsWith(".jsonl")) continue;
    for (const line of body.toString("utf8").split("\n")) {
      if (line === "") continue;
      const v = JSON.parse(line) as { seq?: string; payload?: unknown };
      if (v.seq !== undefined) {
        records++;
        if (isS7F3(v.payload)) programs++;
      }
      expect(strings(v).some(holds), `${name} holds the process program`).toBe(false);
    }
  }
  expect(records, "the bundle's records, at least those the list read").toBeGreaterThanOrEqual(listed);
  expect(listed).toBeGreaterThan(0);
  expect(programs, "the bundle's process programs").toBeGreaterThan(0);
});

// The records read before an upgrade (TRACEKEEP_EVIDENCE, written by the run before it) are read the same after it.
// After an upgrade (TRACEKEEP_AFTER_UPGRADE) the evidence must be there: a run without it fails, never passing for an upgrade it did not check.
test("the records read before an upgrade are read the same after it", async ({ page }) => {
  test.skip(!process.env.TRACEKEEP_AFTER_UPGRADE, "runs after an upgrade");
  expect(evidence !== undefined && existsSync(evidence), `the evidence of the run before the upgrade (${evidence ?? "TRACEKEEP_EVIDENCE unset"})`).toBe(true);
  const before = JSON.parse(readFileSync(evidence ?? "", "utf8")) as Evidence;
  await signIn(page);
  await page.goto(before.url);
  const end = page.getByText(/^\d+ records? in the list;/);
  await expect(end).toBeVisible({ timeout: 60_000 });
  expect(await end.innerText()).toBe(before.end);
  expect(await page.getByRole("button", { name: /^Seq \d+$/ }).first().getAttribute("aria-label")).toBe(before.first);
});

test("a person no group maps is signed in and admitted to nothing", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Sign in" }).click();
  await page.getByText("Log in with Email").click();
  await page.getByRole("textbox", { name: /email/i }).fill(process.env.TRACEKEEP_UNMAPPED_EMAIL ?? "nobody@example.test");
  await page.getByRole("textbox", { name: /password/i }).fill(process.env.TRACEKEEP_UNMAPPED_PASSWORD ?? "password");
  await page.getByRole("button", { name: "Login" }).click();
  await expect(page.getByText("Your roles do not admit this page; it needs viewer.")).toBeVisible();
  // The server refuses it too, not only the page: the session cookie and this origin's Origin on a call of the machine API.
  const res = await page.evaluate(async (toolId) => {
    const r = await fetch("/tracekeep.v1.MachineService/ListDevices", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ selector: { toolId } }),
    });
    return { status: r.status, body: await r.text() };
  }, tool);
  expect(res.status, res.body).toBe(403);
  expect(res.body).toContain("permission_denied");
});

// The IdP down after start and back (the gate's case, api.md §10.2 *People's sign-in*): the leg's commands stop it (TRACEKEEP_IDP_STOP)
// and start it again (TRACEKEEP_IDP_START), each bounded. Down, a signed-in page keeps reading with its session, and a new login cannot sign in:
// a replica that never fetched refuses it as unavailable (503); one that has redirects to the IdP (TRACEKEEP_ISSUER), which does not answer.
// Back, a sign-in succeeds once the replicas fetch again, retried while the fetch interval holds them back.
test("a session outlives an outage of the IdP, and sign-in returns with it", async ({ page, browser, baseURL }) => {
  const stop = process.env.TRACEKEEP_IDP_STOP;
  const start = process.env.TRACEKEEP_IDP_START;
  const issuer = process.env.TRACEKEEP_ISSUER;
  test.skip(!stop || !start || !issuer, "needs a leg's IdP, with the commands to stop and start it");
  test.setTimeout(600_000);
  await signIn(page);
  try {
    execSync(stop ?? "", { stdio: "inherit", timeout: 180_000 });
    await page.reload();
    await page.getByLabel("Tool id").fill(tool);
    await page.getByRole("button", { name: "Open" }).click();
    await expect(page.getByRole("checkbox", { name: `Select ${device}` })).toBeVisible();

    const other = await browser.newContext({ baseURL });
    const login = await other.newPage();
    const answered = login.waitForResponse((r) => new URL(r.url()).pathname === "/auth/login", { timeout: 30_000 });
    const navigated = login.goto("/auth/login?return=%2F", { timeout: 30_000 }).then(
      () => "",
      (e: unknown) => String(e),
    );
    const res = await answered;
    const failure = await navigated;
    if (res.status() === 503) {
      expect(await res.text()).toContain("Sign-in failed: idp-unavailable.");
    } else {
      expect(res.status(), "a new login while the IdP is down").toBe(302);
      const location = (await res.headerValue("location")) ?? "";
      expect(location.startsWith(`${issuer ?? ""}/`), `redirected to ${location}`).toBe(true);
      expect(failure, "the IdP's authorization page while it is down").toMatch(/net::ERR_|Timeout/);
    }
    await login.goto("/");
    await expect(login.getByRole("link", { name: "Sign in" })).toBeVisible();
    await other.close();
  } finally {
    execSync(start ?? "", { stdio: "inherit", timeout: 360_000 });
  }

  const back = await browser.newContext({ baseURL });
  const again = await back.newPage();
  await expect(async () => {
    await signIn(again);
  }).toPass({ timeout: 240_000, intervals: [5_000] });
  await back.close();
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
