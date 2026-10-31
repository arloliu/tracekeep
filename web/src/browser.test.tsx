import { timestampDate, timestampFromDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, type ConnectRouter } from "@connectrpc/connect";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, test } from "vitest";

import { thinningText } from "@/browser";
import { MachineService, type ListCapturesRequest, type ListDevicesRequest, type ListPacksRequest } from "@/gen/tracekeep/v1/machine_pb";
import { recentTools, validateSelector, windowOf } from "@/selector";
import { renderApp, stubFetch } from "@/test/render";

const capID = "0192f1a0-0000-7000-8000-000000000001";
const hour0 = new Date("2026-10-31T08:00:00Z");
const ts = (iso: string) => timestampFromDate(new Date(iso));

// asked records the requests the fake machine API received.
interface Asked {
  devices: ListDevicesRequest[];
  captures: ListCapturesRequest[];
  packs: ListPacksRequest[];
}

function machine(asked: Asked) {
  return (r: ConnectRouter) =>
    r.service(MachineService, {
      listDevices(req) {
        asked.devices.push(req);
        return { devices: [{ name: "eq-a", openCaptures: 1n, overlap: true, lastRecord: ts("2026-10-31T08:30:00Z"), gap: false }, { name: "eq-b", openCaptures: 0n }] };
      },
      listCaptures(req) {
        asked.captures.push(req);
        return {
          captures: [
            {
              captureId: capID, state: "open", device: "eq-a", origin: ts("2026-10-31T07:00:00Z"),
              thinning: [{ stream: 6n, function: 11n, source: "every 10", keepEveryN: 10n }],
              unregistered: [{ end: ts("2026-10-31T07:00:00Z") }],
            },
          ],
        };
      },
      listPacks(req) {
        asked.packs.push(req);
        return {
          hours: [
            { hour: timestampFromDate(hour0), scopes: [{ captureId: capID, state: "sealed", packs: [{ packId: "p1", ranges: [{ seqFrom: 1n, seqTo: 40n }] }] }] },
            { hour: timestampFromDate(new Date(hour0.getTime() + 3_600_000)), noData: true, spanned: true },
            { hour: timestampFromDate(new Date(hour0.getTime() + 7_200_000)), evicted: [capID] },
          ],
          captures: [
            {
              capture: { captureId: capID, state: "open", device: "eq-a" },
              completeness: { frontier: 40n, gaps: [{ seqFrom: 12n, seqTo: 14n, start: ts("2026-10-31T08:10:00Z"), end: ts("2026-10-31T08:11:00Z") }], unknownTailFrom: 41n },
            },
          ],
        };
      },
    });
}

const fresh = (): Asked => ({ devices: [], captures: [], packs: [] });

beforeEach(() => {
  localStorage.clear();
  localStorage.setItem("tracekeep.zone", "utc");
  stubFetch();
});

test("a tool typed opens its devices over the last hour and is remembered", async () => {
  const asked = fresh();
  const { router } = renderApp("/", machine(asked));
  await userEvent.type(await screen.findByLabelText("Tool id"), "T-1");
  const before = Date.now();
  await userEvent.click(screen.getByRole("button", { name: "Open" }));
  expect(await screen.findByText("eq-a")).toBeInTheDocument();
  expect(router.state.location.search).toEqual({ tool: "T-1", last: "1h" });
  const sel = asked.devices[0]?.selector;
  expect(sel?.toolId).toBe("T-1");
  expect(sel?.devices).toEqual([]);
  const span = Number((sel?.to?.seconds ?? 0n) - (sel?.from?.seconds ?? 0n));
  expect(span).toBe(3600);
  expect(Number(sel?.to?.seconds ?? 0n) * 1000).toBeGreaterThanOrEqual(Math.floor(before / 1000) * 1000);
  expect(recentTools()).toEqual(["T-1"]);
  expect(screen.getByText("overlap")).toBeInTheDocument();
  expect(asked.captures).toHaveLength(0); // no device selected yet
});

test("selecting a device lists its captures and their completeness by hour, with links into the records", async () => {
  const asked = fresh();
  const { router } = renderApp("/?tool=T-1&last=6h", machine(asked));
  await userEvent.click(await screen.findByRole("checkbox", { name: "Select eq-a" }));
  await waitFor(() => { expect(router.state.location.search.devices).toEqual(["eq-a"]); });
  expect(await screen.findAllByText(capID)).toHaveLength(2); // its row and its completeness
  const sel = asked.captures[0]?.selector;
  expect(sel?.devices).toEqual(["eq-a"]);
  expect(Number((sel?.to?.seconds ?? 0n) - (sel?.from?.seconds ?? 0n))).toBe(6 * 3600);
  expect(asked.packs[0]?.selector?.devices).toEqual(["eq-a"]);

  expect(screen.getByText("S6F11 every 10")).toBeInTheDocument();
  expect(screen.getByText("open", { selector: "span" })).toBeInTheDocument(); // the unregistered interval's open start

  const records = screen.getByRole("link", { name: `Records of ${capID}` });
  const url = new URL(records.getAttribute("href") ?? "", "https://h");
  expect(url.pathname).toBe("/records");
  expect(validateSelector(Object.fromEntries([...url.searchParams].map(([k, v]) => [k, k === "devices" ? JSON.parse(v) as unknown : v])))).toEqual({
    tool: "T-1", devices: ["eq-a"], capture: capID, from: sel?.from && timestampDate(sel.from).toISOString(), to: sel?.to && timestampDate(sel.to).toISOString(),
  });

  const hours = await screen.findByRole("table", { name: "Hours" });
  const first = within(hours).getByRole("link", { name: /2026-10-31 08:00:00.000 UTC/ });
  const hourURL = new URL(first.getAttribute("href") ?? "", "https://h");
  expect(hourURL.searchParams.get("from")).toBe("2026-10-31T08:00:00.000Z");
  expect(hourURL.searchParams.get("to")).toBe("2026-10-31T09:00:00.000Z");
  expect(hourURL.searchParams.get("capture")).toBeNull();
  expect(within(hours).getByText(/1 pack, seqs 1–40/)).toBeInTheDocument();
  expect(within(hours).getByText("no data within a capture's span")).toBeInTheDocument();
  expect(within(hours).getByText("evicted")).toBeInTheDocument();
  expect(screen.getByText(/seqs 12–14/)).toBeInTheDocument(); // the gap
  expect(screen.getByText("from seq 41")).toBeInTheDocument(); // the unknown tail
});

test("a custom window is read in the chosen zone, and from must come before to", async () => {
  const asked = fresh();
  const { router } = renderApp("/", machine(asked));
  await userEvent.type(await screen.findByLabelText("Tool id"), "T-1");
  const from = screen.getByLabelText("From");
  const to = screen.getByLabelText("To");
  await userEvent.clear(from);
  await userEvent.type(from, "2026-10-31T09:00");
  await userEvent.clear(to);
  await userEvent.type(to, "2026-10-31T08:00");
  await userEvent.click(screen.getByRole("button", { name: "Open" }));
  expect(screen.getByRole("alert")).toHaveTextContent("From must be a time before to.");
  expect(asked.devices).toHaveLength(0);
  await userEvent.clear(to);
  await userEvent.type(to, "2026-10-31T10:30");
  await userEvent.click(screen.getByRole("button", { name: "Open" }));
  await screen.findByText("eq-a");
  expect(router.state.location.search).toEqual({ tool: "T-1", from: "2026-10-31T09:00:00.000Z", to: "2026-10-31T10:30:00.000Z" });
  expect(asked.devices[0]?.selector?.from?.seconds).toBe(BigInt(Date.parse("2026-10-31T09:00:00Z") / 1000));
});

test("an operation the roles do not admit says so where its result would be", async () => {
  renderApp("/?tool=T-1", (r) =>
    r.service(MachineService, {
      listDevices() {
        throw new ConnectError("roles", Code.PermissionDenied);
      },
    }),
  );
  expect(await screen.findByText("Your roles do not admit this.")).toBeInTheDocument();
});

describe("the selector in the URL", () => {
  test("keeps what it understands and drops the rest", () => {
    expect(validateSelector({ tool: 123, devices: "eq-a", last: "2h", from: "nope", to: "2026-10-31T00:00:00Z", junk: 1 })).toEqual({ tool: "123", devices: ["eq-a"] });
    expect(validateSelector({ tool: "T", from: "2026-10-31T01:00:00Z", to: "2026-10-31T00:00:00Z", last: "24h" })).toEqual({ tool: "T", last: "24h" });
    expect(validateSelector({ devices: ["a", "a", 7] })).toEqual({ devices: ["a", "7"] });
  });

  test("a preset ends at now, and from and to win", () => {
    const now = Date.parse("2026-10-31T12:00:00Z");
    expect(windowOf({ last: "7d" }, now).from.toISOString()).toBe("2026-10-24T12:00:00.000Z");
    expect(windowOf({}, now).from.toISOString()).toBe("2026-10-31T11:00:00.000Z");
    expect(windowOf({ last: "1h", from: "2026-10-30T00:00:00.000Z", to: "2026-10-30T01:00:00.000Z" }, now).to.toISOString()).toBe("2026-10-30T01:00:00.000Z");
  });

  test("a thinning names its sampling rule, or its source when that does not parse", () => {
    expect(thinningText({ stream: 1n, function: 3n, source: "every 500ms", keepIntervalMs: 500n } as never)).toBe("S1F3 every 500 ms");
    expect(thinningText({ stream: 1n, function: 3n, source: "odd" } as never)).toBe("S1F3 odd");
  });
});
