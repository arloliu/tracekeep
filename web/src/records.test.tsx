import type { MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, type ConnectRouter, type HandlerContext, type ServiceImpl } from "@connectrpc/connect";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, test } from "vitest";

import {
  MachineService,
  type FindTransactionRequest,
  type QueryRecordsRequest,
  type QueryRecordsResponseSchema,
  type RecordItemSchema,
} from "@/gen/tracekeep/v1/machine_pb";
import { validateRecords } from "@/selector";
import { json, renderApp, stubFetch } from "@/test/render";

type Batch = MessageInitShape<typeof QueryRecordsResponseSchema>;
type Item = MessageInitShape<typeof RecordItemSchema>;

const capID = "0192f1a0-0000-7000-8000-000000000001";
const hour = timestampFromDate(new Date("2026-10-31T08:00:00Z"));
const window = "from=2026-10-31T08%3A00%3A00.000Z&to=2026-10-31T09%3A00%3A00.000Z";

const start = (truncated = false): Item => ({
  item: { case: "capture", value: { capture: { captureId: capID, device: "eq-a", state: "open" }, completeness: { gaps: [{ seqFrom: 5n, seqTo: 6n }] }, completenessTruncated: truncated } },
});
const record = (seq: bigint, extra: object = {}): Item => ({
  item: {
    case: "record",
    value: {
      hour, seq, time: timestampFromDate(new Date(Date.parse("2026-10-31T08:00:00Z") + Number(seq) * 1000)), kind: "data", direction: "to_host",
      header: { stream: 6, function: 11, w: true, systemBytes: new Uint8Array([0, 0, 0, Number(seq)]) }, sml: `<L [0]> /* seq ${String(seq)} */`, ...extra,
    },
  },
});

// stream answers QueryRecords from answer, recording each request.
function stream(asked: QueryRecordsRequest[], answer: (req: QueryRecordsRequest, ctx: HandlerContext) => Iterable<Batch> | AsyncIterable<Batch>, also: Partial<ServiceImpl<typeof MachineService>> = {}) {
  return (r: ConnectRouter) => {
    r.service(MachineService, {
      ...also,
      queryRecords(req, ctx) {
        asked.push(req);
        return each(answer(req, ctx));
      },
    });
  };
}

async function* each(batches: Iterable<Batch> | AsyncIterable<Batch>) {
  for await (const b of batches) yield b;
}

beforeEach(() => {
  localStorage.clear();
  localStorage.setItem("tracekeep.zone", "utc");
  stubFetch();
});

test("the stream fills one list with the records and the markers, the URL's predicates asked", async () => {
  const asked: QueryRecordsRequest[] = [];
  renderApp(
    `/records?tool=T-1&devices=%5B%22eq-a%22%5D&capture=${capID}&${window}&messages=%5B%22S6F11%22%5D&dir=to_host&kinds=%5B%22data%22%5D&events=true&seqFrom=%2210%22&seqTo=%2218446744073709551615%22`,
    stream(asked, function* () {
      yield { items: [start(true), record(1n)], cursor: "c1" };
      yield {
        items: [record(2n, { conflict: true }), { item: { case: "incomplete", value: { hour, reason: "corrupt", seqFrom: 3n, seqTo: 4n, detail: "a block" } } }, { item: { case: "unknownTail", value: { from: 3n, open: true } } }],
        cursor: "c2",
        end: { records: 2n, incomplete: true },
      };
    }),
  );
  expect(await screen.findByText(/2 records in the list/)).toBeInTheDocument();
  const req = asked[0];
  expect(req.selector?.toolId).toBe("T-1");
  expect(req.selector?.captureId).toBe(capID);
  expect(req.selector?.devices).toEqual(["eq-a"]);
  expect(req.messages).toEqual(["S6F11"]);
  expect(req.direction).toBe("to_host");
  expect(req.kinds).toEqual(["data"]);
  expect(req.includeTransportEvents).toBe(true);
  expect(req.seqFrom).toBe(10n);
  expect(req.seqTo).toBe(18446744073709551615n);
  expect(req.cursor).toBe("");

  const list = screen.getByRole("list", { name: "Records" });
  expect(within(list).getByText(/gaps 1/)).toBeInTheDocument();
  expect(within(list).getByText("conflict")).toBeInTheDocument();
  expect(within(list).getByText(/Incomplete: corrupt \(hour 2026-10-31T08Z, seqs 3–4\): a block/)).toBeInTheDocument();
  expect(within(list).getByText("Unknown tail from seq 3; the capture is still open")).toBeInTheDocument();
  expect(screen.getByText("incomplete")).toBeInTheDocument();
  // A truncated completeness links to the browser, which lists it whole.
  const whole = within(list).getByRole("link", { name: "the browser has it whole" });
  const url = new URL(whole.getAttribute("href") ?? "", "https://h");
  expect(url.pathname).toBe("/");
  expect(url.searchParams.get("tool")).toBe("T-1");
  expect(url.searchParams.get("from")).toBe("2026-10-31T08:00:00.000Z");
  expect(screen.queryByRole("button", { name: "Next page" })).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Continue" })).not.toBeInTheDocument();
});

test("a selected record shows its header, SML, holders and transaction, looked up in its capture", async () => {
  const asked: QueryRecordsRequest[] = [];
  const looked: FindTransactionRequest[] = [];
  renderApp(
    `/records?tool=T-1&${window}`,
    stream(
      asked,
      function* () {
        yield {
          items: [start(), record(7n, { conflict: true, holders: [{ packId: "pack-1", state: "staged" }], smlTruncated: true }), record(8n, { sml: "", smlError: "bad item length", header: { stream: 6, function: 12, systemBytes: new Uint8Array([0, 0, 0, 7]) } })],
          cursor: "c1",
          end: { records: 2n },
        };
      },
      {
        findTransaction(req) {
          looked.push(req);
          return {
            outcome: "matched", windowEnd: 9n, hours: [hour],
            records: [{ record: { seq: 7n, kind: "data", direction: "to_host", header: { stream: 6, function: 11, w: true } }, roles: ["primary"], inWindow: true, valid: true }],
            gaps: [{ reason: "evicted", seq: 9n }],
          };
        },
      },
    ),
  );
  const list = await screen.findByRole("list", { name: "Records" });
  await userEvent.click(await within(list).findByRole("button", { name: "Seq 7" }));
  const panel = screen.getByRole("complementary", { name: "Selected" });
  expect(within(panel).getByText("Seq 7")).toBeInTheDocument();
  expect(within(panel).getByText("S6F11 W")).toBeInTheDocument();
  expect(within(panel).getByText("00000007")).toBeInTheDocument();
  expect(within(panel).getByText("<L [0]> /* seq 7 */")).toBeInTheDocument();
  expect(within(panel).getByText("The SML was cut at 64 KiB.")).toBeInTheDocument();
  expect(within(panel).getByText("pack-1 (staged)")).toBeInTheDocument();
  expect(await within(panel).findByText("matched")).toBeInTheDocument();
  expect(within(panel).getByText(/primary ✓/)).toBeInTheDocument();
  expect(within(panel).getByText(/Gap: evicted, seq 9/)).toBeInTheDocument();
  expect(looked[0]?.captureId).toBe(capID);
  expect(looked[0]?.seq).toBe(7n);
  expect(looked[0]?.hour?.seconds).toBe(hour.seconds);

  const lookups = looked.length;
  await userEvent.click(within(list).getByRole("button", { name: "Seq 8" }));
  const reply = screen.getByRole("complementary", { name: "Selected" });
  expect(await within(reply).findByText("The body does not render: bad item length")).toBeInTheDocument();
  expect(within(reply).getByText("A reply: its transaction is looked up from its primary.")).toBeInTheDocument();
  expect(looked).toHaveLength(lookups);
});

test("a limit's cursor offers the next page, whose items join the same list", async () => {
  const asked: QueryRecordsRequest[] = [];
  renderApp(
    `/records?tool=T-1&${window}`,
    stream(asked, function* (req) {
      if (req.cursor === "") {
        yield { items: [start(), record(1n)], cursor: "c1", end: { records: 1n, cursor: "page-2" } };
      } else {
        yield { items: [record(2n)], cursor: "c2", end: { records: 1n } };
      }
    }),
  );
  await userEvent.click(await screen.findByRole("button", { name: "Next page" }));
  expect(await screen.findByText(/^2 records in the list/)).toBeInTheDocument();
  expect(asked.map((r) => r.cursor)).toEqual(["", "page-2"]);
  expect(asked[1]?.selector?.from).toEqual(asked[0]?.selector?.from); // the cursor continues the same selector
  const list = screen.getByRole("list", { name: "Records" });
  expect(within(list).getAllByRole("listitem")).toHaveLength(3);
});

test("an interrupted stream keeps its items, says the code, and continues from the last batch's cursor", async () => {
  const asked: QueryRecordsRequest[] = [];
  renderApp(
    `/records?tool=T-1&${window}`,
    stream(asked, function* (req) {
      if (req.cursor === "") {
        yield { items: [start(), record(1n)], cursor: "c1" };
        yield { items: [record(2n)], cursor: "c2" };
        throw new ConnectError("the store failed", Code.Unavailable);
      }
      yield { items: [record(3n)], cursor: "c3", end: { records: 1n } };
    }),
  );
  expect(await screen.findByText("Interrupted after 2 records in the list.")).toBeInTheDocument();
  expect(screen.getByRole("alert")).toHaveTextContent("Unavailable: the store failed");
  await userEvent.click(screen.getByRole("button", { name: "Continue" }));
  expect(await screen.findByText(/^3 records in the list/)).toBeInTheDocument();
  expect(asked.map((r) => r.cursor)).toEqual(["", "c2"]);
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});

test("a stream that stops without its end is interrupted too", async () => {
  renderApp(
    `/records?tool=T-1&${window}`,
    stream([], function* () {
      yield { items: [start(), record(1n)], cursor: "c1" };
    }),
  );
  expect(await screen.findByText("Interrupted after 1 record in the list.")).toBeInTheDocument();
});

test("changing a filter cancels the stream, which is not shown as a failure", async () => {
  const asked: QueryRecordsRequest[] = [];
  let aborted = false;
  let release: () => void = () => undefined;
  const released = new Promise<void>((resolve) => { release = resolve; });
  const { router } = renderApp(
    `/records?tool=T-1&${window}`,
    stream(asked, async function* (req, ctx) {
      if (req.selector?.toolId === "T-1") {
        yield { items: [start(), record(1n)], cursor: "c1" };
        await new Promise((_, reject) => {
          ctx.signal.addEventListener("abort", () => {
            aborted = true;
            reject(new ConnectError("canceled", Code.Canceled));
          });
        });
      }
      yield { items: [record(9n)], cursor: "x" };
      await released;
      yield { items: [], cursor: "x", end: { records: 1n } };
    }),
  );
  expect(await screen.findByText("Reading… 1 record in the list.")).toBeInTheDocument();
  void router.navigate({ to: "/records", search: (s) => ({ ...s, tool: "T-2" }) });
  await waitFor(() => { expect(aborted).toBe(true); });
  await waitFor(() => { expect(asked.map((r) => r.selector?.toolId)).toEqual(["T-1", "T-2"]); });
  await new Promise((r) => setTimeout(r, 20)); // the old stream's rejection has landed
  expect(screen.getByText("Reading… 1 record in the list.")).toBeInTheDocument();
  expect(screen.queryByText(/Interrupted/)).not.toBeInTheDocument();
  release();
  expect(await screen.findByText(/^1 record in the list; complete/)).toBeInTheDocument();
});

test("a preset's window is fixed when the page opens, so the stream starts once", async () => {
  const asked: QueryRecordsRequest[] = [];
  renderApp(
    "/records?tool=T-1&last=6h",
    stream(asked, function* () {
      yield { items: [start(), record(1n)], cursor: "c1", end: { records: 1n } };
    }),
  );
  expect(await screen.findByText(/^1 record in the list; complete/)).toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "Time zone" })); // the page renders again
  await new Promise((r) => setTimeout(r, 20));
  expect(asked).toHaveLength(1);
  const sel = asked[0]?.selector;
  expect(Number((sel?.to?.seconds ?? 0n) - (sel?.from?.seconds ?? 0n))).toBe(6 * 3600);
});

test("a stream answering unauthenticated returns to the sign-in page", async () => {
  let me = 0;
  stubFetch({ "/auth/me": () => (++me === 1 ? json({ subject: "alice", roles: ["viewer"], expiry: "2099-01-01T00:00:00Z" }) : new Response(null, { status: 401 })) });
  renderApp(
    `/records?tool=T-1&${window}`,
    stream([], function* () {
      yield* [];
      throw new ConnectError("session", Code.Unauthenticated);
    }),
  );
  expect(await screen.findByRole("link", { name: "Sign in" })).toBeInTheDocument();
});

test("a long list renders only the rows in view", async () => {
  const items = [start(), ...Array.from({ length: 2000 }, (_, i) => record(BigInt(i + 1)))];
  renderApp(
    `/records?tool=T-1&${window}`,
    stream([], function* () {
      yield { items, cursor: "c", end: { records: 2000n } };
    }),
  );
  expect(await screen.findByText(/^2000 records in the list/)).toBeInTheDocument();
  const rows = within(screen.getByRole("list", { name: "Records" })).getAllByRole("listitem");
  expect(rows.length).toBeGreaterThan(15);
  expect(rows.length).toBeLessThan(60);
});

test("the filter bar refuses a seq range without a capture", async () => {
  const asked: QueryRecordsRequest[] = [];
  renderApp(`/records?tool=T-1&${window}`, stream(asked, function* () {
    yield { items: [], cursor: "", end: { records: 0n } };
  }));
  await screen.findByText(/^0 records in the list/);
  await userEvent.type(screen.getByLabelText("Seq from"), "5");
  await userEvent.click(screen.getByRole("button", { name: "Apply" }));
  expect(screen.getByRole("alert")).toHaveTextContent("A seq range needs a capture.");
  expect(asked).toHaveLength(1);
});

describe("the records' search", () => {
  test("keeps the predicates it understands, seqs only with a capture", () => {
    expect(validateRecords({ tool: "T", messages: "S1F1", dir: "sideways", kinds: ["data", "bogus"], events: "true", seqFrom: "5" })).toEqual({
      tool: "T", messages: ["S1F1"], kinds: ["data"], events: true,
    });
    expect(validateRecords({ capture: "c", seqFrom: 5, seqTo: "18446744073709551616" })).toEqual({ capture: "c", seqFrom: "5" });
  });
});
