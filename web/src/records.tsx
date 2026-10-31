import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Code } from "@connectrpc/connect";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef, useState, type ReactNode, type SyntheticEvent } from "react";

import { useMachine } from "@/api";
import { facts, seqs } from "@/browser";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ErrorNote } from "@/errors";
import { ExportLink } from "@/exports";
import type { CaptureStart, Completeness, HSMSHeader, Incomplete, Record as TkRecord, UnknownTail } from "@/gen/tracekeep/v1/machine_pb";
import { cn } from "@/lib/utils";
import { directions, kinds, selectorOf, windowOf, type RecordsSearch, type Window } from "@/selector";
import { useRecordStream, type RecordsRequest, type Row, type Stream } from "@/stream";
import { fromInput, inputValue, Time, useZone } from "@/time";

// Records is the records page (api.md §10.3 *Pages*): a filter bar, QueryRecords streamed into a virtualized list,
// and the selected item's panel.
export function Records() {
  const search = useSearch({ from: "/records" });
  const { zone } = useZone();
  // A preset's window is fixed when the page opens: a cursor continues only the selector that issued it.
  const [now] = useState(() => Date.now());
  const w = useMemo(() => windowOf(search, now), [search, now]);
  const req = useMemo(() => (search.tool === undefined ? undefined : requestOf(search, w)), [search, w]);
  return (
    <div className="space-y-4">
      <FilterBar key={`${JSON.stringify(search)}|${zone}`} search={search} w={w} />
      {req === undefined ? (
        <p className="text-muted-foreground">Choose a tool to read its records.</p>
      ) : (
        <>
          <ExportLink search={search} from={w.from} to={w.to} />
          <RecordList req={req} search={search} w={w} />
        </>
      )}
    </div>
  );
}

// requestOf is search's QueryRecords request over w.
export function requestOf(s: RecordsSearch, w: Window): RecordsRequest {
  return {
    selector: selectorOf(s, w),
    messages: s.messages ?? [],
    direction: s.dir ?? "",
    kinds: s.kinds ?? [],
    includeTransportEvents: s.events ?? false,
    seqFrom: s.seqFrom === undefined ? undefined : BigInt(s.seqFrom),
    seqTo: s.seqTo === undefined ? undefined : BigInt(s.seqTo),
  };
}

const split = (v: string) =>
  v
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x !== "");

function FilterBar({ search, w }: { search: RecordsSearch; w: Window }) {
  const { zone } = useZone();
  const navigate = useNavigate({ from: "/records" });
  const [f, setF] = useState({
    tool: search.tool ?? "",
    devices: (search.devices ?? []).join(", "),
    capture: search.capture ?? "",
    from: inputValue(w.from, zone),
    to: inputValue(w.to, zone),
    messages: (search.messages ?? []).join(", "),
    dir: search.dir ?? "",
    kinds: search.kinds ?? [],
    events: search.events ?? false,
    seqFrom: search.seqFrom ?? "",
    seqTo: search.seqTo ?? "",
  });
  const [problem, setProblem] = useState("");
  const set = (k: keyof typeof f) => (e: { target: { value: string } }) => {
    setF({ ...f, [k]: e.target.value });
  };

  const submit = (e: SyntheticEvent) => {
    e.preventDefault();
    const from = fromInput(f.from, zone);
    const to = fromInput(f.to, zone);
    const problems = [
      f.tool.trim() === "" && "Type a tool id.",
      (from === undefined || to === undefined || from >= to) && "From must be a time before to.",
      (f.seqFrom !== "" || f.seqTo !== "") && f.capture.trim() === "" && "A seq range needs a capture.",
      [f.seqFrom, f.seqTo].some((s) => s !== "" && !/^\d+$/.test(s)) && "A seq is a whole number.",
      f.seqFrom !== "" && f.seqTo !== "" && /^\d+$/.test(f.seqFrom) && /^\d+$/.test(f.seqTo) && BigInt(f.seqFrom) > BigInt(f.seqTo) && "The seq range is reversed.",
    ].filter((p): p is string => typeof p === "string");
    if (problems.length > 0 || from === undefined || to === undefined) {
      setProblem(problems.join(" "));
      return;
    }
    const devices = split(f.devices);
    const messages = split(f.messages).map((m) => m.toUpperCase());
    void navigate({
      search: {
        tool: f.tool.trim(),
        devices: devices.length > 0 ? devices : undefined,
        capture: f.capture.trim() || undefined,
        from: from.toISOString(),
        to: to.toISOString(),
        messages: messages.length > 0 ? messages : undefined,
        dir: f.dir === "" ? undefined : (f.dir as RecordsSearch["dir"]),
        kinds: f.kinds.length > 0 ? f.kinds : undefined,
        events: f.events || undefined,
        seqFrom: f.seqFrom || undefined,
        seqTo: f.seqTo || undefined,
      },
    });
  };

  const field = (label: string, input: ReactNode) => (
    <label className="grid gap-1 text-sm">
      {label}
      {input}
    </label>
  );
  return (
    <form onSubmit={submit} className="space-y-2" aria-label="Filters">
      <div className="flex flex-wrap items-end gap-3">
        {field("Tool id", <Input className="w-40" value={f.tool} onChange={set("tool")} />)}
        {field("Devices", <Input className="w-48" placeholder="every device" value={f.devices} onChange={set("devices")} />)}
        {field("Capture", <Input className="w-80 font-mono text-xs" value={f.capture} onChange={set("capture")} />)}
        {field("From", <Input type="datetime-local" className="w-52" value={f.from} onChange={set("from")} />)}
        {field("To", <Input type="datetime-local" className="w-52" value={f.to} onChange={set("to")} />)}
      </div>
      <div className="flex flex-wrap items-end gap-3">
        {field("Messages", <Input className="w-56" placeholder="S6F11, S2F*" value={f.messages} onChange={set("messages")} />)}
        {field(
          "Direction",
          <select className="h-9 rounded-md border border-input bg-transparent px-2 text-sm" value={f.dir} onChange={set("dir")}>
            <option value="">both</option>
            {directions.map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>,
        )}
        <fieldset className="flex items-center gap-2 text-sm">
          <legend className="sr-only">Kinds</legend>
          {kinds.map((k) => (
            <label key={k} className="flex items-center gap-1">
              <input
                type="checkbox"
                checked={f.kinds.includes(k)}
                onChange={() => {
                  setF({ ...f, kinds: f.kinds.includes(k) ? f.kinds.filter((x) => x !== k) : [...f.kinds, k] });
                }}
              />
              {k}
            </label>
          ))}
          <label className="flex items-center gap-1">
            <input type="checkbox" checked={f.events} onChange={() => { setF({ ...f, events: !f.events }); }} />
            transport events
          </label>
        </fieldset>
        {field("Seq from", <Input className="w-28" inputMode="numeric" value={f.seqFrom} onChange={set("seqFrom")} />)}
        {field("Seq to", <Input className="w-28" inputMode="numeric" value={f.seqTo} onChange={set("seqTo")} />)}
        <Button type="submit">Apply</Button>
      </div>
      {problem !== "" && <p role="alert" className="text-sm text-destructive">{problem}</p>}
    </form>
  );
}

const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const message = (h: HSMSHeader) => `S${String(h.stream)}F${String(h.function)}${h.w ? " W" : ""}`;

// summary sums a capture's completeness up in one line.
export function summary(c: Completeness | undefined): string {
  if (!c) return "no completeness";
  const parts = facts(c)
    .filter(([label, items]) => items.length > 0 && label !== "Frontier" && label !== "Retained" && label !== "Unknown tail")
    .map(([label, items]) => `${label.toLowerCase()} ${String(items.length)}`);
  return parts.length > 0 ? parts.join(" · ") : "nothing missing in the window";
}

const recordHeight = 32;
const markerHeight = 52;

function RecordList({ req, search, w }: { req: RecordsRequest; search: RecordsSearch; w: Window }) {
  const { stream, more } = useRecordStream(req);
  const [picked, setPicked] = useState<{ rows: Row[]; index: number } | undefined>();
  // A selection belongs to the list it was made in; a new stream clears it.
  const selected = picked && stream.rows[picked.index] === picked.rows[picked.index] ? picked.index : undefined;
  const scroller = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line react-hooks/incompatible-library -- the build runs no React Compiler to skip it
  const v = useVirtualizer({
    count: stream.rows.length,
    getScrollElement: () => scroller.current,
    estimateSize: (i) => (stream.rows[i]?.item.item.case === "record" ? recordHeight : markerHeight),
    overscan: 10,
  });
  const records = stream.rows.filter((r) => r.item.item.case === "record").length;
  return (
    <div className="flex gap-4">
      <div className="min-w-0 flex-1 space-y-2">
        <div ref={scroller} data-scroller="" className="h-[65vh] overflow-auto rounded-md border" role="list" aria-label="Records">
          <div className="relative w-full" style={{ height: v.getTotalSize() }}>
            {v.getVirtualItems().map((vi) => {
              const row = stream.rows[vi.index];
              return (
                <div key={vi.key} role="listitem" className="absolute left-0 w-full" style={{ height: vi.size, transform: `translateY(${String(vi.start)}px)` }}>
                  <RowView row={row} w={w} search={search} selected={selected === vi.index} onSelect={() => { setPicked({ rows: stream.rows, index: vi.index }); }} />
                </div>
              );
            })}
          </div>
        </div>
        <StreamEnd stream={stream} records={records} more={more} />
      </div>
      {selected !== undefined && stream.rows[selected] && (
        <aside className="w-[30rem] shrink-0 space-y-3 overflow-auto rounded-md border p-3 text-sm" aria-label="Selected">
          <Panel row={stream.rows[selected]} />
        </aside>
      )}
    </div>
  );
}

function StreamEnd({ stream, records, more }: { stream: Stream; records: number; more: (cursor: string) => void }) {
  const said = `${String(records)} ${records === 1 ? "record" : "records"} in the list`;
  if (stream.status === "streaming") return <p className="text-sm text-muted-foreground">Reading… {said}.</p>;
  if (stream.status === "interrupted") {
    const code = stream.error?.code ?? Code.Unknown;
    const final = code === Code.InvalidArgument || code === Code.PermissionDenied || code === Code.Unauthenticated;
    return (
      <div className="space-y-2">
        <p className="text-sm font-medium text-destructive">Interrupted after {said}.</p>
        <ErrorNote error={stream.error} />
        {!final && (
          <Button size="sm" variant="outline" onClick={() => { more(stream.cursor); }}>
            Continue
          </Button>
        )}
      </div>
    );
  }
  const end = stream.end;
  return (
    <div className="flex items-center gap-3 text-sm">
      <p>
        {said}{end && end.records !== BigInt(records) && ` (${String(end.records)} in the last page)`}
        {stream.incomplete ? <>; <Badge variant="warn">incomplete</Badge></> : "; complete."}
      </p>
      {end && end.cursor !== "" && (
        <Button size="sm" variant="outline" onClick={() => { more(end.cursor); }}>
          Next page
        </Button>
      )}
    </div>
  );
}

function RowView({ row, w, search, selected, onSelect }: { row: Row; w: Window; search: RecordsSearch; selected: boolean; onSelect: () => void }) {
  const it = row.item.item;
  const pick = (children: ReactNode, className?: string, label?: string) => (
    <button type="button" aria-label={label} aria-pressed={selected} onClick={onSelect} className={cn("flex h-full w-full items-center gap-3 px-2 text-left text-sm hover:bg-muted/50", selected && "bg-muted", className)}>
      {children}
    </button>
  );
  switch (it.case) {
    case "record":
      return pick(<RecordLine r={it.value} />, "font-mono text-xs", `Seq ${String(it.value.seq)}`);
    case "capture":
      return (
        <div className="flex h-full items-center border-y bg-secondary/60">
          {pick(
            <span className="min-w-0 truncate">
              Capture <span className="font-mono text-xs">{it.value.capture?.captureId}</span> · {it.value.capture?.device} · {it.value.capture?.state} — {summary(it.value.completeness)}
            </span>,
          )}
          {it.value.completenessTruncated && <TruncatedNote start={it.value} w={w} search={search} />}
        </div>
      );
    case "withheld":
      return pick(<span className="text-warn">{withheldText}</span>);
    case "incomplete":
      return pick(<span className="truncate text-warn">Incomplete: {incompleteText(it.value)}</span>);
    case "unknownTail":
      return pick(<span className="text-muted-foreground">{tailText(it.value)}</span>);
    default:
      return null;
  }
}

const withheldText = "Withheld: the capture's configuration has no policy, so none of its records follow.";
const tailText = (t: UnknownTail) => `Unknown tail${t.from !== undefined ? ` from seq ${String(t.from)}` : ""}${t.open ? "; the capture is still open" : ""}`;

function TruncatedNote({ start, w, search }: { start: CaptureStart; w: Window; search: RecordsSearch }) {
  return (
    <span className="shrink-0 px-2 text-xs">
      Completeness cut to fit;{" "}
      <Link
        to="/"
        search={{ tool: search.tool, devices: start.capture ? [start.capture.device] : search.devices, from: w.from.toISOString(), to: w.to.toISOString() }}
        className="text-primary underline-offset-4 hover:underline"
      >
        the browser has it whole
      </Link>
    </span>
  );
}

function incompleteText(x: Incomplete): string {
  const where = [x.hour && `hour ${timestampDate(x.hour).toISOString().slice(0, 13)}Z`, seqs(x.seqFrom, x.seqTo)].filter(Boolean).join(", ");
  return `${x.reason}${where && ` (${where})`}${x.detail && `: ${x.detail}`}`;
}

function RecordLine({ r }: { r: TkRecord }) {
  return (
    <>
      <span className="w-60 shrink-0"><Time ts={r.time} /></span>
      <span className="w-16 shrink-0 text-right">{String(r.seq)}</span>
      <span className="w-24 shrink-0">{r.direction}</span>
      <span className="w-28 shrink-0">{r.kind}</span>
      <span className="w-20 shrink-0">{r.header && r.kind === "data" ? message(r.header) : ""}</span>
      <span className="w-20 shrink-0">{r.header ? hex(r.header.systemBytes) : ""}</span>
      <span className="flex gap-1">
        {r.conflict && <Badge variant="destructive">conflict</Badge>}
        {r.excluded && <Badge variant="warn">excluded</Badge>}
        {r.unobserved && <Badge variant="secondary">unobserved</Badge>}
      </span>
    </>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="contents">
      <dt className="text-muted-foreground">{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

function Panel({ row }: { row: Row }) {
  const it = row.item.item;
  switch (it.case) {
    case "record":
      return <RecordPanel r={it.value} capture={row.capture} />;
    case "capture": {
      const c = it.value.capture;
      return (
        <>
          <h3 className="font-semibold">Capture {c?.captureId}</h3>
          <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
            <Field label="Device">{c?.device}</Field>
            <Field label="State">{c?.state}</Field>
            <Field label="Origin"><Time ts={c?.origin} /></Field>
            <Field label="Last record"><Time ts={c?.lastRecord} /></Field>
            {it.value.completeness &&
              facts(it.value.completeness)
                .filter(([, items]) => items.length > 0)
                .map(([label, items]) => <Field key={label} label={label}>{items.map((x, i) => <div key={i}>{x}</div>)}</Field>)}
          </dl>
          {it.value.completenessTruncated && <p className="text-warn">The completeness was cut to fit a message; the browser gives it whole.</p>}
        </>
      );
    }
    case "incomplete":
      return (
        <>
          <h3 className="font-semibold">Incomplete</h3>
          <p>{incompleteText(it.value)}</p>
          {it.value.packs.length > 0 && (
            <ul className="font-mono text-xs">
              {it.value.packs.map((p) => <li key={p}>{p}</li>)}
              {it.value.morePacks > 0 && <li>and {it.value.morePacks} more</li>}
            </ul>
          )}
        </>
      );
    case "withheld":
      return <p>{withheldText}</p>;
    case "unknownTail":
      return <p>{tailText(it.value)}</p>;
    default:
      return null;
  }
}

function RecordPanel({ r, capture }: { r: TkRecord; capture: string }) {
  return (
    <>
      <h3 className="font-semibold">Seq {String(r.seq)}</h3>
      <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
        <Field label="Capture"><span className="font-mono text-xs">{capture}</span></Field>
        <Field label="Time"><Time ts={r.time} /></Field>
        <Field label="Hour"><Time ts={r.hour} /></Field>
        <Field label="Epoch">{r.epoch}</Field>
        <Field label="Kind">{r.kind}</Field>
        <Field label="Direction">{r.direction}</Field>
        {r.header && (
          <>
            {r.kind === "data" && <Field label="Message">{message(r.header)}</Field>}
            <Field label="Session id">{r.header.sessionId}</Field>
            <Field label="System bytes"><span className="font-mono">{hex(r.header.systemBytes)}</span></Field>
          </>
        )}
        <Field label="Flags">{[r.conflict && "conflict", r.excluded && "excluded", r.unobserved && "unobserved"].filter(Boolean).join(", ") || "—"}</Field>
      </dl>
      {r.holders.length > 0 && (
        <section>
          <h4 className="font-medium">Holders</h4>
          <ul className="font-mono text-xs">
            {r.holders.map((h) => <li key={h.packId}>{h.packId} ({h.state})</li>)}
          </ul>
        </section>
      )}
      {r.kind === "data" && (
        <section className="space-y-1">
          <h4 className="font-medium">SML</h4>
          {r.smlError !== "" ? (
            <p className="text-destructive">The body does not render: {r.smlError}</p>
          ) : (
            <pre className="max-h-80 overflow-auto rounded bg-muted p-2 text-xs whitespace-pre-wrap">{r.sml}</pre>
          )}
          {r.smlTruncated && <p className="text-warn">The SML was cut at 64 KiB.</p>}
        </section>
      )}
      {r.kind === "data" && r.hour && r.header && (r.header.function % 2 === 1 ? (
        <Transaction capture={capture} r={r} />
      ) : (
        <p className="text-muted-foreground">A reply: its transaction is looked up from its primary.</p>
      ))}
    </>
  );
}

function Transaction({ capture, r }: { capture: string; r: TkRecord }) {
  const machine = useMachine();
  const hourISO = r.hour ? timestampDate(r.hour).toISOString() : "";
  const q = useQuery({
    queryKey: ["transaction", capture, String(r.seq), hourISO],
    queryFn: async ({ signal }) => await machine.findTransaction({ captureId: capture, seq: r.seq, hour: r.hour }, { signal }),
  });
  return (
    <section className="space-y-1">
      <h4 className="font-medium">Transaction</h4>
      {q.isPending ? (
        <p className="text-muted-foreground">Looking it up…</p>
      ) : q.isError ? (
        <ErrorNote error={q.error} />
      ) : (
        <>
          <p>
            Outcome: <Badge variant={q.data.outcome === "matched" ? "ok" : "warn"}>{q.data.outcome}</Badge>
            {q.data.windowEnd !== undefined && ` · the reply window ends at seq ${String(q.data.windowEnd)}`}
            {` · ${String(q.data.hours.length)} ${q.data.hours.length === 1 ? "hour" : "hours"} searched`}
          </p>
          <ul aria-label="Transaction records" className="space-y-0.5 font-mono text-xs">
            {q.data.records.map((t, i) => (
              <li key={i}>
                seq {String(t.record?.seq ?? 0n)} {t.record?.header ? message(t.record.header) : t.record?.kind} {t.record?.direction} — {t.roles.join(", ")}
                {t.valid && " ✓"}
                {!t.inWindow && " (outside the reply window)"}
              </li>
            ))}
          </ul>
          {q.data.gaps.map((g, i) => (
            <p key={i} className="text-warn">
              Gap: {g.reason}{g.packId !== undefined && `, pack ${g.packId}`}{g.seq !== undefined && `, seq ${String(g.seq)}`}{g.detail && `: ${g.detail}`}
            </p>
          ))}
          {q.data.conflicts.map((c) => (
            <p key={String(c.seq)} className="text-destructive">
              Conflict at seq {String(c.seq)}: {c.versions.map((v) => v.packIds.join(" + ")).join(" vs ")}
            </p>
          ))}
          {q.data.unreadable.map((u) => (
            <p key={u.packId} className="text-destructive">Unreadable pack {u.packId}: {u.error}</p>
          ))}
        </>
      )}
    </section>
  );
}
