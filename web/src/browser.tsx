import type { Timestamp } from "@bufbuild/protobuf/wkt";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import { useState, type ReactNode, type SyntheticEvent } from "react";

import { useMachine } from "@/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ErrorNote } from "@/errors";
import type { Capture, Completeness, Hour, Thinning } from "@/gen/tracekeep/v1/machine_pb";
import { presetLabels, presets, recentTools, rememberTool, selectorOf, windowOf, type Preset, type SelectorSearch, type Window } from "@/selector";
import { fromInput, inputValue, Time, useZone } from "@/time";

const hour = 3_600_000;

// Browser is the browser page (api.md §10.3 *Pages*): a tool and a window, its devices,
// and the selected devices' captures with each capture's completeness hour by hour.
export function Browser() {
  const search = useSearch({ from: "/" });
  const client = useQueryClient();
  const { zone } = useZone();
  // A preset's window ends at the time the page opened it, so its queries keep their keys until it is opened again.
  const [now, setNow] = useState(() => Date.now());
  const w = windowOf(search, now);
  const reopen = () => {
    setNow(Date.now());
    void client.invalidateQueries({ queryKey: ["browser"] });
  };
  return (
    <div className="space-y-8">
      <ToolForm key={`${search.tool ?? ""}|${search.last ?? ""}|${search.from ?? ""}|${search.to ?? ""}|${zone}`} search={search} w={w} onOpen={reopen} />
      {search.tool === undefined ? (
        <p className="text-muted-foreground">Type a tool id to browse its devices.</p>
      ) : (
        <>
          <Devices search={search} w={w} />
          {search.devices === undefined ? (
            <p className="text-muted-foreground">Select devices to list their captures.</p>
          ) : (
            <>
              <Captures search={search} w={w} />
              <CompletenessByHour search={search} w={w} />
            </>
          )}
        </>
      )}
    </div>
  );
}

function ToolForm({ search, w, onOpen }: { search: SelectorSearch; w: Window; onOpen: () => void }) {
  const { zone } = useZone();
  const navigate = useNavigate({ from: "/" });
  const [tool, setTool] = useState(search.tool ?? "");
  const [last, setLast] = useState<Preset | "custom">(search.from !== undefined ? "custom" : (search.last ?? "1h"));
  const [from, setFrom] = useState(inputValue(w.from, zone));
  const [to, setTo] = useState(inputValue(w.to, zone));
  const [problem, setProblem] = useState("");
  const recent = recentTools();

  const open = (t: string, window: Pick<SelectorSearch, "last" | "from" | "to">) => {
    rememberTool(t);
    onOpen();
    void navigate({ search: { tool: t, devices: t === search.tool ? search.devices : undefined, ...window } });
  };
  const submit = (e: SyntheticEvent) => {
    e.preventDefault();
    const t = tool.trim();
    if (t === "") {
      setProblem("Type a tool id.");
      return;
    }
    if (last !== "custom") {
      open(t, { last });
      return;
    }
    const f = fromInput(from, zone);
    const u = fromInput(to, zone);
    if (f === undefined || u === undefined || f >= u) {
      setProblem("From must be a time before to.");
      return;
    }
    open(t, { from: f.toISOString(), to: u.toISOString() });
  };

  return (
    <form onSubmit={submit} className="space-y-3">
      <div className="flex flex-wrap items-end gap-3">
        <label className="grid gap-1 text-sm">
          Tool id
          <Input className="w-56" value={tool} onChange={(e) => { setTool(e.target.value); }} />
        </label>
        <div role="group" aria-label="Window" className="flex gap-1">
          {(Object.keys(presets) as Preset[]).map((p) => (
            <Button key={p} type="button" size="sm" variant={last === p ? "default" : "outline"} aria-pressed={last === p} onClick={() => { setLast(p); }}>
              {presetLabels[p]}
            </Button>
          ))}
        </div>
        <label className="grid gap-1 text-sm">
          From
          <Input type="datetime-local" className="w-52" value={from} onChange={(e) => { setFrom(e.target.value); setLast("custom"); }} />
        </label>
        <label className="grid gap-1 text-sm">
          To
          <Input type="datetime-local" className="w-52" value={to} onChange={(e) => { setTo(e.target.value); setLast("custom"); }} />
        </label>
        <Button type="submit">Open</Button>
      </div>
      {problem !== "" && <p role="alert" className="text-sm text-destructive">{problem}</p>}
      {recent.length > 0 && (
        <p className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
          Recent:
          {recent.map((t) => (
            <Button key={t} type="button" size="sm" variant="ghost" onClick={() => { open(t, { last: "1h" }); }}>
              {t}
            </Button>
          ))}
        </p>
      )}
    </form>
  );
}

// keyOf is a query's key from its selector's parts; a Timestamp holds a bigint, which a key cannot.
function keyOf(op: string, s: SelectorSearch, w: Window) {
  return ["browser", op, s.tool, s.devices, w.from.toISOString(), w.to.toISOString()];
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="space-y-2">
      <h2 className="text-base font-semibold">{title}</h2>
      {children}
    </section>
  );
}

function Flag({ on, label }: { on: boolean; label: string }) {
  return on ? <Badge variant="warn">{label}</Badge> : <span className="text-muted-foreground">—</span>;
}

function Devices({ search, w }: { search: SelectorSearch; w: Window }) {
  const machine = useMachine();
  const navigate = useNavigate({ from: "/" });
  const q = useQuery({
    queryKey: keyOf("devices", { tool: search.tool }, w),
    queryFn: async ({ signal }) => (await machine.listDevices({ selector: selectorOf({ tool: search.tool }, w) }, { signal })).devices,
  });
  const selected = new Set(search.devices);
  const toggle = (name: string) => {
    const next = selected.has(name) ? [...selected].filter((d) => d !== name) : [...selected, name];
    void navigate({ search: (prev) => ({ ...prev, devices: next.length > 0 ? next : undefined }) });
  };
  return (
    <Section title="Devices">
      {q.isPending ? (
        <p className="text-muted-foreground">Loading…</p>
      ) : q.isError ? (
        <ErrorNote error={q.error} />
      ) : q.data.length === 0 ? (
        <p className="text-muted-foreground">The tool has no device with a capture in the window.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead />
              <TableHead>Device</TableHead>
              <TableHead>Endpoint</TableHead>
              <TableHead>Device id</TableHead>
              <TableHead>Open captures</TableHead>
              <TableHead>Overlap</TableHead>
              <TableHead>Last record</TableHead>
              <TableHead>Gap</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {q.data.map((d) => (
              <TableRow key={d.name} data-state={selected.has(d.name) ? "selected" : undefined}>
                <TableCell>
                  <input type="checkbox" aria-label={`Select ${d.name}`} checked={selected.has(d.name)} onChange={() => { toggle(d.name); }} />
                </TableCell>
                <TableCell className="font-medium">{d.name}</TableCell>
                <TableCell>{d.equipmentEndpoint ?? "—"}</TableCell>
                <TableCell>{d.deviceId === undefined ? "—" : String(d.deviceId)}</TableCell>
                <TableCell>{String(d.openCaptures)}</TableCell>
                <TableCell><Flag on={d.overlap} label="overlap" /></TableCell>
                <TableCell><Time ts={d.lastRecord} /></TableCell>
                <TableCell><Flag on={d.gap} label="gap" /></TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </Section>
  );
}

// thinningText names a thinned stream and function with its sampling rule, or its source when that does not parse.
export function thinningText(t: Thinning): string {
  const sf = `S${String(t.stream)}F${String(t.function)}`;
  if (t.keepEveryN !== undefined) return `${sf} every ${String(t.keepEveryN)}`;
  if (t.keepIntervalMs !== undefined) return `${sf} every ${String(t.keepIntervalMs)} ms`;
  return `${sf} ${t.source}`;
}

function Interval({ start, end }: { start: Timestamp | undefined; end: Timestamp | undefined }) {
  return (
    <span>
      {start === undefined ? "open" : <Time ts={start} />} – <Time ts={end} />
    </span>
  );
}

function Captures({ search, w }: { search: SelectorSearch; w: Window }) {
  const machine = useMachine();
  const q = useQuery({
    queryKey: keyOf("captures", search, w),
    queryFn: async ({ signal }) => (await machine.listCaptures({ selector: selectorOf({ tool: search.tool, devices: search.devices }, w) }, { signal })).captures,
  });
  return (
    <Section title="Captures">
      {q.isPending ? (
        <p className="text-muted-foreground">Loading…</p>
      ) : q.isError ? (
        <ErrorNote error={q.error} />
      ) : q.data.length === 0 ? (
        <p className="text-muted-foreground">No capture of the selected devices meets the window.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Capture</TableHead>
              <TableHead>Device</TableHead>
              <TableHead>State</TableHead>
              <TableHead>Origin</TableHead>
              <TableHead>Last record</TableHead>
              <TableHead>Overlap</TableHead>
              <TableHead>Thinning</TableHead>
              <TableHead>Unregistered</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {q.data.map((c) => (
              <TableRow key={c.captureId}>
                <TableCell className="font-mono text-xs">{c.captureId}</TableCell>
                <TableCell>{c.device}</TableCell>
                <TableCell><Badge variant="secondary">{c.state}</Badge></TableCell>
                <TableCell><Time ts={c.origin} /></TableCell>
                <TableCell><Time ts={c.lastRecord} /></TableCell>
                <TableCell><Flag on={c.overlap} label="overlap" /></TableCell>
                <TableCell>{c.thinning.length === 0 ? "—" : c.thinning.map((t) => <div key={`${String(t.stream)}/${String(t.function)}`} title={t.source}>{thinningText(t)}</div>)}</TableCell>
                <TableCell>
                  {c.unregistered.length === 0 ? "—" : c.unregistered.map((u, i) => <div key={i}><Interval start={u.start} end={u.end} /></div>)}
                </TableCell>
                <TableCell>
                  <RecordsLink search={search} capture={c} from={w.from} to={w.to}>Records</RecordsLink>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </Section>
  );
}

// RecordsLink opens the records of the selected devices, or of one capture, between from and to.
function RecordsLink({ search, capture, from, to, children }: { search: SelectorSearch; capture?: Capture; from: Date; to: Date; children: ReactNode }) {
  return (
    <Link
      aria-label={capture ? `Records of ${capture.captureId}` : undefined}
      to="/records"
      search={{
        tool: search.tool,
        devices: capture ? [capture.device] : search.devices,
        capture: capture?.captureId,
        from: from.toISOString(),
        to: to.toISOString(),
      }}
      className="text-primary underline-offset-4 hover:underline"
    >
      {children}
    </Link>
  );
}

const seqs = (a: bigint | undefined, b: bigint | undefined) =>
  a === undefined || b === undefined ? "" : a === b ? `seq ${String(a)}` : `seqs ${String(a)}–${String(b)}`;

// facts are a capture's completeness as labelled lists, the empty ones left out.
function facts(c: Completeness): [string, ReactNode[]][] {
  return [
    ["Frontier", c.frontier === undefined ? [] : [`seq ${String(c.frontier)}`]],
    ["Retained", c.retained.map((r) => <>{seqs(r.seqFrom, r.seqTo)}, <Interval start={r.from} end={r.to} /></>)],
    ["Evicted", c.evicted.map((r) => <>{seqs(r.seqFrom, r.seqTo)}{r.from && <>, <Interval start={r.from} end={r.to} /></>}</>)],
    ["Deferred", c.deferred.map((d) => <>seq {String(d.seq)} at <Time ts={d.time} />, {d.state}</>)],
    ["Gaps", c.gaps.map((g) => <>{seqs(g.seqFrom, g.seqTo)}, {g.openStart ? "from the origin" : <Time ts={g.start} />} – <Time ts={g.end} /></>)],
    ["Declared losses", c.losses.map((l) => <>{seqs(l.seqFrom, l.seqTo) || "a pack's coverage"}{l.from && <>, <Interval start={l.from} end={l.to} /></>}{l.actionId !== "" && `, action ${l.actionId}`}</>)],
    ["Crash gaps", c.crashGaps.map((g) => <Interval start={g.start} end={g.end} />)],
    ["Conflicts", c.conflicts.map((x) => <>{seqs(x.seqFrom, x.seqTo)}, {x.state}</>)],
    ["Unknown tail", c.unknownTailFrom === undefined ? [] : [`from seq ${String(c.unknownTailFrom)}`]],
  ];
}

// cell is one capture's part of an hour: its scope, or its ranges retention evicted.
function cell(h: Hour, captureId: string): ReactNode {
  const s = h.scopes.find((x) => x.captureId === captureId);
  if (s) {
    const ranges = s.packs.flatMap((p) => p.ranges);
    const lo = ranges.reduce<bigint | undefined>((m, r) => (m === undefined || r.seqFrom < m ? r.seqFrom : m), undefined);
    const hi = ranges.reduce<bigint | undefined>((m, r) => (m === undefined || r.seqTo > m ? r.seqTo : m), undefined);
    return (
      <span>
        <Badge variant="secondary">{s.state}</Badge> {s.packs.length} {s.packs.length === 1 ? "pack" : "packs"}{lo !== undefined && `, ${seqs(lo, hi)}`}
      </span>
    );
  }
  if (h.evicted.includes(captureId)) return <Badge variant="warn">evicted</Badge>;
  return <span className="text-muted-foreground">—</span>;
}

// hourNotes are what an hour without scopes holds instead: parked seqs, declared losses, or no data.
function hourNotes(h: Hour): string {
  const notes: string[] = [];
  if (h.deferred.length > 0) notes.push(`${h.deferred.length} deferred`);
  if (h.losses.length > 0) notes.push(`${h.losses.length} declared ${h.losses.length === 1 ? "loss" : "losses"}`);
  if (h.noData) notes.push(h.spanned ? "no data within a capture's span" : "no data");
  return notes.join(", ");
}

function CompletenessByHour({ search, w }: { search: SelectorSearch; w: Window }) {
  const machine = useMachine();
  const q = useQuery({
    queryKey: keyOf("packs", search, w),
    queryFn: async ({ signal }) => await machine.listPacks({ selector: selectorOf({ tool: search.tool, devices: search.devices }, w) }, { signal }),
  });
  if (q.isPending) return <Section title="Completeness"><p className="text-muted-foreground">Loading…</p></Section>;
  if (q.isError) return <Section title="Completeness"><ErrorNote error={q.error} /></Section>;
  const caps = q.data.captures.flatMap((c) => (c.capture && c.completeness ? [{ capture: c.capture, completeness: c.completeness }] : []));
  return (
    <Section title="Completeness">
      {caps.map(({ capture, completeness }) => (
        <div key={capture.captureId} className="rounded-md border p-3 text-sm">
          <p className="mb-2 font-medium">
            <span className="font-mono text-xs">{capture.captureId}</span> · {capture.device}
          </p>
          <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
            {facts(completeness)
              .filter(([, items]) => items.length > 0)
              .map(([label, items]) => (
                <div key={label} className="contents">
                  <dt className="text-muted-foreground">{label}</dt>
                  <dd>{items.map((it, i) => <div key={i}>{it}</div>)}</dd>
                </div>
              ))}
          </dl>
        </div>
      ))}
      <Table aria-label="Hours">
        <TableHeader>
          <TableRow>
            <TableHead>Hour</TableHead>
            {caps.map(({ capture }) => (
              <TableHead key={capture.captureId} className="font-mono text-xs" title={capture.captureId}>
                {capture.captureId.slice(0, 8)} · {capture.device}
              </TableHead>
            ))}
            <TableHead>Notes</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {q.data.hours.map((h) => {
            const start = h.hour ? timestampDate(h.hour) : undefined;
            return (
              <TableRow key={start?.toISOString()}>
                <TableCell>
                  {start ? (
                    <RecordsLink search={search} from={start} to={new Date(start.getTime() + hour)}>
                      <Time ts={h.hour} />
                    </RecordsLink>
                  ) : "—"}
                </TableCell>
                {caps.map(({ capture }) => <TableCell key={capture.captureId}>{cell(h, capture.captureId)}</TableCell>)}
                <TableCell>{hourNotes(h)}</TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Section>
  );
}
