import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useState } from "react";

import { useMachine } from "@/api";
import { can } from "@/auth";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ErrorNote } from "@/errors";
import type { ExportJob, Selector } from "@/gen/tracekeep/v1/machine_pb";
import { useMe } from "@/me";
import { selectorOf, windowOf, type SelectorSearch } from "@/selector";
import { Time } from "@/time";

// ended are the states a job leaves no more; expired is a done job too near its deadline for a URL.
const ended = new Set(["done", "failed", "cancelled", "expired"]);

// download opens a bundle's URL; tests replace it, since jsdom navigates nowhere.
export const download = { open: (url: string) => { window.location.assign(url); } };

const size = (n: bigint | undefined) => {
  if (n === undefined) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = Number(n);
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u++;
  }
  return `${u === 0 ? String(v) : v.toFixed(1)} ${units[u] ?? "B"}`;
};

function StateBadge({ state }: { state: string }) {
  const variant = state === "done" ? "ok" : state === "failed" || state === "expired" ? "destructive" : state === "cancelled" ? "outline" : "secondary";
  return <Badge variant={variant}>{state}</Badge>;
}

// SelectorText says what a selector selects and over which window.
function SelectorText({ sel }: { sel: Selector | undefined }) {
  if (!sel) return <span className="text-muted-foreground">—</span>;
  return (
    <span>
      {sel.toolId} · {sel.devices.length > 0 ? sel.devices.join(", ") : "every device"}
      {sel.captureId !== undefined && <> · capture <span className="font-mono text-xs">{sel.captureId}</span></>}
      <br />
      <Time ts={sel.from} /> – <Time ts={sel.to} />
    </span>
  );
}

// ExportLink starts a new export of a page's selector and window, for a principal whose roles admit exports.
export function ExportLink({ search, from, to }: { search: SelectorSearch; from: Date; to: Date }) {
  const me = useMe();
  if (!can.export(me) || search.tool === undefined) return null;
  return (
    <Link
      to="/exports/new"
      search={{ tool: search.tool, devices: search.devices, capture: search.capture, from: from.toISOString(), to: to.toISOString() }}
      className="text-sm text-primary underline-offset-4 hover:underline"
    >
      Export this selection…
    </Link>
  );
}

// Exports lists the principal's jobs, newest first.
export function Exports() {
  const machine = useMachine();
  const q = useQuery({ queryKey: ["exports"], queryFn: async ({ signal }) => (await machine.listExports({}, { signal })).jobs });
  if (q.isPending) return <p className="text-muted-foreground">Loading…</p>;
  if (q.isError) return <ErrorNote error={q.error} />;
  if (q.data.length === 0) return <p className="text-muted-foreground">No export yet; start one from the browser or the records.</p>;
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Job</TableHead>
          <TableHead>State</TableHead>
          <TableHead>Selection</TableHead>
          <TableHead>Created</TableHead>
          <TableHead>Ended</TableHead>
          <TableHead>Size</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {q.data.map((j) => (
          <TableRow key={j.jobId}>
            <TableCell>
              <Link to="/exports/$jobId" params={{ jobId: j.jobId }} className="font-mono text-xs text-primary underline-offset-4 hover:underline">
                {j.jobId}
              </Link>
            </TableCell>
            <TableCell><StateBadge state={j.state} /></TableCell>
            <TableCell><SelectorText sel={j.selector} /></TableCell>
            <TableCell><Time ts={j.created} /></TableCell>
            <TableCell><Time ts={j.ended} /></TableCell>
            <TableCell>{size(j.size)}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

// NewExport confirms the selector and window a page passed before StartExport, kind canonical.
export function NewExport() {
  const search = useSearch({ from: "/exports/new" });
  const machine = useMachine();
  const navigate = useNavigate();
  const client = useQueryClient();
  // A preset is resolved once, so the job exports the window confirmed.
  const [now] = useState(() => Date.now());
  const sel = search.tool === undefined ? undefined : selectorOf(search, windowOf(search, now));
  const start = useMutation({
    mutationFn: async (s: Selector) => (await machine.startExport({ selector: s, kind: "canonical" })).jobId,
    onSuccess: (jobId) => {
      void client.invalidateQueries({ queryKey: ["exports"] });
      void navigate({ to: "/exports/$jobId", params: { jobId } });
    },
  });
  if (sel === undefined) return <p className="text-muted-foreground">Start an export from the browser or the records, which pass it their selection.</p>;
  return (
    <div className="max-w-xl space-y-4">
      <h2 className="text-base font-semibold">Export</h2>
      <p className="text-sm">
        <SelectorText sel={sel} />
      </p>
      <p className="text-sm text-muted-foreground">The bundle holds the masked packs of every selected capture's hours in the window, with their manifest.</p>
      {start.isError && <ErrorNote error={start.error} />}
      <Button disabled={start.isPending} onClick={() => { start.mutate(sel); }}>
        Start export
      </Button>
    </div>
  );
}

// ExportPage follows one job, asking GetExport every 2 s until it ends.
export function ExportPage() {
  const { jobId } = useParams({ from: "/exports/$jobId" });
  const machine = useMachine();
  const client = useQueryClient();
  const key = ["exports", jobId];
  const q = useQuery({
    queryKey: key,
    queryFn: async ({ signal }) => await machine.getExport({ jobId }, { signal }),
    refetchInterval: (query) => (query.state.data && ended.has(query.state.data.state) ? false : 2000),
  });
  const cancel = useMutation({
    mutationFn: async () => await machine.cancelExport({ jobId }),
    onSettled: () => client.invalidateQueries({ queryKey: key }),
  });
  // The download asks again, so no URL that expired while the page stayed open is offered;
  // an answer in another state, expired say, replaces the page's.
  const fetchURL = useMutation({
    mutationFn: async () => await machine.getExport({ jobId }),
    onSuccess: (j) => {
      client.setQueryData(key, j);
      if (j.state === "done" && j.url !== "") download.open(j.url);
    },
  });
  if (q.isPending) return <p className="text-muted-foreground">Loading…</p>;
  if (q.isError) return <ErrorNote error={q.error} />;
  const j: ExportJob = q.data;
  return (
    <div className="max-w-2xl space-y-4">
      <h2 className="text-base font-semibold">
        Export <span className="font-mono text-sm">{j.jobId}</span> <StateBadge state={j.state} />
      </h2>
      <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">
        <dt className="text-muted-foreground">Selection</dt>
        <dd><SelectorText sel={j.selector} /></dd>
        <dt className="text-muted-foreground">Created</dt>
        <dd><Time ts={j.created} /></dd>
        <dt className="text-muted-foreground">Ended</dt>
        <dd><Time ts={j.ended} /></dd>
        <dt className="text-muted-foreground">Attempts</dt>
        <dd>{String(j.attempts)}</dd>
        <dt className="text-muted-foreground">Size</dt>
        <dd>{size(j.size)}</dd>
        {j.deadline && (
          <>
            <dt className="text-muted-foreground">Kept until</dt>
            <dd><Time ts={j.deadline} /></dd>
          </>
        )}
        {j.reason !== "" && (
          <>
            <dt className="text-muted-foreground">Reason</dt>
            <dd>{j.reason}</dd>
          </>
        )}
      </dl>
      {!ended.has(j.state) && (
        <Button variant="outline" disabled={cancel.isPending} onClick={() => { cancel.mutate(); }}>
          Cancel
        </Button>
      )}
      {cancel.isError && <ErrorNote error={cancel.error} />}
      {j.state === "done" && (
        <Button disabled={fetchURL.isPending} onClick={() => { fetchURL.mutate(); }}>
          Download
        </Button>
      )}
      {fetchURL.isError && <ErrorNote error={fetchURL.error} />}
      {j.state === "expired" && <p className="text-sm text-destructive">The bundle can no longer be downloaded.</p>}
    </div>
  );
}
