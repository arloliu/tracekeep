import type { MessageInitShape } from "@bufbuild/protobuf";
import { timestampFromDate, type Timestamp } from "@bufbuild/protobuf/wkt";
import type { ConnectRouter, ServiceImpl } from "@connectrpc/connect";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { download } from "@/exports";
import { MachineService, type ExportJobSchema, type GetExportRequest, type StartExportRequest } from "@/gen/tracekeep/v1/machine_pb";
import { json, renderApp, stubFetch } from "@/test/render";

type Job = MessageInitShape<typeof ExportJobSchema>;

const jobID = "0192f1a0-0000-7000-8000-00000000000a";
const ts = (iso: string) => timestampFromDate(new Date(iso));
const selector = { toolId: "T-1", devices: ["eq-a"], from: ts("2026-10-31T08:00:00Z"), to: ts("2026-10-31T09:00:00Z") };
const job = (state: string, extra: { jobId?: string; size?: bigint; ended?: Timestamp; url?: string; reason?: string } = {}): Job => ({ jobId: jobID, state, kind: "canonical", selector, created: ts("2026-10-31T09:01:00Z"), ...extra });

const both = { subject: "alice", roles: ["viewer", "exporter"], expiry: "2099-01-01T00:00:00Z" };

beforeEach(() => {
  localStorage.clear();
  localStorage.setItem("tracekeep.zone", "utc");
  stubFetch({ "/auth/me": () => json(both) });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

// answers serves GetExport from the states given in turn, the last repeated, recording each request.
function answers(asked: GetExportRequest[], jobs: Job[], also: Partial<ServiceImpl<typeof MachineService>> = {}) {
  return (r: ConnectRouter) => {
    r.service(MachineService, {
      ...also,
      getExport(req) {
        asked.push(req);
        return jobs[Math.min(asked.length - 1, jobs.length - 1)] ?? {};
      },
      cancelExport() {
        return {};
      },
    });
  };
}

test("the list shows the principal's jobs with their state, selection and size", async () => {
  renderApp("/exports", (r) =>
    r.service(MachineService, {
      listExports() {
        return { jobs: [job("done", { size: 1572864n, ended: ts("2026-10-31T09:02:00Z") }), job("failed", { jobId: "j-2", reason: "the store failed" })] };
      },
    }),
  );
  const link = await screen.findByRole("link", { name: jobID });
  expect(link).toHaveAttribute("href", `/exports/${jobID}`);
  expect(screen.getByText("1.5 MiB")).toBeInTheDocument();
  expect(screen.getByText("failed")).toBeInTheDocument();
  expect(screen.getAllByText(/T-1 · eq-a/)).toHaveLength(2);
  expect(screen.getAllByText("2026-10-31 08:00:00.000 UTC")).toHaveLength(2);
});

test("a new job starts from the browser's selection, confirmed before StartExport, kind canonical", async () => {
  const started: StartExportRequest[] = [];
  const { router } = renderApp(
    "/?tool=T-1&devices=%5B%22eq-a%22%5D&from=2026-10-31T08%3A00%3A00.000Z&to=2026-10-31T09%3A00%3A00.000Z",
    answers([], [job("queued")], {
      listDevices: () => ({ devices: [] }),
      listCaptures: () => ({ captures: [] }),
      listPacks: () => ({}),
      startExport(req) {
        started.push(req);
        return { jobId: jobID };
      },
    }),
  );
  await userEvent.click(await screen.findByRole("link", { name: "Export this selection…" }));
  expect(await screen.findByText(/T-1 · eq-a/)).toBeInTheDocument();
  expect(started).toHaveLength(0);
  await userEvent.click(screen.getByRole("button", { name: "Start export" }));
  await waitFor(() => { expect(router.state.location.pathname).toBe(`/exports/${jobID}`); });
  expect(started[0]?.kind).toBe("canonical");
  expect(started[0]?.selector?.toolId).toBe("T-1");
  expect(started[0]?.selector?.devices).toEqual(["eq-a"]);
  expect(started[0]?.selector?.from?.seconds).toBe(selector.from.seconds);
  expect(started[0]?.selector?.to?.seconds).toBe(selector.to.seconds);
});

test("without the exporter role the pages offer no export", async () => {
  stubFetch({ "/auth/me": () => json({ ...both, roles: ["viewer"] }) });
  renderApp("/?tool=T-1", (r) => r.service(MachineService, { listDevices: () => ({ devices: [] }) }));
  await screen.findByText("The tool has no device with a capture in the window.");
  expect(screen.queryByRole("link", { name: "Export this selection…" })).not.toBeInTheDocument();
});

test("a job's page polls every 2 s until the job ends", async () => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  const asked: GetExportRequest[] = [];
  renderApp(`/exports/${jobID}`, answers(asked, [job("queued"), job("running"), job("done", { size: 10n })]));
  expect(await screen.findByText("queued")).toBeInTheDocument();
  await vi.advanceTimersByTimeAsync(2000);
  expect(await screen.findByText("running")).toBeInTheDocument();
  await vi.advanceTimersByTimeAsync(2000);
  expect(await screen.findByText("done")).toBeInTheDocument();
  const n = asked.length;
  await vi.advanceTimersByTimeAsync(6000);
  expect(asked).toHaveLength(n);
  expect(asked[0]?.jobId).toBe(jobID);
});

test("a job not ended is cancelled by CancelExport", async () => {
  const cancelled: string[] = [];
  let state = "running";
  renderApp(`/exports/${jobID}`, (r) =>
    r.service(MachineService, {
      getExport: () => job(state),
      cancelExport(req) {
        cancelled.push(req.jobId);
        state = "cancelled";
        return {};
      },
    }),
  );
  await userEvent.click(await screen.findByRole("button", { name: "Cancel" }));
  expect(await screen.findByText("cancelled")).toBeInTheDocument();
  expect(cancelled).toEqual([jobID]);
  expect(screen.queryByRole("button", { name: "Cancel" })).not.toBeInTheDocument();
});

test("the download asks GetExport again and opens the URL that answer carries", async () => {
  const open = vi.spyOn(download, "open").mockImplementation(() => undefined);
  const asked: GetExportRequest[] = [];
  renderApp(`/exports/${jobID}`, answers(asked, [job("done", { url: "https://h/old" }), job("done", { url: "https://h/fresh" })]));
  await userEvent.click(await screen.findByRole("button", { name: "Download" }));
  await waitFor(() => { expect(open).toHaveBeenCalledWith("https://h/fresh"); });
  expect(asked).toHaveLength(2);
});

test("a bundle that expired while the page stayed open says it can no longer be downloaded", async () => {
  const open = vi.spyOn(download, "open").mockImplementation(() => undefined);
  renderApp(`/exports/${jobID}`, answers([], [job("done", { url: "https://h/old" }), job("expired")]));
  await userEvent.click(await screen.findByRole("button", { name: "Download" }));
  expect(await screen.findByText("The bundle can no longer be downloaded.")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Download" })).not.toBeInTheDocument();
  expect(open).not.toHaveBeenCalled();
});
