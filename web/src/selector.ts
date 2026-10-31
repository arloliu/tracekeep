import { create } from "@bufbuild/protobuf";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";

import { SelectorSchema, type Selector } from "@/gen/tracekeep/v1/machine_pb";

// presets are the browser's windows ending now (api.md §10.3 *Pages*).
export const presets = { "1h": 3_600_000, "6h": 6 * 3_600_000, "24h": 24 * 3_600_000, "7d": 7 * 24 * 3_600_000 } as const;
export type Preset = keyof typeof presets;
export const presetLabels: Record<Preset, string> = { "1h": "Last hour", "6h": "6 h", "24h": "24 h", "7d": "7 days" };

// SelectorSearch is a page's selector and window in its URL: a preset ending now, or from and to as ISO times.
export interface SelectorSearch {
  tool?: string;
  devices?: string[];
  capture?: string;
  last?: Preset;
  from?: string;
  to?: string;
}

export interface Window {
  from: Date;
  to: Date;
}

// scalar reads a search value the router may have parsed as JSON (a tool named 123 arrives as a number).
function scalar(v: unknown): string | undefined {
  if (typeof v === "string") return v === "" ? undefined : v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return undefined;
}

function time(v: unknown): string | undefined {
  const s = scalar(v);
  return s !== undefined && !Number.isNaN(Date.parse(s)) ? new Date(s).toISOString() : undefined;
}

// validateSelector keeps the search parameters a selector understands and drops the rest.
export function validateSelector(search: Record<string, unknown>): SelectorSearch {
  const out: SelectorSearch = {};
  const tool = scalar(search.tool);
  if (tool !== undefined) out.tool = tool;
  const devices = Array.isArray(search.devices) ? search.devices : search.devices === undefined ? [] : [search.devices];
  const names = devices.map(scalar).filter((d): d is string => d !== undefined);
  if (names.length > 0) out.devices = [...new Set(names)];
  const capture = scalar(search.capture);
  if (capture !== undefined) out.capture = capture;
  const from = time(search.from);
  const to = time(search.to);
  if (from !== undefined && to !== undefined && Date.parse(from) < Date.parse(to)) {
    out.from = from;
    out.to = to;
  } else {
    const last = scalar(search.last);
    if (last !== undefined && last in presets) out.last = last as Preset;
  }
  return out;
}

// windowOf is the search's window: its from and to, or its preset (the last hour by default) ending at now.
export function windowOf(s: SelectorSearch, now: number): Window {
  if (s.from !== undefined && s.to !== undefined) return { from: new Date(s.from), to: new Date(s.to) };
  return { from: new Date(now - presets[s.last ?? "1h"]), to: new Date(now) };
}

// selectorOf is the operations' selector for s over w.
export function selectorOf(s: SelectorSearch, w: Window): Selector {
  return create(SelectorSchema, {
    toolId: s.tool ?? "",
    devices: s.devices ?? [],
    captureId: s.capture,
    from: timestampFromDate(w.from),
    to: timestampFromDate(w.to),
  });
}

const recentKey = "tracekeep.tools";
const recentMax = 8;

// recentTools are the tools the browser opened last, newest first; no operation lists tools.
export function recentTools(): string[] {
  try {
    const v: unknown = JSON.parse(localStorage.getItem(recentKey) ?? "[]");
    return Array.isArray(v) ? v.filter((t): t is string => typeof t === "string").slice(0, recentMax) : [];
  } catch {
    return [];
  }
}

export function rememberTool(tool: string) {
  localStorage.setItem(recentKey, JSON.stringify([tool, ...recentTools().filter((t) => t !== tool)].slice(0, recentMax)));
}

export const kinds = ["data", "control", "transport-event", "annotation"] as const;
export const directions = ["to_host", "to_equipment"] as const;

// RecordsSearch adds QueryRecords' predicates to the selector (api.md §10.1 *QueryRecords*);
// seqs stay decimal strings, since a uint64 above 2^53 is no JSON number.
export interface RecordsSearch extends SelectorSearch {
  messages?: string[];
  dir?: (typeof directions)[number];
  kinds?: (typeof kinds)[number][];
  events?: boolean;
  seqFrom?: string;
  seqTo?: string;
}

const list = (v: unknown) => (Array.isArray(v) ? v : v === undefined ? [] : [v]).map(scalar).filter((s): s is string => s !== undefined);
const seq = (v: unknown) => {
  const s = scalar(v);
  return s !== undefined && /^\d{1,20}$/.test(s) && BigInt(s) < 2n ** 64n ? BigInt(s).toString() : undefined;
};

// validateRecords keeps the selector and the predicates the records page understands; seqs only with a capture, as the server asks.
export function validateRecords(search: Record<string, unknown>): RecordsSearch {
  const out: RecordsSearch = validateSelector(search);
  const messages = list(search.messages);
  if (messages.length > 0) out.messages = messages;
  const dir = scalar(search.dir);
  if (directions.some((d) => d === dir)) out.dir = dir as RecordsSearch["dir"];
  const ks = list(search.kinds).filter((k): k is (typeof kinds)[number] => kinds.some((x) => x === k));
  if (ks.length > 0) out.kinds = [...new Set(ks)];
  if (search.events === true || search.events === "true") out.events = true;
  if (out.capture !== undefined) {
    const from = seq(search.seqFrom);
    const to = seq(search.seqTo);
    if (from !== undefined) out.seqFrom = from;
    if (to !== undefined) out.seqTo = to;
  }
  return out;
}
