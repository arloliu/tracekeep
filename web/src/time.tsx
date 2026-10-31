import { timestampDate, type Timestamp } from "@bufbuild/protobuf/wkt";
import { createContext, useContext, useState, type ReactNode } from "react";

export type Zone = "local" | "utc";

const storageKey = "tracekeep.zone";

const ZoneContext = createContext<{ zone: Zone; setZone: (z: Zone) => void }>({ zone: "local", setZone: () => undefined });

// ZoneProvider keeps the zone times are shown in, the browser's or UTC, as the browser remembers it.
export function ZoneProvider({ children }: { children: ReactNode }) {
  const [zone, set] = useState<Zone>(() => (localStorage.getItem(storageKey) === "utc" ? "utc" : "local"));
  const setZone = (z: Zone) => {
    localStorage.setItem(storageKey, z);
    set(z);
  };
  return <ZoneContext value={{ zone, setZone }}>{children}</ZoneContext>;
}

export function useZone() {
  return useContext(ZoneContext);
}

const pad = (n: number, w = 2) => String(n).padStart(w, "0");

// formatTime shows d in zone with its offset, to the millisecond: 2026-10-31 09:15:02.123 +08:00.
export function formatTime(d: Date, zone: Zone): string {
  if (zone === "utc") {
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.${pad(d.getUTCMilliseconds(), 3)} UTC`;
  }
  const off = -d.getTimezoneOffset();
  const sign = off >= 0 ? "+" : "-";
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)} ${sign}${pad(Math.floor(Math.abs(off) / 60))}:${pad(Math.abs(off) % 60)}`;
}

// Time shows a timestamp in the chosen zone, its ISO form in UTC as its title.
export function Time({ ts }: { ts: Timestamp | undefined }) {
  const { zone } = useZone();
  if (!ts) return <span className="text-muted-foreground">—</span>;
  const d = timestampDate(ts);
  return (
    <time dateTime={d.toISOString()} title={d.toISOString()}>
      {formatTime(d, zone)}
    </time>
  );
}

// inputValue is d as a datetime-local input shows it in zone, to the minute.
export function inputValue(d: Date, zone: Zone): string {
  if (zone === "utc") return d.toISOString().slice(0, 16);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

// fromInput is a datetime-local input's value read in zone, undefined when it names no time.
export function fromInput(v: string, zone: Zone): Date | undefined {
  if (v === "") return undefined;
  const d = new Date(zone === "utc" ? `${v}Z` : v);
  return Number.isNaN(d.getTime()) ? undefined : d;
}
