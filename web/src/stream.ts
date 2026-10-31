import type { MessageInitShape } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { useMachine } from "@/api";
import { whenUnauthenticated } from "@/auth";
import type { QueryEnd, QueryRecordsRequestSchema, QueryRecordsResponse, RecordItem } from "@/gen/tracekeep/v1/machine_pb";

export type RecordsRequest = MessageInitShape<typeof QueryRecordsRequestSchema>;

// Row is a streamed item with the capture whose start it follows, which FindTransaction needs.
export interface Row {
  item: RecordItem;
  capture: string;
}

// Stream is a QueryRecords answer as it arrives, its pages joined into one list (api.md §10.3 *Pages*).
export interface Stream {
  rows: Row[];
  status: "streaming" | "ended" | "interrupted";
  // error is why an interrupted stream stopped.
  error?: ConnectError;
  // end is the last page's end; incomplete is set once any page's end says so.
  end?: QueryEnd;
  incomplete: boolean;
  // cursor is the last batch's, which an interrupted stream continues from; it never leaves the page.
  cursor: string;
  // run is what the stream reads now: the request and the cursor it started from.
  run: { req: RecordsRequest; cursor: string };
  capture: string;
}

function initial(req: RecordsRequest): Stream {
  return { rows: [], status: "streaming", incomplete: false, cursor: "", run: { req, cursor: "" }, capture: "" };
}

function append(s: Stream, res: QueryRecordsResponse): Stream {
  let capture = s.capture;
  const rows = res.items.map((item) => {
    if (item.item.case === "capture") capture = item.item.value.capture?.captureId ?? "";
    return { item, capture };
  });
  return {
    ...s,
    rows: rows.length > 0 ? s.rows.concat(rows) : s.rows,
    capture,
    cursor: res.cursor !== "" ? res.cursor : s.cursor,
    end: res.end ?? s.end,
    incomplete: s.incomplete || (res.end?.incomplete ?? false),
  };
}

// useRecordStream streams req into one list. A new req starts again; leaving the page or changing req cancels the stream,
// which is not a failure. An interrupted stream continues from its last batch's cursor, and a limit's end offers the next page.
export function useRecordStream(req: RecordsRequest) {
  const machine = useMachine();
  const client = useQueryClient();
  const [s, setS] = useState<Stream>(() => initial(req));
  if (s.run.req !== req) {
    // A changed request starts again; the render-time reset keeps no row of the old one on screen.
    setS(initial(req));
  }
  const run = s.run;
  useEffect(() => {
    const ctl = new AbortController();
    void (async () => {
      try {
        for await (const res of machine.queryRecords({ ...run.req, cursor: run.cursor }, { signal: ctl.signal })) {
          if (ctl.signal.aborted) return;
          setS((prev) => append(prev, res));
        }
        if (ctl.signal.aborted) return;
        setS((prev) =>
          prev.end ? { ...prev, status: "ended" } : { ...prev, status: "interrupted", error: new ConnectError("the stream stopped before its end", Code.Unavailable) },
        );
      } catch (e) {
        if (ctl.signal.aborted) return;
        whenUnauthenticated(client, e);
        setS((prev) => ({ ...prev, status: "interrupted", error: ConnectError.from(e) }));
      }
    })();
    return () => {
      ctl.abort();
    };
  }, [run, machine, client]);

  // more continues from cursor into the same list: an interrupted stream's last cursor, or a limit's end cursor.
  const more = (cursor: string) => {
    setS((prev) => ({ ...prev, status: "streaming", error: undefined, end: undefined, run: { req: prev.run.req, cursor } }));
  };
  return { stream: s, more };
}
