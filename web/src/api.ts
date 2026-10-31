import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

import { MachineService } from "@/gen/tracekeep/v1/machine_pb";

// The machine API on this origin, by the Connect protocol with JSON (api.md §10.3);
// the browser sends the session cookie and the Origin header the API checks.
export const transport = createConnectTransport({ baseUrl: window.location.origin, useBinaryFormat: false });

export const machine = createClient(MachineService, transport);
