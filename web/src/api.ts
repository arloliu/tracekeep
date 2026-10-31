import { createClient, type Client } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { createContext, useContext } from "react";

import { MachineService } from "@/gen/tracekeep/v1/machine_pb";

// The machine API on this origin, by the Connect protocol with JSON (api.md §10.3);
// the browser sends the session cookie and the Origin header the API checks.
export const transport = createConnectTransport({ baseUrl: window.location.origin, useBinaryFormat: false });

export type Machine = Client<typeof MachineService>;

// MachineContext holds the pages' client: this origin's, or a test's over a router transport.
export const MachineContext = createContext<Machine>(createClient(MachineService, transport));

export function useMachine(): Machine {
  return useContext(MachineContext);
}
