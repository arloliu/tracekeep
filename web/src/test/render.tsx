import { createClient, createRouterTransport, type ConnectRouter } from "@connectrpc/connect";
import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider, createMemoryHistory } from "@tanstack/react-router";
import { render } from "@testing-library/react";
import { vi } from "vitest";

import { MachineContext } from "@/api";
import type { Me } from "@/auth";
import { MachineService } from "@/gen/tracekeep/v1/machine_pb";
import { newQueryClient } from "@/query";
import { newRouter } from "@/router";
import { ZoneProvider } from "@/time";

export const viewer: Me = { subject: "alice", roles: ["viewer"], expiry: "2099-01-01T00:00:00Z" };

// Answers are what the stubbed fetch answers per path; /auth/me defaults to the viewer.
export type Answers = Record<string, () => Response>;

export function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

// stubFetch answers the pages' own requests (/auth/) without a network, and records them.
export function stubFetch(answers: Answers = {}) {
  const calls: { url: string; method: string }[] = [];
  const fetch = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.pathname : input.url;
    calls.push({ url, method: init?.method ?? "GET" });
    const path = url.split("?")[0] ?? url;
    const answer = answers[path] ?? (path === "/auth/me" ? () => json(viewer) : () => new Response("not found", { status: 404 }));
    return Promise.resolve(answer());
  });
  vi.stubGlobal("fetch", fetch);
  return calls;
}

// renderApp renders the whole application at path, its machine API answered by routes over a router transport.
export function renderApp(path: string, routes: (r: ConnectRouter) => void = () => undefined) {
  const client = newQueryClient();
  const router = newRouter(createMemoryHistory({ initialEntries: [path] }));
  const machine = createClient(MachineService, createRouterTransport(routes));
  const view = render(
    <QueryClientProvider client={client}>
      <MachineContext value={machine}>
        <ZoneProvider>
          <RouterProvider router={router} />
        </ZoneProvider>
      </MachineContext>
    </QueryClientProvider>,
  );
  return { ...view, client, router };
}
