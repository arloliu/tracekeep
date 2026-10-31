import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider, createMemoryHistory } from "@tanstack/react-router";
import { render } from "@testing-library/react";
import { vi } from "vitest";

import type { Me } from "@/auth";
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

// renderApp renders the whole application at path.
export function renderApp(path: string) {
  const client = newQueryClient();
  const router = newRouter(createMemoryHistory({ initialEntries: [path] }));
  const view = render(
    <QueryClientProvider client={client}>
      <ZoneProvider>
        <RouterProvider router={router} />
      </ZoneProvider>
    </QueryClientProvider>,
  );
  return { ...view, client, router };
}
