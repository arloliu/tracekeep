import { Code, ConnectError } from "@connectrpc/connect";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test } from "vitest";

import { whenUnauthenticated } from "@/auth";
import { formatTime } from "@/time";
import { json, renderApp, stubFetch } from "@/test/render";

beforeEach(() => {
  localStorage.clear();
});

test("signed out, the sign-in page starts a login returning to the page", async () => {
  stubFetch({ "/auth/me": () => new Response("not signed in", { status: 401 }) });
  renderApp("/records?tool=T-1");
  const link = await screen.findByRole("link", { name: "Sign in" });
  expect(link).toHaveAttribute("href", "/auth/login?return=%2Frecords%3Ftool%3DT-1");
});

test("a host without sign-in says so, with nothing to sign in to", async () => {
  stubFetch({ "/auth/me": () => new Response("not found", { status: 404 }) });
  renderApp("/");
  expect(await screen.findByText("People's sign-in is not configured on this host.")).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Sign in" })).not.toBeInTheDocument();
});

test("navigation shows what the roles admit", async () => {
  stubFetch({ "/auth/me": () => json({ subject: "bob", roles: ["exporter"], expiry: "2099-01-01T00:00:00Z" }) });
  renderApp("/");
  expect(await screen.findByRole("link", { name: "Exports" })).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Records" })).not.toBeInTheDocument();
  expect(screen.getByText("Your roles do not admit this page; it needs viewer.")).toBeInTheDocument();
});

test("sign-out posts to /auth/logout and shows the sign-in page", async () => {
  const calls = stubFetch({ "/auth/logout": () => new Response(null, { status: 204 }) });
  renderApp("/");
  await userEvent.click(await screen.findByRole("button", { name: "Sign out" }));
  expect(await screen.findByRole("link", { name: "Sign in" })).toBeInTheDocument();
  expect(calls).toContainEqual({ url: "/auth/logout", method: "POST" });
});

test("an operation answering unauthenticated returns to the sign-in page", async () => {
  let signedIn = true;
  stubFetch({ "/auth/me": () => (signedIn ? json({ subject: "alice", roles: ["viewer"], expiry: "2099-01-01T00:00:00Z" }) : new Response(null, { status: 401 })) });
  const { client } = renderApp("/");
  await screen.findByRole("button", { name: "Sign out" });
  signedIn = false;
  whenUnauthenticated(client, new ConnectError("session", Code.Unauthenticated));
  expect(await screen.findByRole("link", { name: "Sign in" })).toBeInTheDocument();
  whenUnauthenticated(client, new ConnectError("roles", Code.PermissionDenied)); // no effect beyond the page's own note
});

test("a session reaching its expiry returns to the sign-in page", async () => {
  let me = 0;
  stubFetch({
    "/auth/me": () =>
      ++me === 1 ? json({ subject: "alice", roles: ["viewer"], expiry: new Date(Date.now() + 50).toISOString() }) : new Response(null, { status: 401 }),
  });
  renderApp("/");
  await screen.findByRole("button", { name: "Sign out" });
  await waitFor(() => expect(screen.getByRole("link", { name: "Sign in" })).toBeInTheDocument());
});

test("the zone switch shows UTC and is remembered", async () => {
  stubFetch();
  renderApp("/");
  await userEvent.click(await screen.findByRole("button", { name: "Time zone" }));
  expect(screen.getByRole("button", { name: "Time zone" })).toHaveTextContent("UTC");
  expect(localStorage.getItem("tracekeep.zone")).toBe("utc");
  const d = new Date(Date.UTC(2026, 9, 31, 1, 2, 3, 45));
  expect(formatTime(d, "utc")).toBe("2026-10-31 01:02:03.045 UTC");
  expect(formatTime(d, "local")).toMatch(/^2026-10-3\d \d\d:02:03\.045 [+-]\d\d:\d\d$/);
});

test("a session far from its expiry is not asked again", async () => {
  const calls = stubFetch();
  renderApp("/");
  await screen.findByRole("button", { name: "Sign out" });
  await new Promise((r) => setTimeout(r, 50));
  expect(calls.filter((c) => c.url === "/auth/me")).toHaveLength(1);
});
