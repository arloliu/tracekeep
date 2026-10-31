import { Code, ConnectError } from "@connectrpc/connect";
import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { useEffect } from "react";

// Me is the signed-in principal GET /auth/me answers (api.md §10.2).
export interface Me {
  subject: string;
  roles: string[];
  expiry: string;
}

// Sign-in is signed in, signed out (401), or not configured on this host (404).
export type SignIn = { state: "signed-in"; me: Me } | { state: "signed-out" } | { state: "unconfigured" };

export const meKey = ["auth", "me"] as const;

// maxDelay is setTimeout's longest delay.
const maxDelay = 2 ** 31 - 1;

async function fetchMe(): Promise<SignIn> {
  const resp = await fetch("/auth/me", { credentials: "same-origin", headers: { Accept: "application/json" } });
  if (resp.status === 401) return { state: "signed-out" };
  if (resp.status === 404) return { state: "unconfigured" };
  if (!resp.ok) throw new Error(`/auth/me answered ${resp.status}`);
  // A session no role maps answers roles as null: signed in, admitted to nothing.
  const me = (await resp.json()) as { subject?: unknown; roles?: unknown; expiry?: unknown };
  return {
    state: "signed-in",
    me: {
      subject: typeof me.subject === "string" ? me.subject : "",
      roles: Array.isArray(me.roles) ? me.roles.filter((r): r is string => typeof r === "string") : [],
      expiry: typeof me.expiry === "string" ? me.expiry : "",
    },
  };
}

// useSignIn is the page's sign-in, asked once and again when the session ends.
export function useSignIn() {
  const client = useQueryClient();
  const query = useQuery({ queryKey: meKey, queryFn: fetchMe, staleTime: Infinity, retry: false });
  const expiry = query.data?.state === "signed-in" ? Date.parse(query.data.me.expiry) : NaN;
  useEffect(() => {
    if (Number.isNaN(expiry)) return;
    // A session past its expiry is refused; the page returns to the sign-in page then rather than at its next failure.
    // A delay past setTimeout's 32-bit bound would fire at once; a later one is asked again at the bound, and re-armed.
    const delay = Math.min(Math.max(0, expiry - Date.now()), maxDelay);
    const timer = setTimeout(() => void client.invalidateQueries({ queryKey: meKey }), delay);
    return () => {
      clearTimeout(timer);
    };
  }, [client, expiry]);
  return query;
}

// loginURL starts a login that returns to path, the current path and query.
export function loginURL(path: string): string {
  return `/auth/login?return=${encodeURIComponent(path)}`;
}

// signOut clears the session by a same-origin POST, then shows the sign-in page;
// any other answer, from the API or a proxy before it, leaves the session, and the page, as they were.
export async function signOut(client: QueryClient) {
  const resp = await fetch("/auth/logout", { method: "POST", credentials: "same-origin" });
  if (!resp.ok) throw new Error(`Sign-out failed (${String(resp.status)}); you are still signed in.`);
  client.setQueryData(meKey, { state: "signed-out" } satisfies SignIn);
}

// whenUnauthenticated asks /auth/me again after an operation answered unauthenticated,
// such as a session that expired while the page was open, so the page returns to the sign-in page.
export function whenUnauthenticated(client: QueryClient, error: unknown) {
  if (ConnectError.from(error).code === Code.Unauthenticated) {
    void client.invalidateQueries({ queryKey: meKey });
  }
}

export const can = {
  view: (me: Me) => me.roles.includes("viewer"),
  export: (me: Me) => me.roles.includes("exporter"),
};
