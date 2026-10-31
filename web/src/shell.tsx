import { useQueryClient } from "@tanstack/react-query";
import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import type { ReactNode } from "react";

import { can, loginURL, signOut, useSignIn, type Me } from "@/auth";
import { Button } from "@/components/ui/button";
import { ErrorNote } from "@/errors";
import { MeProvider } from "@/me";
import { useZone } from "@/time";

// Shell asks who is signed in and shows the sign-in page, the not-configured page, or the pages the roles admit.
export function Shell() {
  const signIn = useSignIn();
  const location = useRouterState({ select: (s) => s.location });
  if (signIn.isPending) return <Centered>Loading…</Centered>;
  if (signIn.isError) {
    return (
      <Centered>
        <ErrorNote error={signIn.error} />
      </Centered>
    );
  }
  const s = signIn.data;
  if (s.state === "unconfigured") {
    return (
      <Centered>
        <h1 className="text-xl font-semibold">tracekeep</h1>
        <p className="text-muted-foreground">People's sign-in is not configured on this host.</p>
      </Centered>
    );
  }
  if (s.state === "signed-out") {
    return (
      <Centered>
        <h1 className="text-xl font-semibold">tracekeep</h1>
        <p className="text-muted-foreground">Sign in to browse the traffic logs.</p>
        <Button asChild>
          <a href={loginURL(location.href)}>Sign in</a>
        </Button>
      </Centered>
    );
  }
  return (
    <MeProvider value={s.me}>
      <Layout me={s.me} />
    </MeProvider>
  );
}

function Layout({ me }: { me: Me }) {
  const client = useQueryClient();
  const { zone, setZone } = useZone();
  const nav = "text-muted-foreground hover:text-foreground [&.active]:text-foreground [&.active]:font-medium";
  return (
    <div className="flex min-h-screen flex-col">
      <header className="flex items-center gap-6 border-b px-6 py-3 text-sm">
        <Link to="/" className="font-semibold">
          tracekeep
        </Link>
        <nav className="flex gap-4">
          {can.view(me) && (
            <>
              <Link to="/" className={nav} activeOptions={{ exact: true }}>
                Browser
              </Link>
              <Link to="/records" className={nav}>
                Records
              </Link>
            </>
          )}
          {can.export(me) && (
            <Link to="/exports" className={nav}>
              Exports
            </Link>
          )}
        </nav>
        <div className="ml-auto flex items-center gap-3">
          <Button
            variant="ghost"
            size="sm"
            aria-label="Time zone"
            onClick={() => {
              setZone(zone === "utc" ? "local" : "utc");
            }}
          >
            {zone === "utc" ? "UTC" : "Local time"}
          </Button>
          <span className="text-muted-foreground" title={me.roles.join(", ")}>
            {me.subject}
          </span>
          <Button variant="outline" size="sm" onClick={() => void signOut(client)}>
            Sign out
          </Button>
        </div>
      </header>
      <main className="flex-1 p-6">
        <Outlet />
      </main>
    </div>
  );
}

function Centered({ children }: { children: ReactNode }) {
  return <div className="flex min-h-screen flex-col items-center justify-center gap-4 p-6 text-center">{children}</div>;
}
