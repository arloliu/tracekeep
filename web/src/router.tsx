import { createRootRoute, createRoute, createRouter, Link, Outlet } from "@tanstack/react-router";

const rootRoute = createRootRoute({
  component: () => (
    <div className="min-h-screen">
      <header className="flex items-center gap-6 border-b px-6 py-3">
        <Link to="/" className="font-semibold">
          tracekeep
        </Link>
      </header>
      <main className="p-6">
        <Outlet />
      </main>
    </div>
  ),
});

const browserRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => <p className="text-muted-foreground">The browser comes next.</p>,
});

export const routeTree = rootRoute.addChildren([browserRoute]);

export function newRouter() {
  return createRouter({ routeTree });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof newRouter>;
  }
}
