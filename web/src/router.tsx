import { createRootRoute, createRoute, createRouter, type RouterHistory } from "@tanstack/react-router";

import { Allowed } from "@/me";
import { Shell } from "@/shell";

const rootRoute = createRootRoute({ component: Shell });

const browserRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => (
    <Allowed role="viewer">
      <p className="text-muted-foreground">The browser comes next.</p>
    </Allowed>
  ),
});

const recordsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/records",
  component: () => (
    <Allowed role="viewer">
      <p className="text-muted-foreground">The records come next.</p>
    </Allowed>
  ),
});

const exportsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/exports",
  component: () => (
    <Allowed role="exporter">
      <p className="text-muted-foreground">The exports come next.</p>
    </Allowed>
  ),
});

export const routeTree = rootRoute.addChildren([browserRoute, recordsRoute, exportsRoute]);

export function newRouter(history?: RouterHistory) {
  return createRouter({ routeTree, history });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof newRouter>;
  }
}
