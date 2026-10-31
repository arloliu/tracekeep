import { createRootRoute, createRoute, createRouter, type RouterHistory } from "@tanstack/react-router";

import { Browser } from "@/browser";
import { Allowed } from "@/me";
import { validateSelector } from "@/selector";
import { Shell } from "@/shell";

const rootRoute = createRootRoute({ component: Shell });

const browserRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  validateSearch: validateSelector,
  component: () => (
    <Allowed role="viewer">
      <Browser />
    </Allowed>
  ),
});

const recordsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/records",
  validateSearch: validateSelector,
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
