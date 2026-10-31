import { createRootRoute, createRoute, createRouter, type RouterHistory } from "@tanstack/react-router";

import { Browser } from "@/browser";
import { ExportPage, Exports, NewExport } from "@/exports";
import { Allowed } from "@/me";
import { Records } from "@/records";
import { validateRecords, validateSelector } from "@/selector";
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
  validateSearch: validateRecords,
  component: () => (
    <Allowed role="viewer">
      <Records />
    </Allowed>
  ),
});

const exportsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/exports",
  component: () => (
    <Allowed role="exporter">
      <Exports />
    </Allowed>
  ),
});

const newExportRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/exports/new",
  validateSearch: validateSelector,
  component: () => (
    <Allowed role="exporter">
      <NewExport />
    </Allowed>
  ),
});

const exportRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/exports/$jobId",
  component: () => (
    <Allowed role="exporter">
      <ExportPage />
    </Allowed>
  ),
});

export const routeTree = rootRoute.addChildren([browserRoute, recordsRoute, exportsRoute, newExportRoute, exportRoute]);

export function newRouter(history?: RouterHistory) {
  return createRouter({ routeTree, history });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof newRouter>;
  }
}
