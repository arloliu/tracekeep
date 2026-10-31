import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "./index.css";
import { newQueryClient } from "./query";
import { newRouter } from "./router";
import { ZoneProvider } from "./time";

const queryClient = newQueryClient();
const router = newRouter();

const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <ZoneProvider>
          <RouterProvider router={router} />
        </ZoneProvider>
      </QueryClientProvider>
    </StrictMode>,
  );
}
