import { RouterProvider, createMemoryHistory, createRouter } from "@tanstack/react-router";
import { render, screen } from "@testing-library/react";
import { expect, test } from "vitest";

import { routeTree } from "./router";

test("the shell renders its home route", async () => {
  const router = createRouter({ routeTree, history: createMemoryHistory({ initialEntries: ["/"] }) });
  render(<RouterProvider router={router} />);
  expect(await screen.findByText("The browser comes next.")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "tracekeep" })).toHaveAttribute("href", "/");
});
