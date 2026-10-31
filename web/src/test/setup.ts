import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

afterEach(cleanup);

// jsdom has no scrolling; the router restores scroll positions.
window.scrollTo = () => undefined;
