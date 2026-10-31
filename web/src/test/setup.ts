import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

afterEach(cleanup);

// jsdom has no scrolling; the router restores scroll positions.
window.scrollTo = () => undefined;

// jsdom lays nothing out; a virtualized list's scroller (data-scroller) gets a viewport so its rows render.
Object.defineProperty(HTMLElement.prototype, "clientHeight", {
  configurable: true,
  get(this: HTMLElement) {
    return this.dataset.scroller === undefined ? 0 : 640;
  },
});
