import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";

import { whenUnauthenticated } from "@/auth";

// newQueryClient is the pages' query client: an operation answering unauthenticated returns the page to the sign-in page.
export function newQueryClient(): QueryClient {
  const client: QueryClient = new QueryClient({
    queryCache: new QueryCache({
      onError: (e) => {
        whenUnauthenticated(client, e);
      },
    }),
    mutationCache: new MutationCache({
      onError: (e) => {
        whenUnauthenticated(client, e);
      },
    }),
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  return client;
}
