import { Code, ConnectError } from "@connectrpc/connect";

// ErrorNote says why an operation failed where its result would be:
// permission_denied as the roles not admitting it, anything else by its code and message.
export function ErrorNote({ error }: { error: unknown }) {
  const e = ConnectError.from(error);
  const text =
    e.code === Code.PermissionDenied
      ? "Your roles do not admit this."
      : e.code === Code.Unauthenticated
        ? "Your session ended; sign in again."
        : `${Code[e.code]}: ${e.rawMessage}`;
  return (
    <p role="alert" className="rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
      {text}
    </p>
  );
}
