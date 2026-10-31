import { createContext, useContext, type ReactNode } from "react";

import type { Me } from "@/auth";

const Ctx = createContext<Me | null>(null);

export function MeProvider({ value, children }: { value: Me; children: ReactNode }) {
  return <Ctx value={value}>{children}</Ctx>;
}

// useMe is the signed-in principal; pages render only inside the signed-in shell.
export function useMe(): Me {
  const me = useContext(Ctx);
  if (!me) throw new Error("useMe outside the signed-in shell");
  return me;
}

// Allowed renders children when the principal holds role, and says otherwise.
export function Allowed({ role, children }: { role: string; children: ReactNode }) {
  const me = useMe();
  if (!me.roles.includes(role)) {
    return <p className="text-muted-foreground">Your roles do not admit this page; it needs {role}.</p>;
  }
  return children;
}
