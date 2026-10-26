# P1 — parked replay

Status: deferred (2026-10-27) — withdrawn from the admin commands by T-40; kept as the record a later design starts from

Covers an operator command that retries a capture's parked messages before `parking.redelivery_delay` passes.
Read it when the parked consumer's own retry proves too slow for a recovery.

## Problem

After an operator fixes what kept a capture's messages parked (a consumer, a policy, a restored claim object),
the parked consumer retries them only when their nak delay, `parking.redelivery_delay`, has passed.
`operations.md` once listed `parked replay <capture>` for retrying them at once, but no spec said how.

## What a design must answer

- JetStream redelivers a nak-delayed message only after its delay,
  so an early retry cannot come from the parked consumer that holds the message.
- A second consumer filtered to the capture's subjects on the parking stream would receive messages
  the parked consumer may also hold; acknowledgements go through the coordinator and copy rows are idempotent,
  but both consumers' pending sets, a work-queue stream's one-consumer-per-subject rule
  and the incarnation fence (`tk-*` consumer names) must be accounted for.
- Who runs it: the admin command, which holds no lease, or the parked consumer on a catalog request.

## Until then

Wait for `parking.redelivery_delay`, or end the capture with `capture terminate`.
