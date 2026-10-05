# 11 — Future Revision Notes

Decisions that may be revisited. Each entry states the current choice,
the alternatives, and the trigger that would cause a revision.

---

## 1. Read Receipts: Propagation to Sender

**Current approach:** Receipts flow through the outbox, same as messages.
The receipt handler inserts an outbox row per sender device
(`event_type = RECEIPT`). One transport, one cursor, one sync path — every
event (message, receipt, edit, delete) is delivered the same way.

**Why:** Uniformity. The sender's devices learn about receipts through the
same sync cursor that catches them up on messages. Offline senders get
receipts on reconnect without a second delivery mechanism. Redis loss or
a WS Gateway crash doesn't lose receipts — the outbox row stays
undispatched and gets picked up.

**Trigger to revisit:** Outbox write amplification. In a busy chat,
receipts, edits, and deletes can outnumber message rows. If receipt rows
dominate outbox volume and Relay drain becomes a bottleneck, consider
one of the alternatives below.

**Other options:**

- **Direct WS delivery.** Receipt handler XADDs straight to the sender's
  `ws:deliver:{node}` streams, bypassing outbox. Faster, but only works
  when the sender is online and the node is known. No recovery on Redis
  loss or Gateway crash. Receipts to offline senders would be dropped
  unless a second durable path is added — which reintroduces the outbox.

- **Ephemeral receipt channel.** Receipts ride a separate best-effort
  path (e.g. a lightweight pub/sub), not durable. Loses receipts for
  offline senders by design; UI reconciles on next message or sync.
  Acceptable only if the product treats receipts as soft state.

- **Dedicated receipts table.** Receipts stored in their own table with
  their own transport, separate from the outbox. Isolates volume, but
  duplicates the cursor, sync, and delivery machinery the outbox
  already provides.

**Leaning if revisited:** Direct WS delivery for the online fast path,
falling back to outbox for offline senders. Combines low latency with
durability, at the cost of two paths to reason about.

---