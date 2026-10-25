# Persistent transport loss is not provider termination

Unexpected ACP connection loss (including EOF, notification budget exhaustion or
protocol errors) is classified using adapter-owned facts, never a provider event
field or error-message substring. The authenticated attachment captures descriptor
ownership and Linux host/direct-provider PID birth identities. A read-only local
probe distinguishes the same surviving host/provider, a confirmed bound-process
end, and inconclusive identity. No socket attach, process start, signal, host
protocol change or command-line inspection is part of this probe. Unsupported or
unreadable identity is unknown, never proof the provider exited.

Stopped-event handling and a bare event-stream EOF both preserve bound work and
queued inputs when the attached persistent provider survives or its state is
unknown. The controller is still visibly stopped/exited and cannot accept a new
provider turn. It does not automatically dispatch, acknowledge failed events,
reset native history or mark retained work delivered. Confirmed provider/host
termination and nonpersistent stream endings keep the existing cleanup behavior.
Explicit user termination records its intent first and overrides preservation,
including a later Exit after an earlier ordinary detach. A detach's once cannot
consume the independent explicit termination action. Retention is not liveness.

This patch does not restore historically failed queues or expose a repair API.
The existing live-transport journal recovery must still prove exact original
turn/archive/native ownership and restores the original bound turn without Send;
its prior targeted guards remain required. Any exact queued-row recovery is a
separate application operation with its own manifest/owner guards.

`AO_ACP_NOTIFICATION_QUEUE_BYTES=67108864` and capacity16384 are a finite
operational mitigation for an approximately30MiB pending replay; the code default
is unchanged and the deployment owner sets the environment. The SDK charges the
active handler and queued wire frames. Its finite byte/frame budgets remain
intact; exceeding them still disconnects, now without misreporting provider death.
The wire budget is not an RSS cap: decoded events, buffers and each concurrent
connection consume additional memory. A larger journal or slower consumer can
still overflow64MiB. No guarantee of unlimited replay is made.
