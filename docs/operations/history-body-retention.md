# Body retention and event identity

Expired message/activity/session content already equal to the exact application
expiry marker (or empty) needs no new audit. Maintenance rechecks metadata and
body CAS after nonqueued writer admission before advancing these rows.

Raw body-only expiry can use the existing age and unresolved-tail policy for
projected streaming/activity/usage methods, `message.completed`, and `turn.diff`
when no provider-turn link exists. This fallback requires at least 48 hours.
It clears only the established seven body fields; native event IDs and control
proof fields remain. The whole-event deletion whitelist remains eight methods.

Optional internal snapshot coverage is not part of the HTTP JSON contract.
When supplied by the guarded offline caller, it preserves mutable records newer
than the verified backup and unversioned turn bodies, while bounding provider
rows to the covered append prefix. Normal daily requests leave this scope unset.
GC batch caps, weighted scheduling, body budgets and provider/native dependencies
are unchanged by this policy update.
