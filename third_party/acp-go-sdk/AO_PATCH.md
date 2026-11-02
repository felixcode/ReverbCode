# Local ACP SDK notification bounds

Source: github.com/coder/acp-go-sdk v0.13.5, Go module checksum
`h1:LI9jq5xon7xslaYlnoktvTVyDlE37yIk2daT7N9ASYk=`.
The original license and source are retained.

Only `connection.go` and `client.go` change upstream runtime code. Optional
constructors accept queue capacity and retained notification wire-byte limits.
Original constructors keep the SDK's original 1024-slot / no byte-limit behavior.
AO opts into 16384 waiting slots and 16 MiB by default, adjustable through
`AO_ACP_NOTIFICATION_QUEUE_CAPACITY` and `AO_ACP_NOTIFICATION_QUEUE_BYTES`.
Positive overrides are used; empty, invalid and nonpositive values use defaults.

Byte accounting includes the active notification until its handler completes;
queue capacity bounds waiting notifications, with at most one additional active
handler. The byte budget counts encoded frames, not total Go heap/RSS. The
existing scanner also temporarily holds the currently received frame and retains
its original 10 MiB frame ceiling. This is a bounded replay allowance, not
unlimited buffering or a bound on total process RSS. Crossing either bound closes explicitly with the
existing overflow cause and leaves persistent-host journal recovery in place.
Requests, responses, cancellation, notification sequence and response watermark
semantics are unchanged. No reader waits for a notification handler to finish.
