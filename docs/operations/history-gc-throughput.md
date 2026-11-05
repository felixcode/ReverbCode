# GC-specific bounded batch size

`ALBANIA_AO_HISTORY_GC_BATCH_ROWS` defaults to250, accepts1..1000, and caps only the `gc:raw` metadata lifecycle stage. Body `BATCH_ROWS` and all read/write budgets stay unchanged. Request batch sizes may only tighten each stage-specific daemon cap.

The existing normal Store path still prepares a bounded metadata prefix, closes its reader, uses nonqueued TryMaintenance, checks BusinessWaiting, rereads current eligibility, and atomically deletes event and sidecar. The confirmed cursor advances only for rows actually processed; unused prefetched rows are not skipped. No payload is loaded or returned by GC.

A larger cap is not a latency guarantee. Start with250 only after scoped validation; if actual write cost is unacceptable lower this GC cap and caller setting to50. Never raise the body budget or interpret a missing response as a rolled-back transaction. The caller separately controls GC yield and its share of existing body stage rotation.
