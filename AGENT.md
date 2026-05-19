# Agent Notes

- Performance and memory optimization claims must include measured before/after data.
- For memory work, report the workload, command or script shape, build profile, sample source, and the exact metric used, such as RSS/HWM from `/proc/<pid>/status`.
- If a benchmark does not support the optimization claim, say that directly and keep iterating instead of presenting the change as an optimization.
- Prefer isolated-process measurements for server memory. In-process benches that also host upstream and clients can hide the server's RSS behavior.
