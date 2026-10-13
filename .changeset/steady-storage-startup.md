---
'@powersync/lib-service-postgres': patch
'@powersync/service-image': patch
---

Fix the service exiting with `postgres eof unexpectedly` at startup when Postgres bucket storage sits behind a proxy that drops slow connection setups. The storage schema check now runs on first use instead of during construction, retries a failed attempt once, and no longer fails every later storage query after one failed attempt.
