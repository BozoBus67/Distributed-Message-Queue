I made a distributed task queue in Go.

It uses Lua scripts for atomic job claiming so workers can't double-claim the same job, a reaper that catches and re-queues jobs when workers crash mid-execution, retries with exponential backoff via a delayed sorted set, and a dead-letter queue for jobs that have failed too many times. For scheduling, I added priority dispatch via a weighted-lottery scheme, per-queue concurrency caps and rate limits so one noisy tenant can't starve everyone else, and multi-tenant routing keyed on tenant and priority.
