# Rate limiting

Rate limiting lives entirely **node-side** (`crates/node/src/ratelimit.rs`),
since limits are per API key. Two limiter layers, mirroring Riot's
enforcement: an *app* limiter per routing host and a *method* limiter per
(host, endpoint). Both start from the dev-key defaults (20 req/1 s +
100 req/2 min) and adopt the live windows from `X-App-Rate-Limit` /
`X-Method-Rate-Limit` response headers, so a production key applies with
no config change. 429 cooldowns honor `Retry-After` and are scoped by
`X-Rate-Limit-Type` to the offending layer.

App limiters additionally *pace*: sends are spread at the sustained rate
with randomized gaps (mean gap = tightest `window / limit`, so utilization
stays ~100% of budget) instead of bursting a whole window and starving —
the stream is continuous, restarts don't trigger a 429 storm, and the
Crawl Crew visualization flows instead of pulsing.

Dev-key sustained ceiling is ~0.83 req/s per host *per node*: expect
**~40–50 matches/hr stored per node** without timelines (half that with
`FETCH_TIMELINES` on).
