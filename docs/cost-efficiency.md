# Orbit cost efficiency — local validation, 21 September 2026

## Status

Implemented locally on top of `dab165a960b28910fa882cbf71703cf1a4cb6362`. No deployment, paid Cloudflare workload, fleet rerun, or source-database change. These savings are **not yet on preview or production**. Surface publication is the only remote write in this pass.

This supersedes the phase-one estimate. The cost model is illustrative; monthly active organizations alone are insufficient to forecast a bill. Connected socket-hours, changes, view churn, library size, and shared-partition fan-out matter.

## Changes

- Negotiated Cloudflare native heartbeat auto-response avoids waking the DO for idle pings. Legacy JSON heartbeats still work and no longer write `last_seen_at`. Heartbeat timing and loss detection are preserved.
- Fresh authorization renews the existing socket instead of rebuilding SQL sessions and subscriptions every five minutes. The Worker validates the fresh token and partition; the DO requires the same unexpired identity and re-resolves every original query reference against current policy. Policy changes, denial, timeout, old session metadata or a changed endpoint fall back to reconnect. Renewal never revives an expired session.
- Renewed expiry and changed acknowledgment cursors live in hibernation attachments, with no SQL writes. Engine checkpoints, browser cursors, mutation receipts and transaction boundaries remain durable and unchanged. Outbound data is blocked after expiry even if the alarm is delayed.
- Guard unchanged metadata, subscription state and client subscription state. Repeated initialization and identical live subscriptions do not rewrite rows.
- Compare CDC values with the actual cached row and update only changed columns. Do not trust the CDC before-image to prove equality. Missing rows insert normally. Preserve scalar encodings, lossless bigint projection, primary-key changes, membership updates and transactional checkpoints.
- New membership tables use `WITHOUT ROWID`, retaining reverse indexes. Existing populated layouts remain valid; migrate atomically only when all three membership tables are empty. No forced refill or mass rewrite. GC uses composite keys on both layouts.
- Retain up to eight small orphaned views (at most 1,000 membership entries each) beyond the ordinary grace period, for 36 hours by default. Their memberships receive **no CDC maintenance** while inactive. Reactivation reconciles against current cache and authorization; this is not a stale-result fast path. Larger views retain the ordinary grace. Deadline scheduling and eviction use an index; physical GC remains bounded at 1,000 entries per pass.

`warmSubscriptionRetentionMs: 0` disables the extended cache. Its default is independent of `subscriptionGraceMs`, so Anara's explicit 60-second ordinary grace does not accidentally disable the optimization. The eight-view limit applies to views retained beyond the next ordinary grace period; ordinary short-grace orphans can coexist. This bounds extra retained memberships, not total cached source rows or active subscriptions.

The `client_subs.query_ref` and `subscriptions.retire_at` additions are local DO SQLite upgrades, not PlanetScale migrations. Existing sessions without original query references reconnect once. No token lifetime was increased and no authorization check was removed.

## Local measurements

Counters use real workerd SQLite `cursor.rowsWritten`, including index writes. They are local measurements, not Cloudflare invoice telemetry.

| Operation                                |                                   Before |                     Current |
| ---------------------------------------- | ---------------------------------------: | --------------------------: |
| Unchanged initialization                 |                                 2 writes |                           0 |
| Identical live engine subscription       |                                 2 writes |                           0 |
| Rename with a mature sequence log        |                                 6 writes |                           4 |
| First 100-row membership, compact layout |                               311 writes |                         209 |
| Grow 100 to 300                          |                               641 writes |                         434 |
| Grow 100 to 10,000                       |                            46,134 writes |                      31,086 |
| 10,000-row cold fill                     |                            30,209 writes |                      30,209 |
| Retire all 10,000-row windows            |                            15,255 writes |                      15,255 |
| Heartbeat timestamp                      |                             1 write/ping |                           0 |
| Changed client acknowledgment            |                              1 write/ack |                           0 |
| 100 successful changing-grant renewals   | Reconnect rebuilt sessions/subscriptions | 0 SQL changes; same session |

Renewal's zero SQL count does **not** mean zero charge: expiry alarms still re-arm and HTTP requests are metered. Attachments are checked across actual local workerd eviction. Native auto-response timestamps confirm that the transport uses Cloudflare's auto-response facility.

### Thirty-day workload simulation

Actual Anara schema; 300 rows; 30 simulated daily sessions separated by 16 offline hours. Each day: 50 scalar edits, 1 or 4 independent 100-row queries, one query grows to 200 then 300, and 20 unchanged resubscriptions. One cold fill per month. The sequence log starts mature, so pruning costs are included. Initialization, session bookkeeping, memberships, CDC, daily retirement and final cleanup are counted. Renewals, alarms and the external fill registry are separate.

| Live views | Fixed engine without extended retention: writes | With bounded retention: writes | Reduction | Retained reads |
| ---------- | ----------------------------------------------: | -----------------------------: | --------: | -------------: |
| 1          |                                          36,219 |                          9,162 |     74.7% |        569,280 |
| 4          |                                          65,199 |                         11,346 |     82.6% |        754,105 |

Every variant ends with 300 identical rows, cursor 3505 and SHA-256 `9adfc222378e30fa7eee9d957e4adf11a749500d2722c2bd707752ddf17c158e`. Membership is checked against full recomputation every day. This is a synthetic workload using the real schema, not the application's entire permission-aware query mix or a million-org capacity test.

## Monthly SQL-write estimate

All organizations in this table are connected **eight hours every day for 30 days**, one socket per organization, with the workload above. That is 240 connected hours per organization per month, not merely one monthly visit.

| Organizations | Views/socket | Original component model | Phase-one model | Current measured workload + modeled alarms |
| ------------- | -----------: | -----------------------: | --------------: | -----------------------------------------: |
| 50,000        |            1 |                   $9,695 |          $4,551 |                                   **$575** |
| 50,000        |            4 |                  $17,196 |         $11,594 |                                   **$684** |
| 100,000       |            1 |                  $19,439 |          $9,152 |                                 **$1,199** |
| 100,000       |            4 |                  $34,442 |         $23,237 |                                 **$1,417** |
| 1,000,000     |            1 |                 $194,835 |         $91,961 |                                **$12,432** |
| 1,000,000     |            4 |                 $344,865 |        $232,811 |                                **$14,616** |

These are SQL row-write charges only, not total hosting or certified upper bounds. The original and phase-one columns are earlier component models; the current column uses a measured monthly workload plus explicit alarm assumptions. The comparison projects roughly 94–96% lower write charges for this workload; it is not an observed invoice reduction.

Current per-org arithmetic: 9,162 or 11,346 measured writes, plus 3,200 modeled expiry-alarm writes (five-minute grants renewed 30 seconds early), plus a budget of 120 other alarm writes/month for connection/cleanup/fill scheduling. Multiply by org count; subtract the account's 50M shared allowance once; round up to million-row units at $1/M. Other account usage can consume the allowance. The alarm budget is an estimate, not a measured upper bound. Populated legacy membership layouts have somewhat higher construction costs until naturally empty.

An org with 24 connected hours is not automatically one tenth of this table: connection/renewal costs scale with hours, while edits, fills and view reconstruction scale with their own counts. For occasional visits separated by more than 36 hours, budget fresh membership construction. Use actual socket-hours and operation distributions before approving a production budget.

### Other charges and remaining expensive cases

- For 50k orgs at this duty cycle, ten-second application pings produce 4.32B inbound messages/month. Conservatively retain their 20:1 request charge (~$32.25 in isolation); native auto-response removes their additional DO duration, not a claimed request exemption. At one million equally active orgs, that request component is about $647.85.
- The model has ~160M renewal HTTP calls and ~160M expiry-alarm invocations at 50k orgs; together about $48 of marginal DO requests. At one million, about $960. Worker and application token-endpoint costs are additional and have not been measured here. Cross-origin preflights can add requests despite caching.
- The measured workload alone reads 28.46B/37.71B rows at 50k orgs, about $3.47/$12.71 after the shared read allowance. Renewal, wake-up and other reads are additional. At one million orgs, the measured workload portion is about $544.28/$729.11.
- Duration now follows useful requests and alarms rather than idle heartbeat wake-ups. It remains a deployment-dependent unknown. For scale sensitivity, one million orgs with 8,000 non-heartbeat events each per month, averaging 1/5/20ms billable duration per event, yields about $12.50/$62.50/$262.50 DO duration after allowance and rounding. These are assumptions, not measured production timings, and event types overlap.
- Retained data still costs storage; every extra 1 MB per org across one million orgs is approximately 1 TB, ~$200/month marginal. The bounded view cache retains keys, not another copy of documents. Source caches, indexes and replay logs also consume storage, including in inactive orgs.
- Large windows are still expensive: full 100→10,000 growth plus cleanup is 46,341 writes after the earlier fixes (versus 61,389 before), excluding first materialization and fill. The extended cache deliberately does not retain those large views. Repeated fleet-wide deep scrolling or broad query churn can dominate ordinary usage.
- Extra related-table changes, mutation receipts, shared-partition fan-out, source fills/registry, Worker CPU, application token issuance, Railway and PlanetScale are excluded. No all-in invoice forecast is claimed.

Pricing: [Cloudflare Durable Objects](https://developers.cloudflare.com/durable-objects/platform/pricing/), checked 2026-09-21. Native response behavior: [Durable Object state API](https://developers.cloudflare.com/durable-objects/api/state/). Arithmetic and raw results: `/tmp/orbit-stress/cost-phase2-20260921/monthly-model.py`, `monthly-estimates.json`, and `month-*.json`.

## Validation and rollout limits

- Local workerd exercises successful renewal, 100 consecutive changing grants without SQL writes, hibernation, shortened/expired grants, wrong identity, denied partition, spoofed headers, missing session, current query policy changes, and no data delivery after expiry.
- Client tests cover old-server fallback, fresh credentials, server-relative clock skew, renewal failures/timeouts/target changes, cached-token non-thrashing, cancellation, native heartbeat loss, late old-socket pongs and legacy in-flight pong transition.
- Retention tests cover custom short grace with the default warm policy, reactivation across eviction, offline source deletion, ten daily reopens, membership recomputation, view/member caps, legacy deadlines, interrupted transactions, and indexed deadline/retirement reads with 10,000 old views.
- Earlier correctness checks remain: 2,880 randomized transaction steps; scalar differential tests, exact bigints and composite keys; stale before-images, duplicate/out-of-order CDC, rollback/retry; legacy layout compatibility and atomic migration; browser offline queues, closed-tab recovery and owner handoff.
- Final workspace run: **249 tests passed**, 3 existing opt-in Vitess tests skipped. Four additional actual-schema monthly measurements passed. Workspace typecheck and changed-file formatting pass. Full lint retains nine pre-existing errors; no new lint errors. The workerd integration run also emits an intermittent `Expected global Vitest state` teardown warning despite passing assertions and exit status; fault-injection tests intentionally log registry exceptions. These logs are retained, not treated as proof of a clean hosted run. No local Vitess server was configured, so source-database integration was not rerun.

No fresh hosted UI, paid fleet or production billing test was run. Client transport and real Worker behavior are tested separately; this does not certify every application token callback or browser/CORS deployment configuration. Anara must consume the updated Orbit client and Worker together to realize all savings. Old clients remain functional but reconnect on expiry and use the legacy heartbeat path.

Rollback must keep composite-key membership GC: the old rowid-only collector cannot operate on compact tables. Distribute that compatibility before enabling compact layout where rollback to older versions is required, or roll forward. Reverting optional renewal/native-heartbeat capabilities falls back to the old client behavior; reverting retention can reclaim caches without discarding authoritative data.
