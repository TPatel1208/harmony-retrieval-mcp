# PRD 022 — Per-call EDL token override: per-user identity through the tool contract

**Repo:** harmony-retrieval-mcp · **Session scope:** one session, one commit · **Label:** ready-for-agent
**Depends on:** nothing — backward compatible by construction; the consumer side (Talking-to-Air T30 storage, T31 injection) can deploy before or after this. Phase 2 (MCP side) of the per-user credential series. Decision record 2026-07-14 (design review session).

## Problem Statement

This server authenticates to NASA with exactly one identity: the boot-time `EARTHDATA_TOKEN` (or username/password) that `EDLAuth` exports into process env vars and caches as a single earthaccess session. Every workspace, every retrieval, every S3 credential fetch rides that one account. A consumer that knows *which human* is asking (Talking-to-Air has real per-user accounts and already scopes every call by `workspace_id`) has no way to make this server act as that human: quota and rate-limit attribution pools onto one account, EULA-gated collections work for everyone iff the service account accepted the license (invisibly wrong in both directions), and a compromised shared credential is everyone's credential. The auth layer's process-global design (env-var export, one cached session, `identity()` feeding workspace ownership) blocks any per-call identity even though every downstream client already supports explicit tokens — harmony-py takes `token=`, AppEEARS is bearer-authenticated, and the live EDL auth test proves a bare token reaches in-region S3.

## Solution

Every workspace-scoped tool gains an optional `edl_token` parameter. When present, that call — and any durable job it spawns — authenticates to NASA as that token's identity end to end (CMR, Harmony, AppEEARS, OPeNDAP, S3 temporary credentials). When absent, behavior is byte-for-byte today's: the env-level credential. Because the parameter is optional and advertised in the tool schemas, an old consumer against this server and a new consumer against an old server both keep working — the consumer feature-detects from the advertised schema. Durable jobs persist the credential (encrypted, in this stack's own Postgres, with its own key) for the life of the job because retries and provider fallbacks can need it hours later. Auth failures become first-class classified errors (expired/invalid token, EULA not accepted) instead of generic retrieval failures, and result-reuse/dedupe caching gains a credential-identity dimension so data fetched under one user's access rights is never served across users for access-restricted collections.

## User Stories

1. As a consumer backend (Talking-to-Air), I want an optional `edl_token` parameter on every workspace-scoped tool, so that I can make any call execute under the calling user's own EDL identity.
2. As a consumer backend, I want that parameter advertised in each tool's input schema, so that I can feature-detect support from the schema I already fetch at connect time instead of needing a version handshake.
3. As a consumer backend, I want calls without `edl_token` to behave exactly as today (env-level credential), so that this server upgrade deploys independently of my own release.
4. As a researcher whose token was injected, I want the entire retrieval pipeline — CMR queries, Harmony submits/polls, AppEEARS tasks, OPeNDAP subsets, S3 temporary credentials, result downloads — to use my identity, so that "acts as me" is true end to end, not just at the front door.
5. As a researcher, I want a durable job started with my token to keep using my token across worker restarts, poll cycles, and the Harmony→OPeNDAP fallback, so that a retrieval that outlives the original tool call doesn't silently degrade to the service account mid-flight.
6. As a researcher whose token expires mid-job, I want the job to fail with a specific "token expired" classification (not a generic retrieval error), so that the consumer can tell me to renew and retry rather than shrugging.
7. As a researcher who hasn't accepted a collection's EULA, I want the resulting authorization failure classified as "EULA not accepted" with the resolution URL when NASA provides one, so that the fix ("accept the license at Earthdata Login, then retry") is actionable.
8. As a researcher who pasted a revoked or malformed token, I want the first use to fail with a specific "token invalid" classification, so that the consumer can flip my connector to an error state instead of retrying forever.
9. As a researcher, I want my retrievals attributed to my EDL account for NASA-side quotas and rate limits, so that one heavy user doesn't exhaust a shared account everyone depends on.
10. As a researcher retrieving from an access-restricted (EULA-gated) collection, I want cached/deduped results scoped to credential identity, so that I'm never served bytes another user's entitlements fetched — and my entitlements never leak data to others.
11. As a researcher retrieving from an unrestricted public collection, I want dedupe and result reuse to keep working across users, so that per-user identity doesn't destroy the cache savings that make repeat queries fast.
12. As a platform operator, I want the persisted per-job credential encrypted at rest under a key private to this stack, so that a database dump exposes no user tokens.
13. As a platform operator, I want the persisted credential erased when a job reaches a terminal state, so that tokens don't accumulate at rest beyond their operational need.
14. As a platform operator, I want the token scrubbed from every log line, job trail entry, provenance record, and error message that might echo call arguments, so that per-user credentials never leak through observability surfaces.
15. As a platform operator, I want per-identity auth sessions cached (bounded, expiring), so that per-user calls don't re-authenticate to EDL on every tool invocation.
16. As a platform operator, I want the process-global env-var credential export confined to the legacy fallback path, so that two users' concurrent calls can never race on shared process state.
17. As a workspace owner, I want workspace ownership semantics unchanged (still keyed by `workspace_id` the consumer supplies), so that per-call tokens change *data-access* identity without silently rewiring whose workspace is whose.
18. As a maintainer, I want unit tests to exercise per-token auth with the injectable earthaccess/client seams that already exist, so that no test needs a real NASA credential.
19. As a maintainer, I want one live test proving a real token flows through the per-call path to S3 credentials, so that the end-to-end claim stays verified the same way the existing EDL live test verifies the env path.
20. As a security reviewer, I want `edl_token` to be the only new secret-bearing surface and its handling centralized (parse once at the tool boundary, thread an opaque identity handle after), so that audit scope stays narrow.

## Implementation Decisions

- **Contract shape.** `edl_token` (optional string, default absent) is added uniformly to every tool that already carries `workspace_id` — uniformity over minimalism, because the consumer's feature detection and injection wrapper treat the toolset as one surface. Empty/absent means "use env credential". The parameter is documented in each schema as consumer-supplied, never model-supplied (the consumer strips it from model-facing schemas, same as it already does for `workspace_id`).
- **Auth layer refactor.** The current single cached session becomes the *fallback* identity. A per-identity session layer keys cached auth sessions by token (bounded LRU with expiry), constructing each **without process-global env mutation**: harmony-py clients via its explicit `token=` argument; AppEEARS via its bearer header; S3 temporary credentials and OPeNDAP via a per-instance authenticated session built from the token directly (bypassing earthaccess's environment-strategy login where it insists on process env — direct bearer-header HTTP against the DAAC credential endpoints is acceptable where simpler; the live test defines "works"). The env-var export path survives untouched for the no-token fallback.
- **Identity ≠ ownership.** Workspace ownership stays exactly as-is (consumer-scoped `workspace_id`). The token is a data-access credential for NASA-facing calls only; nothing about workspace listing, handle resolution, or job ownership consults it.
- **Durable jobs.** The queue payload stays `job_id`-only (no credential ever enters Redis). The job spec row gains an encrypted-credential column: written at enqueue when a token was supplied, decrypted by the worker per attempt (submit, poll, materialize, provider fallback), and **nulled when the job reaches any terminal state** (success, failure, cancel). Encryption is Fernet/MultiFernet under a new env key private to this repo — explicitly a *different* key than any consumer holds; the schema change follows this repo's existing schema-management practice for the jobs table.
- **Error classification.** Three new first-class error classes in the structured error surface consumers already parse: `TOKEN_EXPIRED` (EDL/downstream rejects a formerly-valid token, including mid-job — the job terminates with this classification in its trail), `TOKEN_INVALID` (malformed/revoked at first use), `EULA_NOT_ACCEPTED` (authorization failure whose body/headers indicate a license-acceptance requirement; include the resolution URL when NASA supplies one). Everything else keeps its current classification.
- **Cache/dedupe scoping.** Result-reuse and granule/extract dedupe keys gain a credential-identity dimension governed by collection access metadata: collections CMR reports as unrestricted keep the globally shared cache (cross-user reuse preserved — this is the common case and the whole value of dedupe); collections with access constraints, or whose restriction status is unknown, scope reuse to the fetching identity (the EDL username behind the token, or the service identity for env-credential fetches). Conservative default: unknown → per-identity. A EULA-classified failure observed for a collection marks it restricted for future scoping decisions.
- **Secret hygiene.** One scrub chokepoint: wherever call arguments, job specs, or provider requests get logged, echoed into error envelopes, written to the job trail, or recorded in provenance, `edl_token` (and the decrypted per-job credential) is redacted. The token is parsed once at the tool boundary into an opaque identity handle (token value + derived EDL username + expiry) that the rest of the pipeline threads; raw token strings do not travel loose through provider code.

## Testing Decisions

- Good tests here assert *which identity a NASA-facing call would use* and *what classification a failure surfaces as* — external behavior at existing seams — never the internals of session caching.
- **Unit, auth layer** (existing injectable-earthaccess seam, prior art: the current auth unit tests): token-supplied call constructs a per-token session and never touches process env; absent token falls back to the env path unchanged; two different tokens yield two sessions; the session cache expires/evicts.
- **Unit, providers** (existing injectable-client seams): the Harmony provider builds its client from the per-call token when present, env settings when absent; AppEEARS sends the per-call bearer.
- **Unit, tools:** every workspace-scoped tool's schema advertises optional `edl_token`; a call without it is byte-identical in behavior to today (regression guard).
- **Integration, durable pipeline** (prior art: the existing durable-pipeline integration tests): a job enqueued with a token persists it encrypted, the worker decrypts it per attempt including across the Harmony→OPeNDAP fallback, and the column is null after terminal state; Redis payloads contain only `job_id`.
- **Integration, error classes:** stubbed provider responses for expired token, invalid token, and EULA-403 map to their classifications, and the EULA resolution URL passes through.
- **Cache scoping:** unrestricted collection → cross-identity reuse; restricted/unknown → same-identity reuse only; a EULA failure flips a collection's scoping.
- **Scrub tests:** with a token supplied, captured logs, error envelopes, job trail entries, and provenance records contain no substring of it.
- **Live** (`-m live`, prior art: the existing EDL live test): one test drives a real token through the *per-call* path to S3 temporary credentials.

## Out of Scope

- Any consumer-side work: token storage, encryption keys outside this stack, injection wrappers, UI (Talking-to-Air T30/T31).
- OAuth flows, token refresh, or minting tokens on a user's behalf — this server consumes whatever bearer token arrives.
- Per-user rate limiting *inside this server* (NASA-side attribution is the goal; local token buckets stay identity-blind for now).
- Changing workspace ownership semantics or `identity()`-based ownership mapping.
- Multi-credential fan-out (one call, one identity).
- Cross-host TLS for the MCP transport (single-host Docker network assumption stands; revisit if the stacks ever split hosts).

## Further Notes

The consumer's connect-time schema check is the deployment hinge: because it already verifies advertised parameters, *this* PRD's only compatibility obligation is "advertise `edl_token`, tolerate its absence". Do not make `edl_token` required anywhere — a hard requirement would force lockstep deploys, which the two-stack separation exists to avoid.

EDL user tokens live 60 days, so a token being valid at enqueue but expired at a late retry is a real (if uncommon) path — that's why `TOKEN_EXPIRED` must be reachable from the worker, not just the tool boundary, and why it terminates the job rather than falling back to the service credential (silent identity substitution is worse than a classified failure).

## Kickoff

**Recommended model:** Fable 5 or Opus. The auth-layer refactor (per-identity sessions without process-global state, threaded through providers and the durable worker) is the one genuinely delicate piece; everything else is well-precedented in this repo.

**Starter prompt:**
> Implement PRD 022 (`docs/prds/prd-022-per-call-edl-token-override.md`) in harmony-retrieval-mcp. Read the PRD fully first. Add the optional `edl_token` parameter to every workspace-scoped tool (advertised in schemas, absent = exact current behavior), refactor the auth layer to per-identity cached sessions constructed without process env mutation (harmony-py `token=`, AppEEARS bearer, direct bearer-header session for S3/OPeNDAP where earthaccess insists on env state — keep the env path as the no-token fallback), persist the credential encrypted on the job row under a new repo-private Fernet key (Redis stays job_id-only; null the column on terminal state), add the `TOKEN_EXPIRED` / `TOKEN_INVALID` / `EULA_NOT_ACCEPTED` classifications reachable from both the tool boundary and the worker, scope result-reuse/dedupe keys by credential identity per the CMR-restriction rule (unknown → per-identity), and centralize token redaction so no log, trail, envelope, or provenance record ever carries it. Write the tests in the PRD's Testing Decisions at the existing injectable seams, keep the whole suite green, and add the one live per-call-token S3 test alongside the existing EDL live test.
