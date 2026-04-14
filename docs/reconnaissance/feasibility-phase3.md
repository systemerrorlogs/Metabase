# Phase 3: Feasibility Assessment

**Branch:** `claude/metabase-codebase-reconnaissance-aFnS5`

---

## Executive Summary

Implementing user-scoped (per-request) JDBC connections in Metabase is *technically
feasible* but carries meaningful risk across three dimensions:

1. **LDAP credentials are not available at the DB connection layer** — they are
   discarded after login and would require an explicit new storage/forwarding path.
2. **Request-scoped connections require solving an async timing problem** —
   Ring async middleware's cleanup lifecycle does not align with when the query
   actually executes.
3. **Several EE safety checks and SSH tunnel setups are bypassed** if you skip pool
   checkout, which raises correctness and security concerns.

---

## 1. LDAP Credential Availability

### Bottom line: Not available. Would need explicit forwarding.

Metabase treats user authentication and database authentication as **completely
separate concerns**. LDAP is used only to verify a user's identity when they log
into Metabase; the LDAP password is used once then discarded.

#### Code evidence

| Location | What happens |
|----------|-------------|
| `src/metabase/sso/ldap/default_implementation.clj:46-65` | `search` finds the user entry; `verify-password` checks the credential |
| `src/metabase/request/session.clj:38-61` | `do-with-current-user` binds only `user-id`, `is-superuser?`, `is-data-analyst?`, `user-locale`, permissions — **no credential material** |
| `core_user` table (users/models/user.clj) | Stores `password` hash (bcrypt, for non-SSO users), `login_attributes` (LDAP directory attributes, not secrets), `sso_source` — **no plaintext or reversible credential** |
| `src/metabase/driver/util.clj` — `fetch-and-incorporate-auth-provider-details` | Fetches credentials from **database-level** auth providers (Azure Managed Identity, OAuth) — never from the current user's session |

The `UserInfo` map returned from LDAP contains: `dn`, `first-name`, `last-name`,
`email`, `groups` — no password, no token.

#### What would be required to forward LDAP credentials to the DB

1. **Encrypted session credential store** — during login, encrypt and store the LDAP
   password (or a derived token) alongside `core_session` or in a separate
   `session_credential` table with appropriate TTL.
2. **New dynamic var** — e.g., `*request-credential*`, bound in
   `request/session.clj:do-with-current-user` after decryption from the store.
3. **Connection spec injection** — pass the credential into
   `connection-details->spec` or apply it via `with-swapped-connection-details`
   immediately before pool lookup.

None of these pieces exist today. This is new infrastructure, not an extension of
existing patterns. **Kerberos/SPNEGO ticket delegation, SAML assertion forwarding,
and OAuth token relay are also absent from the codebase.**

---

## 2. Risk Areas

### Risk A — Async timing: middleware `finally` fires before the query runs

**Severity: CRITICAL (implementation-blocking without redesign)**

This is the fundamental problem with request-scoped connections in Ring async
handlers.

The standard Ring middleware pattern is:

```clojure
(defn wrap-request-connection [handler]
  (fn [request respond raise]
    (let [conn (checkout-connection!)]
      (try
        (binding [*request-connection* conn]
          (handler request respond raise))   ; ← (A)
        (finally
          (.close conn))))))                ; ← (B)
```

For **synchronous** responses, `(A)` blocks until the response is fully sent, so
`(B)` fires at the right time.

For **streaming (async) responses** — which all query endpoints use — `(A)` returns
immediately after `respond` is called with a `StreamingResponse` object. `(B)` fires
at that moment, **closing the connection before the query worker thread has even
started**.

Relevant code:
- `src/metabase/server/streaming_response.clj:440` — `streaming-response` macro
  uses `bound-fn` to capture current bindings (so `*request-connection*` IS
  captured by the worker closure)
- `src/metabase/server/streaming_response.clj:191-211` — `do-f-async` submits
  the task to a thread pool; the task runs after the handler returns
- `src/metabase/query_processor/middleware/process_userland_query.clj:62-64` —
  **Metabase itself documents this exact pitfall**: "By *not* using `bound-fn` or
  `future`, any dynamic variables in play... such as `db/*connection*`, won't be in
  play when the task is actually executed. That way we won't attempt to use closed DB
  connections."

**Safe fix requires tying cleanup to the async completion signal, not to the
middleware `finally`:**
- The `finished-chan` (a `promise-chan`) in `StreamingResponse` is closed by
  `do-f-async`'s `finally` block (line 207) after the worker thread exits.
- Connection cleanup must be scheduled on `finished-chan`'s close, e.g. via
  `core.async/go` or by adding a `donechan` listener — not via middleware `finally`.

### Risk B — Concurrent / multi-connection queries

**Severity: HIGH**

A single `java.sql.Connection` is not safe for concurrent use. A single HTTP request
can trigger multiple simultaneous `do-with-connection-with-options` calls in:

| Scenario | Where | Notes |
|----------|-------|-------|
| Dashboard "batch queries" | `/api/dashboard/:id/cards/query` | Multiple card queries, potentially parallel |
| Pivot queries | `src/metabase/query_processor/pivot.clj:243` | Multiple sub-queries; codebase comment explicitly states "each sub-query gets its own connection" |
| Nested queries / CTEs | `qp/middleware/fetch_source_query.clj` | Each source card resolved by its own QP call |
| Action DDL | `driver/sql_jdbc/actions.clj:161` | Separate connection for DDL alongside query connection |

Sharing one connection for all of these would cause: cursor state corruption,
transaction isolation violations, and `Connection already closed` errors when one
path closes the connection while another is mid-query.

**The `*connection*` var in `sql_jdbc/actions.clj:135` is the exception** — it is
explicitly scoped to a single transactional block and is not intended to be shared
across parallel branches.

### Risk C — Connection leaks on exception paths

**Severity: HIGH**

`do-f-async` (streaming_response.clj:200-210) has a `finally` block that always
fires, but it only closes the async context and channels — it does not know about
a `*request-connection*`.

Exception-swallowing points that would prevent connection cleanup from propagating:

| Location | Behaviour |
|----------|-----------|
| `qp/middleware/catch_exceptions.clj:138-154` | Catches ALL `Throwable`; formats the error and calls `*result*` instead of re-raising — connection cleanup in a QP middleware `finally` would NOT run after this |
| `streaming_response.clj:163-168` | `EofException` and `InterruptedException` silently post to `canceled-chan` and return `nil` — no exception propagated to outer code |
| `do-f-async` outer `catch Throwable` (line 196-199) | Writes error, posts to `finished-chan` — connection cleanup depends entirely on where it's placed |

**Correct pattern**: connection must be closed in the same `finally` block as
`do-f-async` itself (i.e., inside the streaming response abstraction) or as a
`finished-chan` listener — not inside QP middleware.

### Risk D — EE security check bypass

**Severity: HIGH (in EE deployments)**

`db->pooled-connection-spec` (sql_jdbc/connection.clj:449) calls
`driver-api/check-allowed-access!` at pool checkout. In EE this enforces database
routing access policies (which database a user is permitted to route to).

Using any pool-bypass mechanism (`:connection` key, `*connection*` var,
`*resilient-connection-ctx*`) skips `db->pooled-connection-spec` entirely, so
`check-allowed-access!` is never called. This silently allows access to databases
that should be blocked by EE routing policy.

**Fix**: call `check-allowed-access!` explicitly before any pool bypass.

### Risk E — SSH tunnel bypass

**Severity: MEDIUM**

`create-pool!` (sql_jdbc/connection.clj:196-198) calls
`driver/incorporate-ssh-tunnel-details` to set up an SSH tunnel before opening
connections. Connections obtained through any pool-bypass path receive a raw JDBC
URL pointing at the tunnel entrance port — but the tunnel is only started as part
of pool creation.

Bypassing pool checkout for databases configured with SSH tunnels will fail unless
the tunnel has already been opened by a prior pool creation for the same database.

### Risk F — C3P0 `unreturnedConnectionTimeout` kill

**Severity: MEDIUM**

C3P0 monitors connections checked out from the pool and forcibly closes any
connection not returned within `unreturnedConnectionTimeout` seconds
(sql_jdbc/connection.clj:142), which defaults to the query timeout. A request-scoped
connection held for the full request lifetime (checkout → query → streaming → client
receive) can easily exceed this timeout for slow networks or large result sets,
causing C3P0 to destroy the connection mid-stream.

Connections obtained outside the pool (via `:connection` key or `*connection*` var)
are not monitored by C3P0 at all — so there is no automated cleanup on timeout,
increasing leak risk.

### Risk G — Driver coverage gap

**Severity: MEDIUM**

The guard logic proposed in Phase 2 — checking `*request-connection*` at the top of
`do-with-resolved-connection` — applies to the default `:sql-jdbc` method. However,
9 drivers override `do-with-connection-with-options` and would bypass that guard:

| Driver | File |
|--------|------|
| `:h2` | `src/metabase/driver/h2.clj:565` |
| `:clickhouse` | `modules/drivers/clickhouse/src/.../clickhouse.clj:78` |
| `:databricks` | `modules/drivers/databricks/src/.../databricks.clj:310` |
| `:presto-jdbc` | `modules/drivers/presto-jdbc/src/.../presto_jdbc.clj:710` |
| `:redshift` | `modules/drivers/redshift/src/.../redshift.clj:316` |
| `:sparksql` | `modules/drivers/sparksql/src/.../sparksql.clj:207` |
| `:sqlite` | `modules/drivers/sqlite/src/.../sqlite.clj:493` |
| `:starburst` | `modules/drivers/starburst/src/.../starburst.clj:554` |

Each driver-specific override would need its own guard or would need to delegate
back to a shared helper.

---

## 3. Estimated Files Touched

### Feature 1 — User-identity-aware connection setup

Extend `set-default-connection-options!` with user-specific logic (analogous to
enterprise `set-role-if-supported!`):

| File | Change | Status |
|------|--------|--------|
| `src/metabase/driver/sql_jdbc/execute.clj` | Add hook in `set-default-connection-options!` (L361) or new `defenterprise` stub | **Modify** |
| `src/metabase/driver/sql_jdbc/execute.clj` | OSS stub for the new enterprise fn | **Modify** |
| `enterprise/.../impersonation/driver.clj` | Pattern to follow, no change needed | Reference |

**Subtotal: 1 file modified.**

No driver-level changes needed for this feature because `set-default-connection-options!` is called from the base `:sql-jdbc` `do-with-connection-with-options`, and individual drivers call `set-default-connection-options!` via delegation.

### Feature 2 — Pool bypass / direct connection

Using Option A (`:connection` key) or Option B (`*connection*` var) requires **zero
new code**. The call site supplying the connection would need to be written, but that
is application-level logic, not Metabase core changes.

If using Option C (`with-swapped-connection-details`), no changes either; the call
site wraps the QP call.

Additional files to touch **if EE bypass risk (Risk D) must be addressed**:

| File | Change |
|------|--------|
| New call site (TBD) | Explicitly call `driver-api/check-allowed-access!` before bypass |

**Subtotal: 0 core files modified (plus 1 call-site file TBD).**

### Feature 3 — Request-scoped connection lifetime

Accounting for the async timing fix (Risk A), the plan grows from 3 files to 5:

| File | Change | Why |
|------|--------|-----|
| `src/metabase/server/middleware/request_connection.clj` | **Create** | New middleware + `*request-connection*` var |
| `src/metabase/server/handler.clj` | **Modify** — insert new middleware after `bind-current-user` (L96) | Wire into stack |
| `src/metabase/driver/sql_jdbc/execute.clj` | **Modify** — guard in `do-with-resolved-connection` (L336) | Short-circuit to reuse connection |
| `src/metabase/server/streaming_response.clj` | **Modify** — attach cleanup to `finished-chan`/`donechan` close instead of middleware `finally` | Fix async timing (Risk A) |
| `src/metabase/query_processor/streaming.clj` | **Modify or read-only** — verify `*request-connection*` is captured by `bound-fn` in `streaming-response` macro (L440) and confirm cleanup order | Verify Risk A mitigation |
| 8 driver overrides (see Risk G) | **Modify or delegate** — each must call the shared guard | Driver coverage |

**Subtotal: 5 core files + up to 8 driver files modified, 1 new file.**

### Combined total

| Scope | Count |
|-------|-------|
| New files | 1 |
| Core files modified | 6–7 |
| Driver module files modified | 0–8 (depending on chosen bypass strategy) |
| EE enterprise files modified | 0–1 (if building on `defenterprise` pattern) |
| **Realistic minimum** | **8 files** |
| **Realistic maximum (full driver coverage)** | **16 files** |

---

## 4. Recommendation

| Goal | Feasibility | Blocker |
|------|-------------|---------|
| Read user identity at checkout | **High** — already works via dynamic vars; EE impersonation is the template | None |
| Bypass C3P0 pool | **High** — multiple existing mechanisms, zero new code | EE `check-allowed-access!` must be called explicitly |
| Request-scoped connection | **Medium** — feasible but requires solving async timing | Must attach cleanup to `finished-chan`, not middleware `finally` |
| LDAP credential forwarding to DB | **Low** — no infrastructure exists; new storage + forwarding path needed | LDAP password not stored post-login; 3+ new components required |

The highest-risk / lowest-feasibility item is LDAP credential forwarding. The
request-scoped connection is feasible if the async timing problem is solved. User
identity at checkout is already solved by the EE impersonation system and can be
extended cleanly.
