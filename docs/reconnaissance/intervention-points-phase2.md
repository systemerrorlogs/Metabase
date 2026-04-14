# Phase 2: Minimal Intervention Points for User-Scoped DB Connections

**Branch:** `claude/metabase-codebase-reconnaissance-aFnS5`

---

## Goal

Identify the smallest possible set of files/namespaces to touch in order to:

1. Read the authenticated user's identity/credentials at connection checkout time
2. Bypass or skip the C3P0 pool for a direct/raw connection
3. Tie a connection's lifetime to the HTTP request rather than a single query

---

## Intervention Point 1 — Read the Authenticated User at Connection Checkout

### How the binding chain works

`api/*current-user-id*` (and siblings `*current-user*`, `*is-superuser?*`,
`*current-user-permissions-set*`) are bound by the session middleware at the top of
every HTTP request. Because Clojure dynamic vars propagate through the call stack,
they are **still live inside `do-with-resolved-connection`** when `.getConnection()`
is called on the C3P0 DataSource:

```
server/middleware/session.clj  — bind-current-user
  request/session.clj:38       — do-with-current-user  ← binding [*current-user-id* …]
    qp/process-query
      qp.execute/execute
        sql_jdbc.execute/do-with-connection-with-options
          sql_jdbc.execute/do-with-resolved-connection   ← STILL IN BINDING SCOPE
            .getConnection()                             ← pool checkout happens here
            set-default-connection-options! (L361)
              set-role-if-supported! (L378)              ← EE already reads *current-user-id*
```

### Existing hook

`src/metabase/driver/sql_jdbc/execute.clj:361` — `set-default-connection-options!`
calls `set-role-if-supported!` at **line 378** on every freshly checked-out
connection. The enterprise impersonation driver
(`enterprise/.../impersonation/driver.clj:122-146`) is the production reference:
it reads `api/*current-user-id*` at L62/L80 to look up a per-user DB role and
calls `driver/set-role!` on the live connection.

### Minimal files to touch

| # | File | What |
|---|------|------|
| 1 | `src/metabase/driver/sql_jdbc/execute.clj` | Extend `set-default-connection-options!` (L361) or add a new `defenterprise` stub analogous to `set-role-if-supported!` |
| — | `src/metabase/api/common.clj` (L104-127) | **Read-only** — `*current-user-id*` is already there |

**No new files required.** Pattern to follow:
`enterprise/.../impersonation/driver.clj:75-146`

---

## Intervention Point 2 — Bypass the C3P0 Pool for a Direct Connection

Four mechanisms already exist in the codebase. None require new code to *use*:

### Option A — `:connection` key in spec map (recommended; zero new code)

`src/metabase/driver/sql_jdbc/execute.clj:349-351` —
`do-with-resolved-connection` checks for a pre-existing `java.sql.Connection`
in the spec before touching the pool:

```clojure
(if-let [conn (:connection db-or-id-or-spec)]
  (f conn)      ; ← C3P0 pool completely skipped
  …pool path…)
```

Pass `{:connection <your-java.sql.Connection>}` as `db-or-id-or-spec` to any
function that ultimately calls `do-with-connection-with-options` and the pool is
never consulted.

**Touch-points: zero.** The code path exists today.

### Option B — `sql-jdbc.actions/*connection*` dynamic var (zero new code)

`src/metabase/driver/sql_jdbc/actions.clj:135` — a `^:dynamic *connection*` var
and `with-jdbc-transaction` macro (L182-188). If bound before entering the QP the
same connection is reused for all operations inside that dynamic scope.

**Touch-points: zero.** Bind before calling `qp/process-query`.

### Option C — `with-swapped-connection-details` (zero new code; per-user *pool*)

`src/metabase/driver/connection/workspaces.clj:65-84` — macro that merges a
caller-supplied map (e.g. `{:user "alice" :password "…"}`) into the DB `:details`
before the pool cache key is computed. Different details hash → separate C3P0 pool
in the Guava TTL cache (15-min TTL, `swapped-connection-pools`).

```clojure
(driver.w/with-swapped-connection-details db-id {:user "alice" :password "…"}
  (qp/process-query query))
```

**Touch-points: zero.** This is how workspace isolation works today.

### Option D — `*resilient-connection-ctx*` (zero new code)

`src/metabase/driver/sql_jdbc/execute.clj:918-924` — dynamic var holding
`{:db … :conn …}`; `get-resilient-connection` (L969) reuses `:conn` if the
connection is still open instead of checking out from the pool.

**Touch-points: zero.**

### Decision table

| Option | Pool used? | Per-user? | Scope |
|--------|------------|-----------|-------|
| A — `:connection` key | No | Yes (you supply it) | Single checkout call |
| B — `*connection*` var | No | Yes | Transactional multi-op block |
| C — `with-swapped-connection-details` | Yes (per-user sub-pool) | Yes | Dynamic scope |
| D — `*resilient-connection-ctx*` | No (if conn open) | Yes | Dynamic scope |

---

## Intervention Point 3 — Tie Connection Lifetime to the HTTP Request

### What does NOT exist today

`with-open` in `do-with-resolved-connection` (execute.clj:355) closes the
connection after **each individual query**, not at the end of the HTTP response.
There is no per-request connection affinity.

### Available lifecycle signals

| Signal | File | Notes |
|--------|------|-------|
| `do-f-async` `finally` block | `server/streaming_response.clj:200-210` | Fires when worker thread exits regardless of outcome |
| `finished-chan` closed | same file | Promise chan available to callers |
| `bind-current-user` / `do-with-current-user` scope | `request/session.clj:38-61` | Wraps the entire handler — `finally` here = end of request |
| `AsyncListener.onComplete` | `server/instance.clj:78` | Jetty fires after full async response |

### Minimal implementation: new middleware + one-line guard

#### New file (1)

**`src/metabase/server/middleware/request_connection.clj`**

```clojure
(def ^:dynamic *request-connection*
  "Holds a checked-out java.sql.Connection for the lifetime of the current
  HTTP request. nil when no request-scoped connection is in use."
  nil)

(defn wrap-request-connection
  "Middleware that checks out one connection at request start (using
  api/*current-user-id* which is already bound by bind-current-user)
  and closes it when the response is complete."
  [handler]
  (fn [request respond raise]
    ;; acquire connection here using user identity
    (let [conn (checkout-connection-for-current-user!)]
      (try
        (binding [*request-connection* conn]
          (handler request respond raise))
        (finally
          (.close conn))))))
```

#### Files to modify (2)

| File | Change |
|------|--------|
| `src/metabase/server/handler.clj` ~L96 | Add `mw.request-connection/wrap-request-connection` to middleware stack **immediately after** `mw.session/bind-current-user` |
| `src/metabase/driver/sql_jdbc/execute.clj:336` | In `do-with-resolved-connection`, add `(if *request-connection* (f *request-connection*) …pool path…)` before the pool checkout |

#### Middleware placement

```
; handler.clj — relevant section (innermost first):
#'mw.session/bind-current-user                        ; user identity bound  ← existing
#'mw.request-connection/wrap-request-connection       ; connection acquired   ← NEW
#'mw.pf-cache/wrap-premium-features-cache-check       ; existing
…
```

#### Guard in `do-with-resolved-connection`

```clojure
;; execute.clj — do-with-resolved-connection (L336)
(binding [*connection-recursion-depth* (inc *connection-recursion-depth*)]
  (cond
    ;; NEW: request-scoped connection takes priority
    mw.request-connection/*request-connection*
    (f mw.request-connection/*request-connection*)

    ;; existing: `:connection` key in spec
    (:connection db-or-id-or-spec)
    (f (:connection db-or-id-or-spec))

    ;; existing: pool path
    :else
    (let [get-conn (fn [] (.getConnection (do-with-resolved-connection-data-source …)))]
      …)))
```

---

## Summary — Minimal File Set per Goal

| Goal | Files to touch | New files? |
|------|---------------|------------|
| 1 — Read user identity at checkout | `sql_jdbc/execute.clj` (extend `set-default-connection-options!`) | No |
| 2 — Bypass C3P0 pool | None — multiple existing mechanisms; Option A requires zero code | No |
| 3 — Request-scoped connection | `server/handler.clj` (middleware stack) + `sql_jdbc/execute.clj` (guard) | Yes: `server/middleware/request_connection.clj` |

---

## Key File Reference

| File | Purpose | Relevant lines |
|------|---------|---------------|
| `src/metabase/api/common.clj` | `*current-user-id*` and other auth dynamic vars | L104-127 |
| `src/metabase/request/session.clj` | `do-with-current-user` — binding scope | L38-61 |
| `src/metabase/server/handler.clj` | Full middleware stack | L82-100 |
| `src/metabase/server/middleware/session.clj` | `bind-current-user` | L268-283 |
| `src/metabase/driver/sql_jdbc/execute.clj` | `do-with-connection-with-options` multimethod; `do-with-resolved-connection`; `set-default-connection-options!`; `*resilient-connection-ctx*` | L71, L336, L349, L361, L378, L918 |
| `src/metabase/driver/sql_jdbc/actions.clj` | `*connection*` dynamic var for pool bypass | L135-188 |
| `src/metabase/driver/connection/workspaces.clj` | `with-swapped-connection-details` | L65-84 |
| `src/metabase/driver/connection.clj` | `effective-details` — single credential access point | L102 |
| `src/metabase/server/streaming_response.clj` | `do-f-async` request lifecycle signal | L185-213 |
| `enterprise/.../impersonation/driver.clj` | Reference pattern: user identity → connection role | L62, L80, L122-146 |
