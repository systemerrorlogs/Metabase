# Metabase Codebase Reconnaissance — Phase 1

**Date:** 2026-04-13  
**Branch:** `claude/metabase-codebase-reconnaissance-aFnS5`  
**Source:** metabase/metabase @ master (shallow clone)

---

## 1. Query Execution Path

### 1.1 HTTP Entry Points

Two primary REST endpoints kick off query execution:

| Route | File | Line |
|-------|------|------|
| `POST /api/dataset` | `src/metabase/query_processor/api.clj` | ~91 |
| `POST /api/card/:id/query` | `src/metabase/queries_rest/api/card.clj` | ~878 |

**Route registration** (`src/metabase/api_routes/routes.clj`, line 176):
```clojure
"/dataset" → (+auth 'metabase.query-processor.api)
```

### 1.2 Full Call Chain

```
POST /api/dataset  (or /api/card/:id/query)
  └─ defendpoint handler  [query_processor/api.clj ~L91]
       └─ run-streaming-query  [query_processor/api.clj ~L54]
            └─ qp/process-query  [query_processor.clj L77]
                 └─ qp.setup/with-qp-setup  [query_processor/setup.clj ~L150]
                      ├─ do-with-resolved-database   (validates DB ID)
                      ├─ do-with-metadata-provider   (sets up metadata)
                      └─ do-with-driver              (binds driver/*driver*)
                           └─ process-query*  (dynamic middleware stack)
                                └─ process-query**  [query_processor.clj L46]
                                     ├─ qp.preprocess/preprocess      L51  (20+ middleware)
                                     ├─ qp.compile/attach-compiled-query  L52  (MBQL → SQL)
                                     │    └─ driver/mbql->native  (multimethod, dispatches on driver/*driver*)
                                     └─ qp.execute/execute            L54  (8 middleware)
                                          └─ qp.pipeline/*run*  [pipeline.clj L108]
                                               └─ *execute* driver/*driver* query respond  L119
                                                    └─ driver/execute-reducible-query  (final dispatch)
                                                         └─ SQL JDBC: checkout pool conn → PreparedStatement → ResultSet
```

**Card path shortcut** (`queries_rest/api/card.clj` L894):
```
POST /api/card/:id/query
  └─ qp.card/process-query-for-card  [card.clj L297]
       └─ process-query-for-card-default-qp  [card.clj L231]
            └─ qp/process-query (qp/userland-query query)   ← rejoins main path
```

### 1.3 Middleware Layers

**Around-middleware** (error handling / userland bookkeeping):
```clojure
;; src/metabase/query_processor.clj L26-43
[handle-audit-app-internal-queries-middleware   ; enterprise audit queries
 process-userland-query-middleware              ; saves QueryExecution row
 catch-exceptions]                              ; wraps all exceptions
```

**Preprocessing middleware** (~20 steps, `query_processor/preprocess.clj`):
normalise → validate → resolve source-cards → expand aggregations →
substitute parameters → resolve source-tables → … → annotate → wrap

**Execution middleware** (`query_processor/execute.clj` L41-58):
```clojure
[swap-destination-db-middleware      ; enterprise connection swap
 apply-impersonation-postprocessing  ; enterprise impersonation
 update-used-cards!                  ; tracks card usage
 add-native-form-to-result-metadata
 add-preprocessed-query-to-result-metadata-for-userland-query
 maybe-return-cached-results         ; query result cache
 check-query-permissions             ; final permissions gate
 check-download-permissions-middleware]
```

### 1.4 Key Dynamic Variables in the Pipeline

| Var | Namespace | Purpose |
|-----|-----------|---------|
| `driver/*driver*` | `metabase.driver` | current driver keyword (`:postgres`, `:mysql`, …) |
| `qp.pipeline/*canceled-chan*` | `query-processor.pipeline` | cancellation channel |
| `qp.pipeline/*execute*` | `query-processor.pipeline` | overrideable execute fn |
| `qp.pipeline/*run*` | `query-processor.pipeline` | overrideable run fn |

---

## 2. Connection Pool — C3P0, NOT HikariCP

> **Important:** Metabase uses **C3P0** (`com.mchange/c3p0 0.12.0`) as its JDBC
> connection pool. HikariCP appears in `deps.edn` only as an **exclusion** of a
> transitive dependency from `clojurewerkz/quartzite`.

### 2.1 Pool Initialization

**Data-warehouse pools** (`src/metabase/driver/sql_jdbc/connection.clj`):

```clojure
;; L187-211 — create-pool!
(defn- create-pool! [{:keys [id], driver :engine, :as database}]
  (let [details             (driver.conn/effective-details database)    ; decrypts credentials
        details-with-tunnel (driver/incorporate-ssh-tunnel-details …)
        details-with-auth   (driver.u/fetch-and-incorporate-auth-provider-details …)
        spec                (connection-details->spec driver details-with-auth)  ; JDBC spec
        properties          (data-warehouse-connection-pool-properties driver database)]
    (merge
     (connection-pool-spec spec properties)   ; → C3P0 ComboPooledDataSource
     …)))
```

The call to `connection-pool-spec` (L169-174) ultimately calls:
```java
com.mchange.v2.c3p0.DataSources/pooledDataSource(datasource, properties)
```

**App-DB pool** (`src/metabase/app_db/connection_pool_setup.clj` L162):
```java
com.mchange.v2.c3p0.DataSources/pooledDataSource(data_source, pool_props)
```

### 2.2 Default Pool Configuration

`data-warehouse-connection-pool-properties` multimethod (`sql_jdbc/connection.clj` L91-167):

| Property | Value | Notes |
|----------|-------|-------|
| `acquireIncrement` | 1 | fetch one at a time |
| `acquireRetryAttempts` | 0 (prod) / 1 (test) | fail fast on bad creds |
| `minPoolSize` | 0 | serverless-friendly |
| `initialPoolSize` | 0 | lazy init |
| `maxPoolSize` | 15 (configurable) | `MB_DB_CONNECTION_POOL_MAX_SIZE` |
| `maxIdleTime` | 10800 s (3 h) | idle connection lifetime |
| `maxIdleTimeExcessConnections` | 300 s (5 min) | shrink back after spike |
| `testConnectionOnCheckout` | `true` | validate before handing out |
| `unreturnedConnectionTimeout` | matches query timeout | leak detection |

### 2.3 Pool Caching

Two caches in `sql_jdbc/connection.clj`:

```clojure
;; L223 — canonical pools: [db-id, connection-type] → pool-spec
(defonce pool-cache-key->connection-pool (atom {}))

;; L228 — workspace-swapped pools: [db-id, details-hash] → pool-spec
;;         evicted after 15 minutes of inactivity (Guava TTL cache)
(defonce swapped-connection-pools (.. (CacheBuilder/newBuilder)
                                      (expireAfterAccess 15 TimeUnit/MINUTES)
                                      …))
```

Pool invalidation triggers (checked on each `db->pooled-connection-spec` call):
- JDBC spec hash changed (creds updated in app DB)
- SSH tunnel closed
- Password expiry timestamp reached

### 2.4 Connection Checkout

`sql_jdbc/execute.clj`:

```clojure
;; datasource L209-213
(defn datasource ^DataSource [db-or-id-or-spec]
  (:datasource (sql-jdbc.conn/db->pooled-connection-spec db-or-id-or-spec)))

;; do-with-resolved-connection L336-356 — actual checkout
(let [get-conn (fn [] (.getConnection (do-with-resolved-connection-data-source …)))]
  (with-open [conn ^Connection (get-conn)]   ; auto-returns to pool on close
    (f conn)))
```

Pattern is **connection-per-query**: each query calls `do-with-connection-with-options` →
`do-with-resolved-connection` → `.getConnection()` on the C3P0 `DataSource` →
`with-open` returns connection automatically.

**Connection lifecycle hooks** on the App-DB pool (`MetabaseConnectionCustomizer` in
`connection_pool_setup.clj` L36-66):
- `onCheckIn` (PostgreSQL): executes `DISCARD ALL` to reclaim ~300 MB session memory
- `onAcquire` / `onCheckOut` / `onDestroy`: logging

---

## 3. Session / Authenticated User in Request Context

### 3.1 Middleware Chain

Defined in `src/metabase/server/handler.clj` (applied bottom-to-top):

```
wrap-session-key          → extracts session token from cookies/headers
wrap-current-user-info    → validates session against DB; enriches request map
bind-current-user         → binds dynamic vars for downstream handlers
```

### 3.2 Session Token Extraction

`src/metabase/server/middleware/session.clj` L83-91:

Three strategies tried in order:
1. `metabase.EMBEDDED_SESSION` cookie + anti-CSRF header (embedded iframes)
2. `metabase.SESSION` cookie (normal browser sessions)
3. `X-Metabase-Session` HTTP header (API/programmatic access)

Result attached to request as `:metabase-session-key`.

### 3.3 Session Validation

`current-user-info-for-session` (`session.clj` L173-199):
- Looks up `core_session` table; validates UUID format
- Joins with `core_user` and `tenant`
- Compares **SHA-512 hash** of the token against `key_hashed` column (never stores plaintext)
- Returns `{:metabase-user-id … :is-superuser? … :is-data-analyst? … :user-locale …}`

API Key fallback (`session.clj` L217-239):
- Extracted from `X-Api-Key` header
- Validated with bcrypt against `api_key` table
- Produces same user-info map

### 3.4 Dynamic Variable Binding

`src/metabase/api/common.clj` L104-133 — declarations:

```clojure
(def ^:dynamic ^Integer *current-user-id*   nil)
(def ^:dynamic         *current-user*       (atom nil))   ; Delay → User record
(def ^:dynamic ^Boolean *is-superuser?*     false)
(def ^:dynamic ^Boolean *is-group-manager?* false)
(def ^:dynamic ^Boolean *is-data-analyst?*  false)
(def ^:dynamic         *current-user-permissions-set*  (atom #{}))  ; Delay → Set
```

Bound in `request/do-with-current-user` (`src/metabase/request/session.clj` L38-61):
```clojure
(binding [*current-user-id*              metabase-user-id
          *is-superuser?*                (boolean is-superuser?)
          *current-user*                 (delay (find-user metabase-user-id))
          *current-user-permissions-set* (delay (current-user-info->permissions-set …))]
  (handler request respond raise))
```

### 3.5 Session Storage Schema

`core_session` table:

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid | session identifier |
| `user_id` | fk → core_user | |
| `key_hashed` | varchar | SHA-512 of token |
| `created_at` | timestamp | |
| `last_active_at` | timestamp | updated ≤ once/60 s |
| `anti_csrf_token` | varchar | for embedded sessions |
| `auth_identity_id` | fk | links to auth provider |

---

## 4. Data Source Credentials — Storage and Passing

### 4.1 Storage Location

**Model:** `:model/Database` (`src/metabase/warehouses/models/database.clj`)  
**Table:** `metabase_database`  
**Field:** `:details` — single encrypted JSON column (`mi/transform-encrypted-json`)  
Also: `:write_data_details` for a separate writable connection (EE feature)

### 4.2 Encryption at Rest

`src/metabase/util/encryption.clj`:

- **Algorithm:** AES-256-CBC + HMAC-SHA512
- **Key derivation:** PBKDF2+SHA512, 100,000 iterations
- **Key source:** `MB_ENCRYPTION_SECRET_KEY` env var (≥ 16 chars)
- **Transform:** `transform-encrypted-json` in `models/interface.clj` L304-328
  - `in` = `(comp encryption/maybe-encrypt json/encode)`
  - `out` = `(comp json/decode+kw encryption/maybe-decrypt)` — **memoized 1 h** for perf

When `MB_ENCRYPTION_SECRET_KEY` is not set, details are stored as plain JSON (dev/single-node deployments).

### 4.3 Sensitive Field Redaction

`src/metabase/driver/util.clj` L717-731 — `default-sensitive-fields`:

```clojure
#{:password :pass :tunnel-pass :tunnel-private-key
  :tunnel-private-key-passphrase :access-token :refresh-token
  :service-account-json}
```

`secret/clean-secret-properties-from-database` is applied to every `Database`
instance returned via the API (pipeline transform in `warehouses/models/database.clj`
L44-50), stripping all secret values before serialization.

### 4.4 Credential Access API

`src/metabase/driver/connection.clj` — the **single authorised access point** for details:

```clojure
;; L102 — normal use (respects write-connection context + workspace swap)
(defn effective-details [database] …)

;; L175 — bypass context (admin migrations, health checks)
(defn default-details [database] (:details database))
```

`effective-details` composes:
1. `(:details database)` — decrypted by Toucan2 transform
2. Merge with `:write-data-details` if inside `(with-write-connection …)` scope
3. Apply `driver.w/maybe-swap-details` for workspace isolation

### 4.5 Credential Flow to JDBC

```
Database record (decrypted by transform-encrypted-json on select)
  └─ driver.conn/effective-details  [connection.clj L102]
       └─ driver/incorporate-ssh-tunnel-details  (SSH tunnel setup)
            └─ driver.u/fetch-and-incorporate-auth-provider-details  (OAuth/IAM/etc.)
                 └─ sql-jdbc.conn/connection-details->spec  (driver-specific multimethod)
                      └─ connection-pool-spec  [connection.clj L169]
                           └─ C3P0 ComboPooledDataSource  (cached by [db-id, conn-type])
```

Credentials are present in plaintext **only** during `create-pool!` execution. The pool
itself stores a `DataSource` handle; individual `Connection` objects from the pool carry
no user-visible credential strings after handshake.

---

## 5. File Reference Map

| Topic | Key File(s) |
|-------|------------|
| HTTP routing | `src/metabase/api_routes/routes.clj` |
| Dataset API endpoint | `src/metabase/query_processor/api.clj` |
| Card query endpoint | `src/metabase/queries_rest/api/card.clj` |
| QP entry point | `src/metabase/query_processor.clj` |
| QP setup / driver binding | `src/metabase/query_processor/setup.clj` |
| QP pipeline (execute/reduce) | `src/metabase/query_processor/pipeline.clj` |
| QP execution middleware | `src/metabase/query_processor/execute.clj` |
| Driver multimethod defs | `src/metabase/driver.clj` |
| SQL JDBC connection pool | `src/metabase/driver/sql_jdbc/connection.clj` |
| SQL JDBC execute / checkout | `src/metabase/driver/sql_jdbc/execute.clj` |
| App-DB pool setup | `src/metabase/app_db/connection_pool_setup.clj` |
| Credential access API | `src/metabase/driver/connection.clj` |
| Workspace isolation | `src/metabase/driver/connection/workspaces.clj` |
| Database model | `src/metabase/warehouses/models/database.clj` |
| Encryption util | `src/metabase/util/encryption.clj` |
| Model transforms | `src/metabase/models/interface.clj` |
| Session middleware | `src/metabase/server/middleware/session.clj` |
| Server handler (middleware chain) | `src/metabase/server/handler.clj` |
| Auth dynamic vars | `src/metabase/api/common.clj` |
| Request session binding | `src/metabase/request/session.clj` |
| Sensitive field defs | `src/metabase/driver/util.clj` |
| C3P0 Prometheus metrics | `src/metabase/analytics/prometheus.clj` |
