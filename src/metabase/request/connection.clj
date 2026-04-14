(ns metabase.request.connection
  "Dynamic var tracking a request-scoped JDBC connection slot.

  Declared in the `request` layer so that both the server middleware and the
  driver layer can reference it without a circular dependency.

  See `docs/reconnaissance/feasibility-phase3.md` for full design rationale,
  risk analysis, and known limitations.")

(def ^:dynamic *request-connection*
  "Atom holding a `java.sql.Connection` for the current HTTP request, or `nil`
  when request-scoped connection reuse is inactive.

  Lifecycle:
  - `nil`         → feature is off; pool is used per-query as normal.
  - `(atom nil)`  → feature is active; no connection acquired yet (lazy).
  - `(atom conn)` → feature is active; connection acquired on first query and
                    reused for subsequent queries within the same request.

  Bound by [[metabase.server.middleware.request-connection/wrap-request-connection]].
  Consumed by [[metabase.driver.sql-jdbc.execute/do-with-resolved-connection]]."
  nil)
