(ns metabase.server.middleware.request-connection
  "Ring async middleware that provides a lazy, request-scoped JDBC connection slot.

  See `docs/reconnaissance/feasibility-phase3.md` for risk analysis, design
  rationale, and known limitations."
  (:require
   [clojure.core.async :as a]
   [metabase.request.connection :as request.conn]
   [metabase.server.streaming-response :as streaming-response]
   [metabase.util.log :as log])
  (:import
   (java.sql Connection)))

(defn- close-conn!
  "Close `conn` if it is non-nil and not already closed. Swallows exceptions so
  that a cleanup failure never bubbles back to the request thread."
  [^Connection conn]
  (when (and conn (not (.isClosed conn)))
    (try
      (.close conn)
      (catch Exception e
        (log/warn e "Error closing request-scoped JDBC connection")))))

(defn wrap-request-connection
  "Ring async middleware that binds [[request.conn/*request-connection*]] to a
  fresh atom for every incoming request.

  The atom starts as `nil`. The first call to
  `metabase.driver.sql-jdbc.execute/do-with-resolved-connection` within the request
  lazily acquires a JDBC connection from the driver pool and stores it; all
  subsequent calls within the same request reuse the stored connection.

  ## Async-timing fix (Risk A in feasibility-phase3.md)

  A naive implementation would close the connection in the middleware's own
  `finally` block. This is WRONG for streaming responses: the Ring middleware
  `finally` fires as soon as the handler returns a `StreamingResponse` object,
  which is *before* the async worker thread has started executing the query.

  This middleware intercepts the `respond` callback instead:

  - **Streaming responses** – detected by trying to call
    `streaming-response/finished-chan`. If successful, we schedule connection
    cleanup via `core.async/go` on the response's `finished-chan` promise-channel.
    That channel closes inside `do-f-async`'s own `finally` block, which runs
    *after* the worker thread finishes writing all results.

  - **Synchronous responses** – cleanup runs in a `try/finally` around
    `(respond response)`, which is safe because the response is fully written
    before `finally` fires.

  - **Error path** – cleanup runs in a `try/finally` around `(raise e)`.

  ## Concurrency limitation (Risk B in feasibility-phase3.md)

  A single `java.sql.Connection` is NOT safe for concurrent use. Requests that
  fan out into parallel sub-queries (dashboard batch cards, pivot sub-queries)
  will share this one connection, risking cursor-state corruption and
  `Connection already closed` errors.

  This prototype is correct for **sequential single-database query flows** only.
  Production use requires either a per-database connection map with concurrent-
  access detection, or a fallback to normal pool checkout when the slot is
  already in use."
  [handler]
  (fn [request respond raise]
    (let [conn-atom (atom nil)]
      (binding [request.conn/*request-connection* conn-atom]
        (handler
         request
         (fn [response]
           ;; Detect whether this is a StreamingResponse by attempting to read its
           ;; finished-chan.  finished-chan returns nil (or throws) for non-streaming
           ;; responses, so we fall through to the synchronous cleanup path.
           (let [finished-ch (try (streaming-response/finished-chan response)
                                  (catch Throwable _ nil))]
             (if finished-ch
               ;; Async/streaming path ─────────────────────────────────────────
               ;; Schedule cleanup to fire AFTER the worker thread exits (i.e. after
               ;; do-f-async's finally closes finished-chan), not when this respond
               ;; callback returns.
               (do
                 (a/go
                   (a/<! finished-ch)
                   (close-conn! @conn-atom))
                 (respond response))
               ;; Synchronous path ─────────────────────────────────────────────
               (try
                 (respond response)
                 (finally
                   (close-conn! @conn-atom))))))
         (fn [e]
           (try
             (raise e)
             (finally
               (close-conn! @conn-atom)))))))))
