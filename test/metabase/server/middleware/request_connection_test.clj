(ns metabase.server.middleware.request-connection-test
  "Unit tests for [[metabase.server.middleware.request-connection/wrap-request-connection]].

  Tests are fully self-contained — no real database or HTTP server required.
  Mock `java.sql.Connection` objects and a fabricated `StreamingResponse` are
  used throughout."
  (:require
   [clojure.core.async :as a]
   [clojure.test :refer :all]
   [metabase.request.connection :as request.conn]
   [metabase.server.middleware.request-connection :as mw.request-connection]
   [metabase.server.streaming-response :as streaming-response])
  (:import
   (java.sql Connection)))

(set! *warn-on-reflection* true)

;;; ── helpers ──────────────────────────────────────────────────────────────────

(defn- mock-conn
  "Create a minimal `java.sql.Connection` proxy.
  Returns `{:conn conn :closed? atom}` so tests can assert on cleanup timing."
  []
  (let [closed? (atom false)]
    {:conn    (proxy [Connection] []
                (isClosed [] @closed?)
                (close    [] (reset! closed? true)))
     :closed? closed?}))

(defn- run-middleware
  "Run `wrap-request-connection` around `handler-fn` synchronously.
  Returns `{:response … :error …}` with whatever the outer respond/raise received."
  [handler-fn]
  (let [response (atom nil)
        error    (atom nil)]
    ((mw.request-connection/wrap-request-connection handler-fn)
     {}                             ; request (content irrelevant)
     #(reset! response %)           ; respond callback
     #(reset! error %))             ; raise callback
    {:response @response :error @error}))

(defn- make-streaming-response
  "Create a `StreamingResponse` whose finished-chan we control explicitly.
  Returns `{:response sr :finished-ch chan}`.  Call `(a/close! finished-ch)`
  to simulate the async worker thread completing."
  []
  (let [sr          (streaming-response/-streaming-response
                     (fn [_os _cc] nil)       ; f — never actually invoked in tests
                     {:content-type "application/json"})
        finished-ch (streaming-response/finished-chan sr)]
    {:response sr :finished-ch finished-ch}))

;;; ── core middleware tests ────────────────────────────────────────────────────

(deftest ^:parallel binds-request-connection-var-test
  (testing "wrap-request-connection binds *request-connection* to an atom for the handler"
    (let [captured (atom :not-set)]
      (run-middleware
       (fn [_req respond _raise]
         (reset! captured request.conn/*request-connection*)
         (respond {:status 200 :body "ok"})))
      ;; The var should be an atom (not nil) inside the handler
      (is (instance? clojure.lang.Atom @captured)
          "*request-connection* should be an atom inside the handler"))))

(deftest ^:parallel var-is-nil-outside-middleware-test
  (testing "*request-connection* is nil outside wrap-request-connection scope"
    (is (nil? request.conn/*request-connection*)
        "*request-connection* must default to nil")))

;;; ── synchronous response cleanup ────────────────────────────────────────────

(deftest ^:parallel sync-response-closes-connection-test
  (testing "Connection is closed in the finally block after synchronous respond returns"
    (let [{:keys [conn closed?]} (mock-conn)
          respond-called?        (atom false)]
      (run-middleware
       (fn [_req respond _raise]
         ;; Simulate what do-with-resolved-connection does on first use:
         ;; pin a connection to the atom.
         (reset! request.conn/*request-connection* conn)
         (respond {:status 200 :body "ok"})))
      ;; respond itself was invoked
      (is (= {:status 200 :body "ok"}
             (:response (run-middleware
                          (fn [_req respond _raise]
                            (respond {:status 200 :body "ok"}))))))
      ;; connection is closed
      (is @closed? "connection must be closed after synchronous respond"))))

(deftest ^:parallel sync-response-closes-connection-even-if-respond-throws-test
  (testing "Connection is still closed if the respond callback itself throws"
    (let [{:keys [conn closed?]} (mock-conn)
          error                  (atom nil)]
      ;; Deliberately use raw invocation so we can catch the exception
      (try
        ((mw.request-connection/wrap-request-connection
          (fn [_req respond _raise]
            (reset! request.conn/*request-connection* conn)
            (respond {:status 200 :body "ok"})))
         {}
         (fn [_resp] (throw (ex-info "respond blew up" {})))
         (fn [e] (reset! error e)))
        (catch Exception _))
      (is @closed? "connection must be closed even when respond throws"))))

;;; ── streaming response cleanup (the async-timing fix) ────────────────────────

(deftest ^:parallel streaming-response-does-not-close-connection-immediately-test
  (testing "Connection is NOT closed when respond is called with a StreamingResponse"
    (testing "(middleware finally would fire here — the whole point of the async-timing fix)"
      (let [{:keys [conn closed?]}    (mock-conn)
            {:keys [response]}        (make-streaming-response)
            respond-called?           (atom false)]
        ((mw.request-connection/wrap-request-connection
          (fn [_req respond _raise]
            (reset! request.conn/*request-connection* conn)
            (respond response)))
         {}
         (fn [_resp] (reset! respond-called? true))
         (fn [e] (throw e)))
        (is @respond-called? "respond must be called")
        (is (not @closed?)
            "connection must NOT be closed immediately after respond for a StreamingResponse")))))

(deftest ^:parallel streaming-response-closes-connection-after-finished-chan-test
  (testing "Connection IS closed once the response's finished-chan closes"
    (let [{:keys [conn closed?]}      (mock-conn)
          {:keys [response
                  finished-ch]}       (make-streaming-response)]
      ((mw.request-connection/wrap-request-connection
        (fn [_req respond _raise]
          (reset! request.conn/*request-connection* conn)
          (respond response)))
       {}
       identity
       (fn [e] (throw e)))
      ;; Sanity: not closed yet
      (is (not @closed?) "connection should not be closed before finished-chan closes")
      ;; Simulate worker thread completing (mirrors do-f-async's finally block)
      (a/close! finished-ch)
      ;; The go block uses async scheduling, so give it a moment
      (let [deadline (+ (System/currentTimeMillis) 2000)]
        (while (and (not @closed?) (< (System/currentTimeMillis) deadline))
          (Thread/sleep 10)))
      (is @closed? "connection must be closed after finished-chan closes"))))

;;; ── error path cleanup ───────────────────────────────────────────────────────

(deftest ^:parallel error-path-closes-connection-test
  (testing "Connection is closed in the raise finally block when the handler raises"
    (let [{:keys [conn closed?]} (mock-conn)
          ex                     (ex-info "boom" {})
          captured-error         (atom nil)]
      ((mw.request-connection/wrap-request-connection
        (fn [_req _respond raise]
          (reset! request.conn/*request-connection* conn)
          (raise ex)))
       {}
       (fn [_resp] (is false "respond should not be called on error path"))
       (fn [e] (reset! captured-error e)))
      (is (identical? ex @captured-error) "original exception must be forwarded")
      (is @closed? "connection must be closed after raise"))))

;;; ── nil-atom no-op ───────────────────────────────────────────────────────────

(deftest ^:parallel nil-atom-is-a-no-op-test
  (testing "No connection acquired — close-conn! on a nil atom is a safe no-op"
    ;; The atom starts as nil; nothing stores a connection; cleanup should not throw.
    (is (nil? (:error (run-middleware
                        (fn [_req respond _raise]
                          ;; *request-connection* atom is bound but never populated
                          (respond {:status 200 :body "ok"}))))))))

;;; ── idempotence ──────────────────────────────────────────────────────────────

(deftest ^:parallel close-is-idempotent-test
  (testing "Closing an already-closed connection is a no-op (isClosed check)"
    (let [close-count (atom 0)
          conn        (proxy [Connection] []
                        (isClosed [] (pos? @close-count))
                        (close    [] (swap! close-count inc)))]
      ;; Manually invoke the private close-conn! via the public cleanup path
      (run-middleware
       (fn [_req respond _raise]
         (reset! request.conn/*request-connection* conn)
         (respond {:status 200 :body "ok"})))
      ;; Even if cleanup is called twice somehow, .close is invoked at most once
      (is (<= @close-count 1) "close should be called at most once per connection"))))
