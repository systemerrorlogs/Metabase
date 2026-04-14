(ns ^:mb/driver-tests metabase.driver.sql-jdbc.execute-test
  (:require
   [clojure.test :refer :all]
   [malli.error :as me]
   [metabase.config.core :as config]
   [metabase.driver :as driver]
   [metabase.driver.connection :as driver.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.request.connection :as request.conn]
   [metabase.test :as mt]
   [metabase.util.malli.registry :as mr])
  (:import
   (java.sql Connection DatabaseMetaData)
   (javax.sql DataSource)))

(deftest ^:parallel ConnectionOptions-test
  (are [options error] (= error
                          (me/humanize (mr/explain sql-jdbc.execute/ConnectionOptions options)))
    nil                              nil
    {}                               nil
    {:session-timezone nil}          nil
    {:session-timezone "US/Pacific"} nil
    {:session-timezone "X"}          {:session-timezone ["invalid timezone ID: \"X\"" "timezone offset string literal"]}))

(set! *warn-on-reflection* true)

(deftest connection-reuse-test
  (testing "resilient context reuses reconnected connections"
    (mt/test-drivers (descendants driver/hierarchy :sql-jdbc)
      (let [test-db-id (mt/id)  ;; Get the test database ID
            connection-count (volatile! 0)
            orig-do-with-resolved-connection-data-source @#'sql-jdbc.execute/do-with-resolved-connection-data-source]
        (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                      (fn [driver db opts]
                        ;; Only count connections for our test database because on startup the audit-db will be
                        ;; synced, which causes this to fail intermittently because it creates connections (to db
                        ;; 13371337)
                        (if (= db test-db-id)
                          (reify javax.sql.DataSource
                            (getConnection [_]
                              (vswap! connection-count inc)
                              (.getConnection ^DataSource (orig-do-with-resolved-connection-data-source driver db opts))))
                          ;; For other databases (like audit DB), just pass through
                          (orig-do-with-resolved-connection-data-source driver db opts)))]
          (let [closed-conn (doto (.getConnection ^DataSource
                                   (orig-do-with-resolved-connection-data-source driver/*driver* test-db-id {}))
                              (.close))]
            (driver/do-with-resilient-connection
             driver/*driver* test-db-id
             (fn [driver _]
               ;; reinit, as we it has been used for setup
               (vreset! connection-count 0)
               (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn)
               (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn)
               (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn)
               (is (= 1 @connection-count))
               (.close (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn))
               (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn)
               (is (= 2 @connection-count))))))))))

(deftest resilient-reconnect-preserves-connection-type-test
  (testing "resilient reconnection preserves *connection-type* binding"
    (mt/test-drivers (descendants driver/hierarchy :sql-jdbc)
      (let [test-db-id               (mt/id)
            captured-connection-type (volatile! nil)
            orig-fn                  @#'sql-jdbc.execute/do-with-resolved-connection-data-source]
        (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                      (fn [driver db opts]
                        (when (and (= db test-db-id) (:keep-open? opts))
                          (vreset! captured-connection-type driver.conn/*connection-type*))
                        (orig-fn driver db opts))]
          (let [closed-conn (doto (.getConnection ^DataSource
                                   (orig-fn driver/*driver* test-db-id {}))
                              (.close))]
            (driver.conn/with-write-connection
              (driver/do-with-resilient-connection
               driver/*driver*
               test-db-id
               (fn [driver _]
                 (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn)
                 (is (= :write-data @captured-connection-type)
                     "Reconnection should preserve write-data connection type"))))))))))

(deftest try-ensure-open-conn-sets-non-recursive-options-test
  (testing "try-ensure-open-conn! sets connection options as non-recursive"
    #_{:clj-kondo/ignore [:metabase/disallow-hardcoded-driver-names-in-tests]}
    (mt/test-drivers (disj (descendants driver/hierarchy :sql-jdbc)
                           ;; too tricky to stub the connection
                           :presto-jdbc :databricks :starburst)
      (let [connection-option-calls (volatile! [])
            is-default-options
            (identical? (get-method sql-jdbc.execute/do-with-connection-with-options :sql-jdbc)
                        (get-method sql-jdbc.execute/do-with-connection-with-options driver/*driver*))

            orig-do-with-resolved-connection-data-source @#'sql-jdbc.execute/do-with-resolved-connection-data-source
            closed-conn (proxy [Connection] []
                          (isClosed [] true)
                          (close [] nil))

            new-conn (proxy [Connection] []
                       (isClosed [] false)
                       (close [] nil)
                       (isReadOnly [] true)
                       (getMetaData []
                         (reify DatabaseMetaData
                           (supportsTransactionIsolationLevel [_ _] false)))
                       (createStatement []
                         (reify java.sql.Statement
                           (execute [_ _] true)
                           (close [_] nil)))
                       (setReadOnly [read-only]
                         (vswap! connection-option-calls conj [:setReadOnly read-only]))
                       (setAutoCommit [auto-commit]
                         (vswap! connection-option-calls conj [:setAutoCommit auto-commit]))
                       (setTransactionIsolation [level]
                         (vswap! connection-option-calls conj [:setTransactionIsolation level]))
                       (setHoldability [holdability]
                         (vswap! connection-option-calls conj [:setHoldability holdability]))
                       (setNetworkTimeout [executor timeout-ms]
                         (vswap! connection-option-calls conj [:setNetworkTimeout timeout-ms])))]
        (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                      (fn [driver db options]
                        (if (:keep-open? options)
                          (reify javax.sql.DataSource
                            (getConnection [_] new-conn))
                          (orig-do-with-resolved-connection-data-source driver db options)))

                      sql-jdbc.execute/recursive-connection?
                      (let [original-recursive-fn sql-jdbc.execute/recursive-connection?]
                        (fn []
                          (let [ret (original-recursive-fn)]
                            (vswap! connection-option-calls conj [:recursive-connection-check ret])
                            ret)))]

          (driver/do-with-resilient-connection
           driver/*driver* (mt/id)
           (fn [driver _db]
             (let [result (sql-jdbc.execute/try-ensure-open-conn! driver closed-conn)]
               ;; Should return the new connection
               (is (identical? new-conn result))

               (is (some #(= % [:recursive-connection-check false]) @connection-option-calls))

               ;; Should have set connection options (since it's non-recursive)
               (when is-default-options
                 (let [calls @connection-option-calls]
                   (is (some #(= [:setReadOnly true] %) calls))
                   (is (some #(= [:setAutoCommit true] %) calls))
                   (is (some #(= (first %) :setHoldability) calls))
                   (is (some #(= (first %) :setNetworkTimeout) calls))))))))))))

(deftest is-conn-open-test
  (testing "is-conn-open with valid check"
    (testing "returns true when connection is open and valid"
      (let [conn (reify Connection
                   (isClosed [_] false)
                   (isValid [_ _] true))]
        (is (true? (sql-jdbc.execute/is-conn-open? conn :check-valid? true)))))

    (testing "returns false when connection is closed"
      (let [conn (reify Connection
                   (isClosed [_] true)
                   (isValid [_ _] true))]
        (is (false? (sql-jdbc.execute/is-conn-open? conn :check-valid? true)))))

    (testing "closes connection and returns false when connection is open but not valid"
      (let [close-called? (atom false)
            conn (reify Connection
                   (isClosed [_] @close-called?)
                   (isValid [_ _] false)
                   (close [_] (reset! close-called? true)))]
        (is (false? (sql-jdbc.execute/is-conn-open? conn :check-valid? true)))
        (is (true? @close-called?) "Connection should be closed when invalid")
        (is (true? (.isClosed conn)))))))

(deftest statement-is-closed-test
  (mt/test-drivers (mt/normal-driver-select {:+parent :sql-jdbc})
    (testing "can check isClosed on statement"
      (when (driver/database-supports? driver/*driver* :jdbc/statements nil)
        (sql-jdbc.execute/do-with-connection-with-options
         driver/*driver* (mt/id) nil
         (fn [^Connection conn]
           (let [stmt (sql-jdbc.execute/statement driver/*driver* conn)]
             (is (false? (.isClosed stmt)))
             (.close stmt)
             (is (true? (.isClosed stmt))))))))
    (testing "can check isClosed on prepared statement"
      (sql-jdbc.execute/do-with-connection-with-options
       driver/*driver* (mt/id) nil
       (fn [^Connection conn]
         (let [prepared-stmt (sql-jdbc.execute/prepared-statement driver/*driver* conn "select 1" [])]
           (is (false? (.isClosed prepared-stmt)))
           (.close prepared-stmt)
           (is (true? (.isClosed prepared-stmt)))))))))

(deftest write-op-metric-test
  (testing "write-op counter tracks default connection acquisitions"
    (mt/with-prometheus-system! [_ system]
      (mt/test-drivers (descendants driver/hierarchy :sql-jdbc)
        (sql-jdbc.execute/do-with-connection-with-options
         driver/*driver* (mt/id) nil
         (fn [_conn] nil))
        (is (pos? (mt/metric-value system :metabase-db-connection/write-op
                                   {:connection-type "default"}))))))
  (when config/ee-available?
    (testing "write-op counter tracks write-data connection acquisitions"
      (mt/with-premium-features #{:writable-connection}
        (mt/with-prometheus-system! [_ system]
          (mt/test-drivers (descendants driver/hierarchy :sql-jdbc)
            (let [db (mt/db)]
              (mt/with-temp-vals-in-db :model/Database (:id db) {:write_data_details (:details db)}
                (driver.conn/with-write-connection
                  (sql-jdbc.execute/do-with-connection-with-options
                   driver/*driver* (mt/id) nil
                   (fn [_conn] nil)))
                (is (pos? (mt/metric-value system :metabase-db-connection/write-op
                                           {:connection-type "write-data"})))))))))))

;;; ── request-scoped connection slot (do-with-resolved-connection guard) ───────
;;
;; These tests exercise the new `request.conn/*request-connection*` branch inside
;; `do-with-resolved-connection`.  They are pure unit tests — no real database is
;; required.  `do-with-resolved-connection-data-source` is replaced with a mock
;; DataSource that yields proxy Connections.

(defn- counting-datasource
  "Return a `DataSource` that increments `checkout-count` atom and returns
  `conn-to-yield` on every `.getConnection` call."
  [conn-to-yield checkout-count]
  (reify DataSource
    (getConnection [_]
      (swap! checkout-count inc)
      conn-to-yield)))

(defn- noop-conn
  "A minimal `java.sql.Connection` proxy sufficient for the slot tests.
  Tracks close calls via `close-count` atom."
  []
  (let [close-count (atom 0)]
    {:conn        (proxy [Connection] []
                    (isClosed [] (pos? @close-count))
                    (close    [] (swap! close-count inc)))
     :close-count close-count}))

(deftest ^:parallel request-scoped-slot-inactive-test
  (testing "When *request-connection* is nil the existing pool-checkout path is used"
    (let [{:keys [conn]}   (noop-conn)
          checkout-count   (atom 0)
          received-conn    (atom nil)]
      (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                    (fn [_ _ _] (counting-datasource conn checkout-count))]
        ;; *request-connection* defaults to nil — pool path must be taken
        (sql-jdbc.execute/do-with-resolved-connection :h2 1 {} #(reset! received-conn %)))
      (is (= 1 @checkout-count) "exactly one pool checkout should occur")
      (is (identical? conn @received-conn) "f must receive the pool connection"))))

(deftest ^:parallel request-scoped-slot-first-use-acquires-connection-test
  (testing "First call with active slot acquires a connection and pins it to the atom"
    (let [{:keys [conn]}   (noop-conn)
          checkout-count   (atom 0)
          received-conn    (atom nil)
          conn-atom        (atom nil)]
      (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                    (fn [_ _ _] (counting-datasource conn checkout-count))]
        (binding [request.conn/*request-connection* conn-atom]
          (sql-jdbc.execute/do-with-resolved-connection :h2 1 {} #(reset! received-conn %))))
      (is (= 1 @checkout-count)    "one pool checkout for the first use")
      (is (identical? conn @received-conn) "f must receive the acquired connection")
      (is (identical? conn @conn-atom)     "atom must be pinned to the acquired connection"))))

(deftest ^:parallel request-scoped-slot-second-use-reuses-connection-test
  (testing "Second call with active slot reuses the pinned connection — zero new checkouts"
    (let [{:keys [conn]}   (noop-conn)
          checkout-count   (atom 0)
          calls            (atom [])]
      (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                    (fn [_ _ _] (counting-datasource conn checkout-count))]
        (binding [request.conn/*request-connection* (atom nil)]
          ;; First call: acquires connection
          (sql-jdbc.execute/do-with-resolved-connection :h2 1 {} #(swap! calls conj [:first %]))
          ;; Second call: must reuse, not checkout again
          (sql-jdbc.execute/do-with-resolved-connection :h2 1 {} #(swap! calls conj [:second %]))))
      (is (= 1 @checkout-count) "only one pool checkout across both calls")
      (is (= 2 (count @calls))  "f must be invoked for both calls")
      (is (identical? conn (second (first @calls)))  "first call received the connection")
      (is (identical? conn (second (second @calls))) "second call received the same connection"))))

(deftest ^:parallel connection-key-in-spec-takes-priority-over-slot-test
  (testing "The :connection key in db-or-id-or-spec overrides *request-connection* (existing behaviour preserved)"
    (let [{:keys [conn]}        (noop-conn)
          {spec-conn :conn}     (noop-conn)       ; a different connection via :connection key
          checkout-count        (atom 0)
          received-conn         (atom nil)]
      (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                    (fn [_ _ _] (counting-datasource conn checkout-count))]
        (binding [request.conn/*request-connection* (atom nil)]
          ;; Pass a pre-opened connection via the :connection key
          (sql-jdbc.execute/do-with-resolved-connection :h2 {:connection spec-conn} {} #(reset! received-conn %))))
      (is (= 0 @checkout-count)        "pool must NOT be hit when :connection key is present")
      (is (identical? spec-conn @received-conn) "f must receive the :connection key's connection, not the slot's"))))

(deftest ^:parallel request-scoped-slot-cas-race-test
  (testing "Concurrent first-use: only one connection is pinned; loser's connection is closed"
    ;; Simulate two threads racing to pin a connection to the same empty atom.
    ;; We create two distinct connections; the CAS loser must close its own connection
    ;; and then call f with the winner's connection.
    (let [conn-a-close  (atom 0)
          conn-b-close  (atom 0)
          conn-a        (proxy [Connection] []
                          (isClosed [] (pos? @conn-a-close))
                          (close    [] (swap! conn-a-close inc)))
          conn-b        (proxy [Connection] []
                          (isClosed [] (pos? @conn-b-close))
                          (close    [] (swap! conn-b-close inc)))
          ;; Track which connections f is actually called with
          f-calls       (atom [])
          ;; Shared atom starts empty
          conn-atom     (atom nil)
          ;; Alternate which connection the DataSource yields based on call order
          checkout-n    (atom 0)
          datasource    (reify DataSource
                          (getConnection [_]
                            (if (odd? (swap! checkout-n inc)) conn-a conn-b)))]
      (with-redefs [sql-jdbc.execute/do-with-resolved-connection-data-source
                    (fn [_ _ _] datasource)]
        ;; Run two concurrent invocations against the same atom
        (let [f1 (future
                   (binding [request.conn/*request-connection* conn-atom]
                     (sql-jdbc.execute/do-with-resolved-connection
                      :h2 1 {} #(swap! f-calls conj %))))
              f2 (future
                   (binding [request.conn/*request-connection* conn-atom]
                     (sql-jdbc.execute/do-with-resolved-connection
                      :h2 1 {} #(swap! f-calls conj %))))]
          @f1
          @f2))
      ;; Exactly one connection must have been pinned and used for both calls
      (let [pinned @conn-atom]
        (is (some? pinned) "atom must be populated after concurrent use")
        ;; f was called twice — both times with the same pinned connection
        (is (= 2 (count @f-calls)) "f must be called by both threads")
        (is (every? #(identical? pinned %) @f-calls)
            "both f invocations must receive the pinned connection"))
      ;; Exactly one of the two connections must be closed (the CAS loser)
      (is (= 1 (+ @conn-a-close @conn-b-close))
          "exactly one connection must be closed (the CAS loser's)"))))
