(ns jdbc.chdb.durable.observation
  "Closed, redacted persistence evidence for one recovered Durable handle.

  This namespace deliberately owns a small atom which never contains a
  backend, head, reference, SQL/WAL payload, dbspec, exception, or native
  handle.  It is not a control-plane cache and it never reads storage.  The
  public projection is consequently useful to embedded consumers without
  becoming a second, weaker Durable protocol API."
  (:require [jdbc.chdb.durable.head :as head]))

(def ^:private unavailable
  {:availability :unavailable})

(defn- safe-sequence [role document]
  (try
    ;; `commit-reference!` returns a canonical validated control document.
    ;; Revalidate at this capability boundary so a raw operation override
    ;; cannot manufacture a public witness with a merely plausible map.
    (head/validate! document (if (= role :reader) :read-only :writer))
    (let [sequence (get-in document ["manifest" "seq"])]
      (when (and (integer? sequence)
                 (<= 0 sequence head/max-safe-integer))
        sequence))
    (catch Throwable _ nil)))

(defn- recovered-state [role document]
  ;; A reader's recovered immutable snapshot is intentionally not a claim that
  ;; it is the current object-store head. A writer has a recovered owned view,
  ;; but only an eventual canonical commit establishes a persistence witness.
  (if-let [sequence (safe-sequence role document)]
    {:availability :available
     :role role
     :state (if (= role :reader) :snapshot :recovered)
     :view-current? (= role :writer)
     :confirmed-boundary :recovered
     :confirmed-sequence sequence
     :last-successful-persistence :unavailable}
    unavailable))

(defn start
  "Create the one private observation atom for a recovered Durable handle.

  An invalid or absent recovery document is represented as unavailable rather
  than guessed. Public open supplies the validated document; raw start seams
  may therefore opt out without gaining a status claim."
  [role recovered-document]
  (atom (recovered-state role recovered-document)))

(defn projection
  "Return only the closed public observation vocabulary.

  This is a pure dereference/projection: it performs no backend I/O and cannot
  alter writer admission, native execution, publication, or cleanup."
  [observation]
  (let [value @observation]
    (if (= :available (:availability value))
      (select-keys value [:availability :role :state :view-current?
                          :confirmed-boundary :confirmed-sequence
                          :last-successful-persistence])
      unavailable)))

(defn unavailable! [observation]
  (reset! observation unavailable)
  nil)

(defn pending! [observation]
  (swap! observation
         (fn [state]
           (if (= :available (:availability state))
             ;; A later native mutation cannot make an earlier ambiguous WAL
             ;; provable. Keep the conservative status until a checkpoint
             ;; covers the complete native state.
             (assoc state
                    :state (if (= :unconfirmed (:state state))
                             :unconfirmed
                             :pending)
                    :view-current? false)
             state)))
  nil)

(defn unconfirmed! [observation]
  (swap! observation
         (fn [state]
           (if (= :available (:availability state))
             (assoc state :state :unconfirmed :view-current? false)
             state)))
  nil)

(defn- landed? [kind reference document]
  (let [manifest (get document "manifest")]
    (case kind
      :wal (= reference (peek (get manifest "wal")))
      :checkpoint (and (= reference (get manifest "base"))
                       (empty? (get manifest "wal")))
      false)))

(defn confirmed!
  "Record a canonical committed/reconciled control result, if and only if it
  carries a validated public sequence and the exact reference landed.

  A later WAL is deliberately powerless to clear `:unconfirmed`: an earlier
  ambiguous WAL may still be present. A confirmed checkpoint includes the
  entire local native state and clears that uncertainty."
  [observation kind reference result]
  (let [document (:head result)
        sequence (safe-sequence :writer document)
        confirmed? (and (contains? #{:committed :reconciled} (:status result))
                        sequence
                        (landed? kind reference document))]
    (when confirmed?
      (swap! observation
             (fn [state]
               (cond
                 (not= :available (:availability state)) state
                 (and (= :unconfirmed (:state state)) (= :wal kind)) state
                 :else
                 (assoc state
                        :state :confirmed
                        :view-current? true
                        :confirmed-boundary kind
                        :confirmed-sequence sequence
                        :last-successful-persistence kind)))))
    confirmed?))
