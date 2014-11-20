(ns goldrausch.twitter
  (:require [com.stuartsierra.component :as component]
            [gezwitscher.core :refer [gezwitscher]]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [datomic.api :as d]
            [clojure.core.async :refer [go go-loop <! >! <!! >!!]]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

(def schema [{:db/id #db/id[:db.part/db]
              :db/ident :publish/at
              :db/valueType :db.type/instant
              :db/cardinality :db.cardinality/one
              :db/index true
              :db/doc "An tweets's publication time"
              :db.install/_attribute :db.part/db}

             {:db/id #db/id[:db.part/db]
              :db/ident :tweet/text
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/fulltext true
              :db/index true
              :db/doc "A tweet's text"
              :db.install/_attribute :db.part/db}

             {:db/id #db/id[:db.part/db]
              :db/ident :tweet/user
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one
              :db/doc "A tweet's user"
              :db.install/_attribute :db.part/db}

             {:db/id #db/id[:db.part/db]
              :db/ident :tweet/id
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one
              :db/doc "A tweet's id"
              :db.install/_attribute :db.part/db}])

(def twitter-date-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))

(defn transact-tweet
  "Transact tweet to datomic"
  [conn {:keys [id user created_at text]}]
  (let [ts (c/to-date (f/parse twitter-date-formatter created_at))]
    (d/transact
     conn
     [{:db/id (d/tempid :db.part/user)
       :tweet/text text
       :tweet/id id
       :publish/at ts
       :tweet/user (:id user)}])))

;; TODO not necessary for component...
(defn get-all-tweets
  "Retrieve all tweets"
  [conn]
  (map
   #(zipmap [:text :user :ts] %)
   (d/q '[:find ?text ?user ?ts
          :where
          [?t :tweet/text ?text]
          [?t :tweet/user ?user]
          [?t :publish/at ?ts]]
        (d/db conn))))

(defrecord TwitterCollector [follow track credentials init-schema?]
  component/Lifecycle
  (start [component]
    (if (:in component) ;; make idempotent
      component
      (let [{:keys [follow track credentials db init-schema?]} component
            [in out] (gezwitscher credentials)]
        (when init-schema?
          (d/transact (:conn db) schema))
        (>!! in {:topic :start-stream :track track :follow follow})
        (info "Start crawling " track)
        (let [output (<!! out)]
          (go-loop [status (<! (:status-ch output))]
            (when status
              (debug "Twitter status:" status)
              (try
                (transact-tweet (:conn db) status)
                (catch Exception e
                  (error "transaction error: " status e)))
              (recur (<! (:status-ch output))))))
        (assoc component :in in :out out))))
  (stop [component]
    (if-not (:in component) ;; make idempotent
      component
      (do
        (>!! (:in component) {:topic :stop-stream})
        (assoc component :in nil :out nil)))))

(defn new-twitter-collector [config]
  (map->TwitterCollector config))
