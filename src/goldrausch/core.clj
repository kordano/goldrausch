(ns goldrausch.core
  (:gen-class :main true)
  (:require [gezwitscher.core :refer [gezwitscher]]
            [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.core.async :refer [go go-loop <! >! <!! >!!]]
            [aprint.core :refer [aprint]]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [taoensso.timbre :as timbre])
  (:import datomic.Util))

(def db-uri-base "datomic:free://0.0.0.0:4334")

(def server-state (atom nil))


;; --- basic datomic interaction ---

(def custom-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))

(defn read-all
  "Read all forms in f, where f is any resource that can
   be opened by io/reader"
  [f]
  (Util/readAll (io/reader f)))


(defn transact-all
  "Load and run all transactions from f, where f is any
   resource that can be opened by io/reader."
  [conn f]
  (doseq [txd (read-all f)]
    (d/transact conn txd))
  :done)


(defn db-conn []
  (let [uri (str db-uri-base "/goldrausch")]
    (d/create-database uri)
    (d/connect uri)))


(defn- scratch-conn
  "Create a connection to an anonymous, in-memory database."
  []
  (let [uri (str "datomic:mem://" (d/squuid))]
    (d/delete-database uri)
    (d/create-database uri)
    (d/connect uri)))


(defn transact-tweet
  "Transact tweet to datomic"
  [conn {:keys [id user created_at text]}]
  (let [ts (c/to-date (f/parse custom-formatter created_at))]
    (d/transact
     conn
     [{:db/id (d/tempid :db.part/user)
       :tweet/text text
       :tweet/id id
       :publish/at ts
       :tweet/user (:id user)}])))


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

;; --- twitter crawling ---

(defn start-stream
  "Start crawling from twitter streaming API"
  [state]
  (let [{:keys [follow track credentials]} (:app @state)
        [in out] (gezwitscher credentials)]
    (>!! in {:topic :start-stream :track track :follow follow})
    (println (str "Start crawling " track))
    (let [output (<!! out)]
      (go-loop [status (<! (:status-ch output))]
        (when status
          (transact-tweet (:conn @state) status)
          (recur (<! (:status-ch output))))))
    [in out]))


(defn initialize
  "Initialize server state using a configuration file"
  [state path]
  (let [conn (db-conn)
        local-config (-> path slurp read-string)]
    (swap! state merge (assoc-in local-config [:conn] conn))
    state))


(defn -main [& args]
  (initialize server-state (first args))
  (when (:cold-start @server-state)
    (transact-all (:conn @server-state) (io/resource (:schema @server-state))))
  (start-stream server-state))


(comment

  (initialize server-state "resources/test-config.edn")

  (transact-all (:conn @server-state) (io/resource "schema.edn"))

  (def g (start-stream server-state))

  (>!! (first g) {:topic :stop-stream})

  )
