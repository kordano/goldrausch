(ns goldrausch.core
  (:gen-class :main true)
  (:require [goldrausch.twitter :refer [new-twitter-collector get-all-tweets]]
            [goldrausch.okcoin :refer [new-okcoin-collector]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.core.async :refer [go go-loop <! >! <!! >!!]]
            [aprint.core :refer [aprint]]
            [taoensso.timbre :as timbre]
            (system.components [datomic :refer [new-datomic-db]])))

(defn dev-system [config]
  (component/system-map
   :db (new-datomic-db (str "datomic:mem://" (d/squuid)))
   :twitter-collector
   (component/using
    (new-twitter-collector (config :twitter))
    {:db :db})
   :okcoin-collector
   (component/using
    (new-okcoin-collector (config :okcoin))
    {:db :db})))

(defn -main [config-filename & args]
  (dev-system (read-string (slurp config-filename))))
