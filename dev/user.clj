(ns user
  (:require [reloaded.repl :refer [system init start stop go reset]]
            [com.stuartsierra.component :as component]
            [goldrausch.twitter :refer [get-all-tweets]]
            [goldrausch.core :refer [prod-system]]
            [datomic.api :as d]))

;; how access system value then?
#_(reloaded.repl/set-init! prod-system)

(def sys (atom (prod-system (read-string (slurp "resources/config.edn")))))

(comment
  (swap! sys component/start)
  (swap! sys component/stop)

  (get-all-tweets (:conn (:db @sys))))
