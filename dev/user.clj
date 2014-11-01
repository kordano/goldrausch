(ns user
  (:require [reloaded.repl :refer [system init start stop go reset]]
            [com.stuartsierra.component :as component]
            [goldrausch.twitter :refer [get-all-tweets]]
            [goldrausch.core :refer [dev-system]]))

;; how access system value then?
#_(reloaded.repl/set-init! dev-system)

(def sys (atom (dev-system (read-string (slurp "resources/config.edn")))))

(comment
  (swap! sys component/start)
  (swap! sys component/stop)

  (get-all-tweets (:conn (:db @sys))))
