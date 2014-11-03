(defproject goldrausch "0.1.0-SNAPSHOT"
  :description "Twitter crawler scanning for bitcoin mentions"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.stuartsierra/component "0.2.2"]
                 [org.danielsz/system "0.1.3"]
                 [com.datomic/datomic-free "0.9.4899"]
                 [aprint "0.1.0"]
                 [com.taoensso/timbre "3.2.1"]

                 ;; twitter
                 [gezwitscher "0.1.1-SNAPSHOT"]
                 [clj-time "0.7.0"]

                 ;; okcoin
                 [http.async.client "0.5.2"]
                 [org.clojure/data.json "0.2.5"]]

  :main goldrausch.core)
