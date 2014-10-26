(defproject goldrausch "0.1.0-SNAPSHOT"
  :description "Twitter crawler scanning for bitcoin mentions"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [clj-time "0.7.0"]

                 [gezwitscher "0.1.1-SNAPSHOT"]
                 [com.datomic/datomic-free "0.9.4899"]

                 [aprint "0.1.0"]
                 [com.taoensso/timbre "3.2.1"]]

  :main goldrausch.core

  )
