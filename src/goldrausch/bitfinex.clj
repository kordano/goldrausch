(ns goldrausch.bitfinex
  (:require [clojure.core.async :as async
             :refer [<!! >!! <! >! timeout chan alt! go go-loop]]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [http.async.client :as cli]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

;; https://www.bitfinex.com/pages/api
;; use separate schema

(def client (cli/create-client))

(def schema [{:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/timestamp
              :db/valueType :db.type/instant
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}

             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/mid
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/bid
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/ask
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/high
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/low
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/last-price
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/volume
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/provider
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/doc "Unique name of the coin data provider."
              :db/index true
              :db.install/_attribute :db.part/db}

             ;; bitfinex-btcusd60
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/bids
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/asks
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/price
              :db/valueType :db.type/double
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :bitfinex-btcusd/amount
              :db/valueType :db.type/double
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}])

(defn pubticker-btcusd []
  (let [{mid "mid" bid "bid" ask "ask" last-price "last_price"
         low "low" high "high" volume "volume" timestamp "timestamp" :as tick}
        (-> (cli/GET client "https://api.bitfinex.com/v1/pubticker/btcusd")
            cli/await
            cli/string
            json/read-str)
        id (d/tempid :db.part/user)]
    [[:db/add id :bitfinex-btcusd/provider "bitfinex_pubticker"]
     [:db/add id :bitfinex-btcusd/mid (Float/parseFloat mid)]
     [:db/add id :bitfinex-btcusd/bid (Float/parseFloat bid)]
     [:db/add id :bitfinex-btcusd/ask (Float/parseFloat ask)]
     [:db/add id :bitfinex-btcusd/high (Float/parseFloat high)]
     [:db/add id :bitfinex-btcusd/low (Float/parseFloat low)]
     [:db/add id :bitfinex-btcusd/last-price (Float/parseFloat last-price)]
     [:db/add id :bitfinex-btcusd/volume (Float/parseFloat (str/replace volume #"," ""))]
     [:db/add id :bitfinex-btcusd/timestamp (java.util.Date. (long (* 1000 (Double/parseDouble timestamp))))]]))


(defn book-btcusd []
  (let [{asks "asks" bids "bids"} (-> (cli/GET client "https://api.bitfinex.com/v1/book/btcusd")
                                      cli/await
                                      cli/string
                                      json/read-str)
        id (d/tempid :db.part/user)]
    (vec (concat (mapcat (fn [{price "price" amount "amount" timestamp "timestamp"}]
                           (let [bid-id (d/tempid :db.part/user)]
                             [[:db/add id :bitfinex-btcusd/bids bid-id]
                              [:db/add bid-id :bitfinex-btcusd/price (Double/parseDouble price)]
                              [:db/add bid-id :bitfinex-btcusd/amount (Double/parseDouble amount)]
                              [:db/add bid-id :bitfinex-btcusd/timestamp (java.util.Date. (long (* 1000 (Double/parseDouble timestamp))))]])) bids)
                 (mapcat (fn [{price "price" amount "amount" timestamp "timestamp"}]
                           (let [ask-id (d/tempid :db.part/user)]
                             [[:db/add id :bitfinex-btcusd/asks ask-id]
                              [:db/add ask-id :bitfinex-btcusd/price (Double/parseDouble price)]
                              [:db/add ask-id :bitfinex-btcusd/amount (Double/parseDouble amount)]
                              [:db/add ask-id :bitfinex-btcusd/timestamp (java.util.Date. (long (* 1000 (Double/parseDouble timestamp))))]])) asks)
                 [[:db/add id :bitfinex-btcusd/provider "bitfinex_btcusd"]]))))

(def supported-chans {"pubticker/btcusd" pubticker-btcusd
                      "book/btcusd" book-btcusd})


(defrecord BitfinexCollector [subscribed-chans db init-schema?]
  component/Lifecycle
  (start [component]
    (if (:close-ch component) ;; idempotent
      component
      (let [conn (:conn db)
            close-ch (chan)]
        (when init-schema?
          (debug "initializing schema:" schema)
          (d/transact conn schema))

        (go-loop []
          (debug "querying bitfinex")
          (alt! close-ch
                :done
                (timeout 10000)
                (do
                  (doseq [c subscribed-chans]
                    (async/thread
                      (d/transact conn ((supported-chans c)))))
                  (recur))))
        (assoc component :close-ch close-ch))))
  (stop [component]
    (if-not (:close-ch component)
      component
      (do
        (async/close! (:close-ch component))
        (dissoc component :close-ch)))))




(defn new-bitfinex-collector [{:keys [subscribed-chans] :as config}]
  (when-not (set/superset? (set (keys supported-chans)) (set subscribed-chans))
    (throw (ex-info "Some channels to subscribe are unknown:"
                    {:supported-chans (set (keys supported-chans))
                     :subscribed-chans subscribed-chans})))
  (map->BitfinexCollector config))


(comment
  (def foo (-> (cli/GET client "https://api.bitfinex.com/v1/book/btcusd")
               cli/await
               cli/string
               json/read-str)))
