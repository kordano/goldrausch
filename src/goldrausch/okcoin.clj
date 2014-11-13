(ns goldrausch.okcoin
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

;; possible sources

;; for bitcoin average

;; Data for USD weighted average
;; exchange 	volume % 	volume ฿ 	last price
;; Bitfinex 	54.66% 	25,119.23 	335.66 USD
;; Bitstamp 	27.99% 	12,861.06 	336.01 USD
;; BTC-e 	12.64% 	5,807.67 	332.99 USD
;; LocalBitcoins 	3.71% 	1,704.36 	373.86 USD
;; Hitbtc 	0.56% 	255.26 	336.42 USD
;; Cointrader 	0.33% 	153.26 	336.00 USD

;; Bitex.la 	0.04% 	16.53 	340.00 USD
;; CampBX 	0.04% 	20.65 	349.00 USD
;; Kraken 	0.02% 	8.87 	335.07 USD
;; Rock Trading 	0.01% 	6.89 	338.00 USD
;; BitKonan 	< 0.01% 	0.82 	345.00 USD
;; Cryptonit 	< 0.01% 	1.27 	348.65 USD
;; Vault of Satoshi 	< 0.01% 	0.00 	439.78 USD
;; Vircurex 	< 0.01% 	0.11 	551.00 USD

;; https://www.bitfinex.com/pages/api
;; https://api.bitfinex.com/v1/pubticker/btcusd

(defn client-connect!
  "Connects to url. Puts [in out] channels on return channel when ready.
Only supports websocket at the moment, but is supposed to dispatch on
protocol of url. "
  [url]
  (let [host (.getHost (java.net.URL. (str/replace url #"^ws" "http"))) ; HACK
        http-client (cli/create-client) ;; TODO use as singleton var?
        in (chan)
        out (chan)
        opener (chan)]
    (go-loop []
      (let [close-ch (chan)]
        (cli/websocket http-client url
                       :open (fn [ws]
                               (info "ws-opened" ws)
                               (go-loop [m (<! out)]
                                 (if m
                                   (do
                                     (debug "client sending msg to:" url m)
                                     (cli/send ws :text (json/write-str m))
                                     (recur (<! out)))
                                   (do
                                     (async/put! close-ch :shutdown)
                                     (cli/close ws))))
                               (async/put! opener [in out])
                               (async/close! opener))
                       :text (fn [ws ms]
                               (let [m (json/read-str ms)]
                                 (debug "client received msg from:" url m)
                                 (async/put! in (with-meta m {:host host}))))
                       :close (fn [ws code reason]
                                (info "closing" ws code reason)
                                (async/close! close-ch))
                       :error (fn [ws err] (error err "ws-error" url)
                                (async/close! opener)
                                (async/close! close-ch)))
        (<! (timeout 60000))
        (when-not (= (<! close-ch) :shutdown)
          (recur)))) ;; wait on unblocking close
    opener))

(def schema [{:db/id #db/id[:db.part/db]
              :db/ident :btcusd/timestamp
              :db/valueType :db.type/instant
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}

             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/buy
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/sell
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/high
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/low
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/last
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/vol
              :db/valueType :db.type/float
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/provider
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/doc "Unique name of the coin data provider."
              :db/index true
              :db.install/_attribute :db.part/db}

             ;; btcusd60
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/bids
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/asks
              :db/valueType :db.type/ref
              :db/cardinality :db.cardinality/many
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/bid
              :db/valueType :db.type/double
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/ask
              :db/valueType :db.type/double
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}
             {:db/id #db/id[:db.part/db]
              :db/ident :btcusd/depth
              :db/valueType :db.type/double
              :db/cardinality :db.cardinality/one
              :db/index true
              :db.install/_attribute :db.part/db}])

(defn btcusd-tick->trans [json-tick]
  (let [{buy "buy" sell "sell" high "high" low "low" last "last" timestamp "timestamp" vol "vol"}
        (get json-tick "data")
        id (d/tempid :db.part/user)]
    [[:db/add id :btcusd/provider (get json-tick "channel")]
     [:db/add id :btcusd/buy (Float/parseFloat buy)]
     [:db/add id :btcusd/sell (Float/parseFloat sell)]
     [:db/add id :btcusd/high (Float/parseFloat high)]
     [:db/add id :btcusd/low (Float/parseFloat low)]
     [:db/add id :btcusd/last (Float/parseFloat last)]
     [:db/add id :btcusd/vol (Float/parseFloat (str/replace vol #"," ""))]
     [:db/add id :btcusd/timestamp (java.util.Date. (Long/parseLong timestamp))]]))

(defn btcusd-depth60->trans [json-tick]
  (let [{bids "bids" asks "asks" timestamp "timestamp"}
        (get json-tick "data")
        id (d/tempid :db.part/user)]
    (vec (concat (mapcat (fn [[bid depth]]
                           (let [bid-id (d/tempid :db.part/user)]
                             [[:db/add id :btcusd/bids bid-id]
                              [:db/add bid-id :btcusd/bid (double bid)]
                              [:db/add bid-id :btcusd/depth (double depth)]])) bids)
                 (mapcat (fn [[ask depth]]
                           (let [ask-id (d/tempid :db.part/user)]
                             [[:db/add id :btcusd/asks ask-id]
                              [:db/add ask-id :btcusd/ask (double ask)]
                              [:db/add ask-id :btcusd/depth (double depth)]])) asks)
                 [[:db/add id :btcusd/provider (get json-tick "channel")]
                  [:db/add id :btcusd/timestamp (java.util.Date. (Long/parseLong timestamp))]]))))

(defn tick->trans [tick]
  (case (get tick "channel")
    "ok_btcusd_ticker" (btcusd-tick->trans tick)
    "ok_btcusd_depth60" (btcusd-depth60->trans tick)
    (info "OKCoin tick unknown:" tick)))

(defrecord OKCoinCollector [websocket-endpoint subscribed-chans db init-schema?]
  component/Lifecycle
  (start [component]
    (if (:in component) ;; idempotent
      component
      (let [[in out] (<!! (client-connect! (or websocket-endpoint
                                               "wss://real.okcoin.com:10440/websocket/okcoinapi")))
            conn (:conn db)]
        (when init-schema?
          (debug "initializing schema:" schema)
          (d/transact conn schema))
        (doseq [c subscribed-chans]
          (>!! out {:event "addChannel" :channel c}))
        (go-loop [ticks (<! in)]
          (when ticks
            (debug "transacting ticks:" ticks)
            (d/transact conn (mapcat tick->trans ticks))
            (recur (<! in))))
        (assoc component :in in :out out))))
  (stop [component]
    (if-not (:in component)
      component
      (do
        (async/close! (:out component))
        (dissoc component :in :out)))))

(def supported-chans #{"ok_btcusd_ticker" "ok_btcusd_depth60"})

(defn new-okcoin-collector [{:keys [subscribed-chans] :as config}]
  (when-not (set/superset? supported-chans (set subscribed-chans))
    (throw (ex-info "Some channels to subscribe are unknown:"
                    {:supported-chans supported-chans
                     :subscribed-chans subscribed-chans})))
  (map->OKCoinCollector config))


(comment
  "wss://real.okcoin.com:10440/websocket/okcoinapi"
  "ok_btcusd_ticker"
  "ok_btcusd_depth60"

  (defn create-db []
    (let [uri "datomic:mem://coindata"
          _ (d/delete-database uri)
          db (d/create-database uri)
          conn (d/connect uri)]
      (d/transact conn schema)
      conn))

  (def conn (d/connect "datomic:free://aphrodite:4334/goldrausch"))



  (let [[_ out] api-sockets]
    (async/close! out))
  (d/transact conn schema)

  (d/transact conn
              (-> (json/read-str "[{
  \"channel\":\"ok_btcusd_depth60\",
  \"data\":{
        \"bids\":[[2473.88,2.025],
                [2473.5,2.4],
		        [2470,12.203]],
        \"asks\":[[2484,17.234],
                [2483.01,6],
		        [2482.88,3]],
  \"timestamp\":\"1411718972024\"}
}]")
                  first
                  btcusd-depth60->trans))

  (d/q '[:find ?p ?buy
         :where
         [?p :btcusd/buy ?buy]]
       (d/db (:conn (:db @sys)))))
