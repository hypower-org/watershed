(ns watershed.client-test
  (:require [lamina.core :as lamina]
          [aleph.udp :as aleph-udp]
          [gloss.core :as gloss]
          [aleph.tcp :as aleph]
          [manifold.deferred :as d]
          [watershed.core :as w]
          [watershed.faucet :as f]
          [net.aqueduct :as a]
          [manifold.stream :as s]))

(def coral-client (f/faucet :coral "10.10.10.5" 10000 (gloss/string :utf-8 :delimiters ["\r\n"])))

(w/flow coral-client)

(s/consume #(println "Coral client: " %) (:sink coral-client))

(s/put! (:source coral-client) "1")



