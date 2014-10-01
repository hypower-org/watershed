(ns networking.networking-test
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.networking :as net]
            [net.faucet :as f]
            [clojure.pprint :as p]
            [manifold.stream :as s]))

(def cpu-1 (f/faucet :cpu-1 "10.10.10.5" 10000 (gloss/string :utf-8 :delimiters ["\r\n"])))

(def cpu-2 (f/faucet :cpu-2 "10.10.10.5" 10000 (gloss/string :utf-8 :delimiters ["\r\n"])))

(w/flow cpu-1)

(w/flow cpu-2)

(def test-framework (net/monitor {:cpu-1 {:edges [:cpu-2]} :cpu-2 {:edges [:cpu-1]}}))

(s/consume #(println "CPU-1: " %) (:sink cpu-1))

(s/consume #(println "CPU-2: " %) (:sink cpu-2))

(s/put! (:source cpu-1) "1")

(s/put! (:source cpu-2) "2")




