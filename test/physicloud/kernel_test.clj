(ns physicloud.kernel-test
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.networking :as net]
            [net.faucet :as f]
            [physicloud.core :as phy]
            [physicloud.quasi-descent :as q]
            [watershed.utils :as u]
            [clojure.pprint :as p]
            [manifold.stream :as s]))

(defn start 
  [ip n]
          
  (def test-kernel (phy/kernel ip n [1000 1000 2000] :target-power 5 :max-power 8 :idle-power 3 :bcps 1000000)))








