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
          
  (def test-kernel (phy/kernel ip n :max-power 10 :target-power 5))  
  (def assembled (w/assemble test-kernel)))









