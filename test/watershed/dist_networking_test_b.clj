(ns watershed.dist-networking-test-a
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.physicloud :as phy]
            [net.faucet :as f]
            [clojure.pprint :as p]
            [manifold.stream :as s]))

(def cpu 
  
  (-> 
    
    @(phy/cpu :10.10.10.6 {:10.10.10.5 {:edges [:10.10.10.6]} :10.10.10.6 {:edges [:10.10.10.5]}} :provides [:cpu-2-data] :requires [:cpu-1-data])
    
    (w/add-river (w/estuary :test [:cpu-1-data] (fn [stream] (s/consume println stream))))
                                                          
    w/flow))
  
  
  



