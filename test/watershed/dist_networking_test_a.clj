(ns watershed.dist-networking-test-a
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.networking :as net]
            [net.faucet :as f]
            [watershed.utils :as u]
            [clojure.pprint :as p]
            [manifold.stream :as s]))

(defn start 
  
  []
  
  (def test-sys (-> 
    
                  @(net/cpu :10.10.10.5 {:10.10.10.5 {:edges [:10.10.10.3]} :10.10.10.3 {:edges [:10.10.10.5]}} 2 :provides [:cpu-1-data] :requires [:cpu-2-data])
    
                  (merge {:test {:tributaries [:cpu-2-data] :sieve (fn [stream] (s/consume println stream))
                                 :type :estuary}
            
                          :cpu-1-data {:tributaries [] :sieve (fn [] (s/periodically 1000 (fn [] (u/time-now))))
                                       :type :source}})
                                                          
                  w/compile*)))


  
  
  



