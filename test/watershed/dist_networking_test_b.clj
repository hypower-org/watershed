(ns watershed.dist-networking-test-b
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
            [watershed.utils :as u]
            [manifold.stream :as s]))

(defn- ping 
  
  [time] 
  
  (u/time-passed time))

(defn start 
  
  []
  
  (def test-sys (-> 
    
                  @(net/cpu :10.10.10.3 {:10.10.10.5 {:edges [:10.10.10.3]} :10.10.10.3 {:edges [:10.10.10.5]}} 2 :provides [:cpu-2-data] :requires [:cpu-1-data])
    
                  (merge {:test {:tributaries [:cpu-1-data] :sieve (fn [stream] (s/consume println stream))
                                 :type :estuary}
            
                          :cpu-2-data {:tributaries [:cpu-1-data] :sieve (fn [stream] (s/map ping stream))
                                       :type :river}
            
                          })
                                                          
                  w/compile*)))
  
  
  



