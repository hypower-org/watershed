(ns net.faucet
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [watershed.utils :as t]
            [manifold.stream :as s])
  (:use [clojure.string :only (join split)]))

;TCP Client

(defprotocol IFaucet 
  (connect [_ interval timeout]))


(defrecord Faucet [title host port frame source sink]
  
  IFaucet 
  
  (connect
    [_ interval timeout]
    
    (let [connector (fn this [start-time interval timeout]   
                      
                      (Thread/sleep interval)
                                
                      (try 
                                                   
                        (lamina/wait-for-result (aleph/tcp-client {:host host,
                                                                   :port port,
                                                                   :frame frame}))
                                        
                        (catch java.net.ConnectException e
                                            
                          (if (> (t/time-passed start-time) timeout)
                            e
                            #(this start-time interval timeout)))))]
           
      (d/let-flow [connection (d/future (trampoline connector (t/time-now) interval timeout))]
                
        (if (lamina/channel? connection)
             
          (do           
            
            (lamina/enqueue connection (str title))          
            
            (s/connect (s/->source connection) sink)            
            (s/connect source (s/->sink connection))
            
            (s/on-closed sink (fn [] (s/close! source) (lamina/force-close connection)))
            (s/on-closed source (fn [] (s/close! sink) (lamina/force-close connection))))))))
  
  w/ITide
  
  (w/flow
    [_]
     (connect _ 1000 10000))
  
  (w/ebb 
    [_]
    
    (s/close! sink)))

(defn faucet 
  
  [title host port frame]
  
  (->Faucet title host port frame (s/stream) (s/stream)))






