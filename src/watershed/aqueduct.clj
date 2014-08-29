(ns watershed.aqueduct
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [manifold.stream :as s])
  (:use [clojure.string :only (join split)]))

(defn- handler [ch client-info aqueduct]
  
  (lamina/receive
    
    ch

    (fn [x]
      (let [msg (read-string x)]        
             
        (s/put! (:sink (msg aqueduct)) {msg ch}))))) 

(defrecord Aqueduct [aqueduct port frame]
  
  w/ITide
  
  (w/flow [_]
    
    (let [server (aleph/start-tcp-server (fn [ch client-info] (handler ch client-info aqueduct)) {:port port, :frame frame})]     
      
      (d/let-flow [connections (apply d/zip (map s/take! (map :sink (vals aqueduct))))]        
        
        (let [streams (reduce merge connections)]           
          
          (doseq [k (keys aqueduct)]      
            
            (let [stream (k streams)
                  
                  aq (k aqueduct)
                  
                  sink (:sink aq)
                  
                  source (:source aq)]
            
              (s/connect (s/->source stream) sink)
            
              (s/connect source (s/->sink stream))     
              
              (s/on-closed sink (fn [] (server) (s/close! source) (lamina/close stream)))
            
              (s/on-closed source (fn [] (server) (s/close! sink) (lamina/close stream)))))))))
  
  (w/ebb [_]
    
    (doseq [s (vals aqueduct)]
      (s/close! s))))


(defn aqueduct [rivers port frame]
  
  (->Aqueduct (zipmap rivers (repeatedly (count rivers) (fn [] {:sink (s/stream) :source (s/stream)}))) port frame))



