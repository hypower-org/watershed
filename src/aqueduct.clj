(ns aqueduct
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed :as w]
            [manifold.stream :as s])
  (:use [clojure.string :only (join split)]))

(defprotocol ITide
  (flow [_])
  (ebb [_]))

(defn- handler [ch client-info aqueduct]
  
  (lamina/receive
    
    ch

    (fn [x]
      (let [msg (read-string x)]        
             
        (s/put! (:sink (msg aqueduct)) {msg ch}))))) 

(defrecord Aqueduct [aqueduct]
  
  ITide
  
  (flow [_]
    
    (let [server (aleph/start-tcp-server (fn [ch client-info] (handler ch client-info aqueduct)) {:port 10000, :frame (gloss/string :utf-8 :delimiters ["\r\n"])})]     
      
      (d/let-flow [connections (apply d/zip (map s/take! (map :sink (vals aqueduct))))]        
        
        (let [streams (reduce merge connections)]         
          
          (doseq [k (keys aqueduct)]           
            
            (s/consume (fn [x] (lamina/enqueue (k streams) x)) (:source (k aqueduct)))
            
            (lamina/receive-all (k streams) (fn [x] (s/put! (:sink (k aqueduct)) x))))
          
          (doseq [s (keys aqueduct)]
            (s/on-closed (:sink (s aqueduct)) (fn [] (server) (lamina/close (s streams))))
            (s/on-closed (:source (s aqueduct)) (fn [] (server) (lamina/close (s streams)))))))))
  
  (ebb [_]
    
    (doseq [s (vals aqueduct)]
      (s/close! s))))


(defn aqueduct [rivers]
  
  (->Aqueduct (zipmap rivers (repeatedly (count rivers) (fn [] {:sink (s/stream) :source (s/stream)})))))



                                         






















