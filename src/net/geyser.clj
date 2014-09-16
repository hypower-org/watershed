(ns net.geyser
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [manifold.stream :as s])
  (:use [clojure.string :only (join split)]))

;UDP broadcast client

(defrecord Geyser [port frame sink source] 
  
  w/ITide
  
  (w/flow   
    
    [_]
    
    (d/let-flow [udp-socket (aleph-udp/udp-socket {:port port :frame frame :broadcast? true})]     
      
      (s/connect (s/->source udp-socket) sink)
	      
      (s/connect source (s/->sink udp-socket))
      
      (s/on-closed sink (fn [] (s/close! source) (lamina/force-close udp-socket)))
      
      (s/on-closed source (fn [] (s/close! sink) (lamina/force-close udp-socket)))
      
      _))
  
  (w/ebb 
    [_]
    
    (s/close! sink)))

(defn geyser [port frame]
  
  (->Geyser port frame (s/stream) (s/stream)))




















