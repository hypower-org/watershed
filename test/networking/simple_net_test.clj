(ns networking.simple-net-test
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

(defn -main
  [ip requires provides]
  
  ;Provide initial data.
  
  (let [sys (:system (n/cpu {:ip ip :neighbors 2 :requires [requires] :provides [provides]}))]
    
    (->>
      
      sys
      
      (cons (w/outline provides [requires] (fn [stream] (s/map (fn [x] provides) stream))))
      
      (cons (w/outline :printing [requires] (fn [stream] (s/consume println (s/map identity stream)))))
      
      (apply w/assemble w/manifold-step w/manifold-connect))))
    

