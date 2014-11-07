(ns networking.beagle-bone-client
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [taoensso.nippy :as nippy]
            [manifold.deferred :as d])
  (:gen-class))

(defn -main
  [ip]
  (def kobuki-client 
    (apply w/assemble w/manifold-step  w/manifold-connect       
           (cons   
             (w/outline :controller [:control-data] (fn [stream] (s/consume (fn [[v w]] (println "Receiving command: " v " m/s " w " rad/s")) stream)))    
             (:system (n/cpu {:requires [:control-data] :provides [] :ip ip :neighbors 2}))))))




