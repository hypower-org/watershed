(ns networking.barebones-net-test
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

(defn -main
  [ip]
  
  ;Provide initial data.
  
  (loop [sys (:system (n/cpu {:ip ip :neighbors 2 :requires [] :provides []}))
        
         c-sys (->>
      
                 sys
      
                 (apply w/assemble w/manifold-step w/manifold-connect))]
    
    
    (let [status (n/find-first #(= (:title %) :system-status) c-sys)]
      
      (when status
        (println status))
      
      (when (and status (= (:connection-status @(:output status)) :net.physi-server/disconnected))
        (println "Connection lost!  Reconnecting...")
        (recur (:system (n/cpu {:ip ip :neighbors 2 :requires [] :provides []}))
               (->>
      
                 sys
      
                 (apply w/assemble w/manifold-step w/manifold-connect)))))))
    

