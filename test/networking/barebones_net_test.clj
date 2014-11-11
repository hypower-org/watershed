(ns networking.barebones-net-test
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

(defn -main
  [ip]
  
  ;Provide initial data.
  
  (let [sys (:system (n/cpu {:ip ip :neighbors 2 :requires [] :provides []}))
        
        c-sys (->>
      
                sys
      
                (apply w/assemble w/manifold-step w/manifold-connect))]
    
    
    (let [status (n/find-first #(= (:title %) :system-status) c-sys)]
      
      (if status
        @(:stream status))
    
    
    
    
    
    ))
    

