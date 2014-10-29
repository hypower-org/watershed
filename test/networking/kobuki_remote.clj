(ns networking.kobuki-remote
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [taoensso.nippy :as nippy]
            [manifold.deferred :as d]))

(defn commands 
  [string] 
  (cond    
    (= "up" string)
    [100 0]    
    (= "down" string)
    [-100 0]    
    (= "left" string)
    [100 -90]   
    (= "right" string)
    [100 90]   
    :else   
    [0 0]))

(def remote-system    
  (apply w/assemble w/manifold-step w/manifold-connect
         (cons                              
           
           (w/outline :control-data [] (fn [] (s/->source (map commands (take-while #(not (= % "quit!")) (repeatedly read-line))))))
           
           (:system (n/cpu {:ip "10.10.10.5" :neighbors 2 :requires [] :provides [:control-data]})))))




