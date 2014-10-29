(ns networking.kobuki-remote
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [taoensso.nippy :as nippy]
            [manifold.deferred :as d]))

(def remote-system    
  (apply w/assemble w/manifold-step w/manifold-connect
         (cons           
           (w/outline :control-data [] (fn [] (s/->source (take-while #(not (= % "quit!")) (repeatedly read-line)))))        
           (n/cpu {:ip "10.10.10.5" :neighbors 2 :requires [] :provides [:control-data]}))))