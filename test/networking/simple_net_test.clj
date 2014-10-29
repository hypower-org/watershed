(ns networking.simple-net-test
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

(defn start 
  [arg] 
  (if arg 
    (do
      (def c (n/physi-client {:host "10.10.10.5"}))
      (def s @(n/physi-server {} "10.10.10.5" "10.10.10.3"))
      (def c @c))
    (def c @(n/physi-client {:host "10.10.10.5"}))))
    

