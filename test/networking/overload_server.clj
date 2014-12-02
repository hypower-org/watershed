(ns networking.overload-server
  (:require [watershed.core :as w]
            [physicloudr.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d])
  (:gen-class))

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(def iterations (atom 0))

(defn -main 
  [ip neighbors] 
  
  (n/physicloud-instance {:ip ip :neighbors neighbors :requires [:overload] :provides []}
         
         (w/outline :printer [:overload] (fn [stream] (s/consume (fn [x] (swap! iterations inc)) (s/map identity stream))))))