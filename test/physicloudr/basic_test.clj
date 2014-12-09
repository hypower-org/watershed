(ns physicloudr.basic-test
  (:require [watershed.core :as w]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [physicloudr.physi-server :as phy]))

(defn -main
  [ip neighbors]
  (phy/physicloud-instance 
  
    {:ip ip
     :neighbors neighbors
     :requires [] 
     :provides []}))

