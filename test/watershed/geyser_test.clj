(ns watershed.geyser-test
  (:require [lamina.core :as lamina]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [watershed.utils :as u]
            [manifold.stream :as s])
  (:use [clojure.string :only (join split)]
        [watershed.geyser]))
  
(def g (geyser 8999 (gloss/string :utf-8)))

(w/flow g)

(def watershed 

  (-> 
  
    (w/watershed)
  
    (w/add-river (w/source :broadcast  
                           
                           (fn [] (s/periodically 1000 (fn [] {:host "10.10.10.255" :port 8999 :message (str (u/cpu-units))})))))
  
    (w/add-river (w/river :geyser [:broadcast] 
                          
                          (fn [x] (s/connect (first x) (:source g)) (s/map (fn [x] {(keyword (:host x)) x}) (:sink g)))))
  
    (w/add-river (w/estuary :result [:geyser] 
                            
                            (fn [x] (s/reduce merge (first x)))))
    
    w/flow)) 

;(def result (w/ebb watershed))


