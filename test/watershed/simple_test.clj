(ns watershed.simple-test
  (:require [watershed.core :as w]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [physicloud.physi-server :as phy]))

(phy/assemble-phy 
  
  (w/outline :a [:c] 
             
             (fn 
               ([] [1])
               ([stream] (s/map #(do (println %) (identity %)) stream))))
  
    (w/outline :b [:a] 
             
             (fn 
               ([] [1])
               ([stream] (s/map identity stream))))
    
    (w/outline :c [:b] 
             
             (fn 
               ([] [1])
               ([stream] (s/map identity stream)))))