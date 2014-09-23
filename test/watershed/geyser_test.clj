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

(def outline 
  
  {:broadcast {:tributaries [] :sieve (fn [] (s/periodically 1000 (fn [] {:host "10.10.10.255" :port 8999 :message (str (u/cpu-units))})))               
            :type :source}

   :geyser {:tributaries [:broadcast] :sieve (fn [x] (s/connect (first x) (:source g)) (s/map (fn [x] {(keyword (:host x)) x}) (:sink g)))
            :type :river}   
   
   :result {:tributaries [:geyser] :sieve (fn [x] (s/reduce merge (s/map identity (first x))))}})

(def test-system (w/compile* outline))

;(def result (w/ebb watershed))


