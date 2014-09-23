(ns watershed.watershed-test 
  (:use [watershed.core]
        [watershed.graph]
        [clojure.set])
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.zip :as z]))

(defn periodical
  [streams period fnc]
  
  (let [val (atom (vec (map (fn [x] nil) streams)))]

    (if (empty? @val)

      (s/periodically period fnc)

      (do
        
        (reduce (fn [cnt stream] (s/consume (fn [x] (swap! val assoc cnt x)) stream) (inc cnt)) 0 streams)

        (s/map (fn [x] (if-not (some nil? x) (fnc x))) (s/periodically period (fn [] @val)))))))

(defn cyclic-fn 
  [[[a b] c]] 
  [a c])
 
(defn dam-fn 
  [w [a b]]
  (println w)
  (println "dam: " a b)
  (when (= b 1)
    (println "DONE!")
    (ebb w)))

(def outline 
  
  {:a {:tributaries [] :sieve (fn [] (s/periodically 1000 (fn [] 1))) :type :source} 
   
   :b {:tributaries [:b :a] :sieve (fn [& streams] (s/map cyclic-fn (apply s/zip streams))) :type :river
       
       :initial [1 2]} 
   
   :c {:tributaries [:b] :sieve (fn [x] (s/consume println x)) :type :estuary}
   
   :watch {:tributaries [:b] :sieve (fn [w b] (s/consume #(dam-fn w %) b)) :type :dam}
  
   :d {:tributaries [:b] :sieve (fn [x] (s/reduce concat (s/map identity x))) :type :estuary}
   
   })

(def test-system (compile* outline))

test-system
  
(ebb test-system)










