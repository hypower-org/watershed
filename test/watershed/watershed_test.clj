(ns watershed.watershed-test 
  (:use [watershed.core]
        [watershed.graph]
        [clojure.set])
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.pprint :as p]
            [clojure.data.generators :as rand]
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

(def ^:private river-sieves 
  [(fn [& streams] (s/map identity (apply s/zip streams)))])

(def ^:private estuary-sieves 
  [(fn [& streams] (s/reduce concat (apply s/zip streams)))])

(def ^:private source-sieves 
  [(fn [] (s/periodically 1000 (fn [] (rand-int 30))))])

(defn gen-titles 
  [n size] 
  (set (mapv (comp keyword str) (range n))))
  
(defn gen-type 
  [] 
  ([:river :estuary :source] (rand-int 3)))

(defn gen-river 
  [type title titles] 
  
  (case type 
    
    :river 
    
    {title {:tributaries (vec (distinct (repeatedly (+ (rand-int (count titles)) 1) (fn [] (rand-nth (seq (disj titles title)))))))
            :sieve (rand-nth river-sieves)
            :initial (rand-int 30)
            :type :river}}
    
    :estuary      
      
    {title {:tributaries (vec (distinct (repeatedly (+ (rand-int (count titles)) 1) (fn [] (rand-nth (seq (disj titles title)))))))
             :sieve (rand-nth estuary-sieves)
             :type :estuary}}
      
    :source
      
    {title {:tributaries []
             :sieve (rand-nth source-sieves)
             :type :source}}))

(defn gen-outline 
  [n] 
  
  (let [titles (gen-titles n n)
        
        types-assigned (zipmap titles (repeatedly gen-type))
        
        possible-sources (apply disj titles (filter (fn [x] (= (x types-assigned) :estuary)) titles))]        
    
    (apply merge (map (fn [[k v]] (gen-river v k possible-sources)) types-assigned))))

;Hand made outline

(def easy-outline 
  
  {:a {:tributaries [] :sieve (fn [] (s/periodically 1000 (fn [] 1))) :type :source} 
   
   :b {:tributaries [:b :a] :sieve (fn [& streams] (s/map cyclic-fn (apply s/zip streams))) :type :river
       
       :initial [1 2]} 
   
   :c {:tributaries [:b] :sieve (fn [x] (s/consume println x)) :type :estuary}
   
   :watch {:tributaries [:b] :sieve (fn [w b] (s/consume #(dam-fn w %) b)) :type :dam}
  
   :d {:tributaries [:b] :sieve (fn [x] (s/reduce concat (s/map identity x))) :type :estuary}
   
   })

;Randomly generated!

(def outline 
  (gen-outline 10))

(def test-system 
  
  (-> 
    
    outline
    
    compile*))
  
;(ebb test-system)










