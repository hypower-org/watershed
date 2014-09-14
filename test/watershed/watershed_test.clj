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

(def test-fn (fn [& x] (periodical x 1000 identity)))

(defn watch 
  [watershed [num]]
  
  (if (= num 3)
    (ebb watershed)))

(def test-system (->

                   (watershed)

                   (add-river (source :reef (fn [] (periodical [] 1000 (fn [] {:hi 1}))) :on-ebbed (fn [] (println "reef removed :("))))
                 
                   (add-river (eddy :coral [:reef :stream]
                                  
                                    (fn [reef stream]                                    
                                    
                                      (s/map (fn [x] (first x)) stream))                                              
                                  
                                    [2] :on-ebbed (fn [] (println "coral removed :("))))
                 
                   (add-river (eddy :stream [:coral] test-fn [3] :on-ebbed (fn [] (println "stream removed :("))))
                 
                   (add-river (estuary :creek [:stream] (fn [x] 
                                                                                                               
                                                          (s/consume println x) 
                                                        
                                                          (d/deferred)) :on-ebbed (fn [] (println "creek removed :("))))
                   
                   (add-river (dam :end [:stream] (fn [watershed & x] (s/map (fn [y] (watch watershed y)) (first x)))))
                 
                   ))

;(def g (reduce merge (map (fn [x] {x {:edges (dependents (:system test-system) x)}}) (keys (:system test-system)))))

;(def gt (zipmap (keys (:system test-system)) (map (fn [x] {:edges (:tributaries x)}) (vals (:system test-system)))))

;(def result (cycles g gt))

(def s (flow test-system))










