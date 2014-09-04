(ns watershed.watershed-test 
  (:use [watershed.core]
        [watershed.graph]
        [clojure.set])
  (:require [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.zip :as z]))

(defn periodical
  [streams period fnc]
  
  (println streams)

  (let [val (atom (vec (map (fn [x] nil) streams)))]

    (if (empty? @val)

      (s/periodically period fnc)

      (do
        (loop [s streams]

          (let [index (dec (count s))]

            (s/consume (fn [x] (swap! val assoc index x)) (last s))

            (if (> index 0)

              (recur (butlast s)))))

        (s/map (fn [x] (if-not (empty? x) (fnc x))) (s/periodically period (fn [] (mapcat identity @val))))))))

(def test-fn (fn [& x] (periodical x 1000 identity)))

(def test-system (->

                 (watershed)

                 (add-river (source :reef (fn [] (periodical [] 1000 (fn [] [1]))) :on-ebbed (fn [] (println "reef removed :("))))
                 
                 (add-river (eddy :coral [:reef :stream]
                                  
                                  (fn [reef stream]                                    
                                    
                                    (s/map (fn [x] [(inc (first x))]) stream))                                              
                                  
                                  [2] :on-ebbed (fn [] (println "coral removed :("))))
                 
                 ;(add-river (river :pond [:coral] test-fn :on-ebbed (fn [] (println "pond removed :("))))
                 
                 (add-river (eddy :stream [:coral] test-fn [3] :on-ebbed (fn [] (println "stream removed :("))))
                 
                 ;(add-river (river :lake [:stream] test-fn :on-ebbed (fn [] (println "lake removed :("))))
                 
                 (add-river (estuary :creek [:stream] (fn [x] 
                                                                                                               
                                                        (s/consume println x) 
                                                        
                                                        (d/deferred)) :on-ebbed (fn [] (println "creek removed :("))))
                 
                 ))

(def g (reduce merge (map (fn [x] {x {:edges (dependents (:system test-system) x)}}) (keys (:system test-system)))))

(def gt (zipmap (keys (:system test-system)) (map (fn [x] {:edges (:tributaries x)}) (vals (:system test-system)))))

(def result (cycles g gt))

(def s (handle-cycles (:system test-system)))










