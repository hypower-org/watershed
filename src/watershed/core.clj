(ns watershed.core
  (:use [clojure.walk])
  (:require [manifold.deferred :as d]
            [manifold.stream :as s]
            [clojure.pprint :as p]
            [clojure.zip :as z]
            [lamina.core :as l]))

(defprotocol ITide
  (flow [_])
  (ebb [_]))

(defn- dependents
  [system title]

  (reduce-kv

   (fn [x y z]

     (let [tributaries (keys (:tributaries z))]

       (if (some (fn [w] (= title w)) tributaries)

         (conj x y)
         x)))

   #{} system))

(defn- contains-many?
  [coll query-coll]

  (every? (fn [x] (some (fn [y] (= y x)) coll)) query-coll))

(defrecord River [title tributaries stream sieve on-ebbed]

  ITide

  (flow
   [_]

   (s/connect (sieve (vals tributaries)) stream))

  (ebb
   [_]

   (doseq [s (vals tributaries)]
     (s/close! s))

   (s/close! stream)

   (on-ebbed)))

(defrecord Source [title stream sieve on-ebbed]
  
  ITide 
  
  (flow 
    [_]
    (s/connect (sieve) stream))
  
  (ebb 
    [_]
    
    (s/close! stream)
    
    (on-ebbed)))

(defrecord Estuary [title tributaries sieve on-ebbed]
  
  ITide 
  
  (flow 
    [_]
    
    (sieve (vals tributaries)))
  
  (ebb 
    [_]
    
    (doseq [s (vals tributaries)]
      (s/close! s))
    
    (on-ebbed)))

(defprotocol IWatershed
  (add-river [_ river])
  (ebb-river [_ title]))

;Title is the name of the rivers
;Tributaries: The dependencies of a river (i.e., the things that compose it)
;Stream: the output of the river.
;Flow: How the tributaries flow into the river
;Ebb: what happens when the river closes

(defn- tree 
  [system state start-order]
  
  (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? (flatten start-order) z)) (conj x y) x)) [] (zipmap (keys state) (map keys (map :tributaries (vals system)))))

        state (reduce dissoc state possible)]   

    (if (empty? state)

      (seq possible)  
      
      (seq (conj possible (tree system state (conj start-order possible)))))))
  

(defrecord Watershed [system]

  IWatershed

  (add-river

   [_ river]

   (assoc-in _ [:system (:title river)] river))

  (ebb-river
   [_ title]

   @(l/run-pipeline

     title

     (fn
       [x]

       (loop [ebbed []

              to-ebb (dependents system x)]

         (if (empty? to-ebb)

           (distinct (conj (mapcat identity ebbed) title))

           (recur (conj ebbed to-ebb) (mapcat (fn [x] (dependents system x)) to-ebb)))))

     (fn [x]

       (reduce (fn [y z] (ebb (z system)) (dissoc y z)) system x))))

  ITide

  (flow

   [_]

   @(l/run-pipeline

      ;In the future, parallelize starting sequence

      nil

      (fn [_]
        
        (tree system system nil))


      (fn [x] (println "Dependency tree: " x) x)
      
      (fn [dependency-tree]
        
        (postwalk (fn [x] (when-let [riv (get system x)] 
                            
                            (mapv s/connect (map :stream (map system (keys (:tributaries riv)))) (vals (:tributaries riv)))
                            
                            (flow riv)))
                  
                  dependency-tree) 
        
        _)))

  (ebb [_]
                 
    (reduce ebb-river _ (reduce-kv (fn [x y z] (if (empty? z) (conj x y) x)) [] (zipmap (keys system) (map keys (map :tributaries (vals system))))))))

(defn watershed []
  (->Watershed {}))

(defn river [title tributaries sieve on-ebbed]
  (->River title (zipmap tributaries (repeatedly (count tributaries) s/stream)) (s/stream) sieve on-ebbed))

(defn source [title sieve on-ebbed]
  (->Source title (s/stream) sieve on-ebbed))

(defn estuary [title tributaries sieve on-ebbed]
  (->Estuary title (zipmap tributaries (repeatedly (count tributaries) s/stream)) sieve on-ebbed))

(defn periodical
  [streams period fnc]

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











