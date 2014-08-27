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

  (reduce-kv (fn [coll k v] (if (some #{title} (keys (:tributaries v))) (conj coll k) coll)) '() system))

(defn- contains-many?
  [coll query-coll]

  (every? (fn [x] (some (fn [y] (= y x)) coll)) query-coll))

(defn start-order 
  
  [state current-order]
  
  (loop [possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? current-order z)) (conj x y) x)) [] (zipmap (keys state) (map (comp keys :tributaries) (vals state))))        
           
         state (reduce dissoc state possible)] 
    
    (if (empty? state)
      
      possible
      
      (reduce conj possible (start-order state possible)))))

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

(defrecord Estuary [title tributaries sieve on-ebbed result]
  
  ITide 
  
  (flow 
    [_]
    
    (d/connect (sieve (vals tributaries)) result))
  
  (ebb 
    [_]
    
    (doseq [s (vals tributaries)]
      (s/close! s))
    
    (on-ebbed)))

(defprotocol IWatershed
  (add-river [_ river])
  (ebb-river [_ title]))

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

       (reduce (fn [y z] (ebb (z system)) (dissoc y z)) system x)
       
       (map (fn [y] (if (= (type (y system)) watershed.core.Estuary) {y (:result (y system))})) x)
       
       )))

  ITide

  (flow
    [_]
    
    @(l/run-pipeline
       
    system
    
    (fn [system]
      
      (start-order system nil))
    
    (fn [x] (println "Starting order: " x) x)
    
    (fn [start-order]     
      
      (doseq [riv start-order]
        
        (let [riv-value (riv system)
              
              tributaries (:tributaries riv-value)]         
        
          (mapv s/connect (map (comp :stream system) (keys tributaries)) (vals tributaries))
        
          (flow riv-value)))))
    _)

  (ebb [_]
                 
    (apply merge (mapcat (fn [x] (ebb-river _ x)) (reduce-kv (fn [x y z] (if (empty? z) (conj x y) x)) [] (zipmap (keys system) (map (comp keys :tributaries) (vals system))))))))

(defn watershed []
  (->Watershed {}))

(defn river [title tributaries sieve on-ebbed]
  (->River title (zipmap tributaries (repeatedly (count tributaries) s/stream)) (s/stream) sieve on-ebbed))

(defn source [title sieve on-ebbed]
  (->Source title (s/stream) sieve on-ebbed))

(defn estuary [title tributaries sieve on-ebbed]
  (->Estuary title (zipmap tributaries (repeatedly (count tributaries) s/stream)) sieve on-ebbed (d/deferred)))





