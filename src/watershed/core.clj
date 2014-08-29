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

(defn- start-order
  [state] 
  
  (letfn [(helper 
            [state current-order]
            (if (empty? state)
              current-order
              (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? current-order z)) (conj x y) x)) [] (zipmap (keys state) (map (comp keys :tributaries) (vals state))))]
                (recur (reduce dissoc state possible)
                       (reduce conj current-order possible)))))]
    
    (helper state nil)))                    

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

   (if on-ebbed
      (on-ebbed))))

(defrecord Source [title stream sieve on-ebbed]
  
  ITide 
  
  (flow 
    [_]
    (s/connect (sieve) stream))
  
  (ebb 
    [_]
    
    (s/close! stream)
    
    (if on-ebbed
      (on-ebbed))))

(defrecord Estuary [title tributaries sieve on-ebbed result]
  
  ITide 
  
  (flow 
    [_]
    
    (d/connect (sieve (vals tributaries)) result))
  
  (ebb 
    [_]
    
    (doseq [s (vals tributaries)]
      (s/close! s))
    
    (if on-ebbed
      (on-ebbed))))

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
       
       (map (fn [y] (let [riv (y system)] (if (= (type riv) watershed.core.Estuary) {y (:result riv)}))) x))))

  ITide

  (flow
    [_]
    
    @(l/run-pipeline
       
    system
    
    (fn [system]
      
      (start-order system))
    
    (fn [start-order]     
      
      (doseq [riv start-order]
        
        (let [riv-value (riv system)
              
              tributaries (:tributaries riv-value)]         
          
          ;Add in assertion to check for proper connects?
        
          (mapv s/connect (map (comp :stream system) (keys tributaries)) (vals tributaries))
        
          (flow riv-value)))))
    _)

  (ebb [_]
                 
    (apply merge (mapcat (fn [x] (ebb-river _ x)) (reduce-kv (fn [x y z] (if (empty? z) (conj x y) x)) [] (zipmap (keys system) (map (comp keys :tributaries) (vals system))))))))

(defn watershed []
  (->Watershed {}))

(defn river 
  
  [title tributaries sieve & {:keys [on-ebbed] :or {on-ebbed nil}}]    
    
  (->River title (zipmap tributaries (repeatedly (count tributaries) s/stream)) (s/stream) sieve on-ebbed))

(defn source 

  [title sieve on-ebbed & {:keys [on-ebbed] :or {on-ebbed nil}}]
    
  (->Source title (s/stream) sieve on-ebbed))

(defn estuary 

  [title tributaries sieve & {:keys [on-ebbed] :or {on-ebbed nil}}]
  
    (->Estuary title (zipmap tributaries (repeatedly (count tributaries) s/stream)) sieve on-ebbed (d/deferred)))

















