(ns watershed.core
  (:require [manifold.deferred :as d]
            [manifold.stream :as s]
            [lamina.core :as l]))

(defprotocol ITide
  (flow [_])
  (ebb [_]))

(defn dependents
  [system title]

  (reduce-kv (fn [coll k v] 
               
               (if (some #{title} (:tributaries v)) (conj coll k) coll)) '() system))

(defn- contains-many?
  [coll query-coll]

  (every? (fn [x] (some (fn [y] (= y x)) coll)) query-coll))

(defn start-order
  [state] 
  
  (letfn [(helper 
            [state current-order]
            (if (empty? state)
              current-order
              (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? current-order z)) (conj x y) x)) [] (zipmap (keys state) (map :tributaries (vals state))))]
                (recur (reduce dissoc state possible)
                       (reduce conj current-order possible)))))]
    
    (reverse (helper state nil))))

(defrecord River [title tributaries output sieve on-ebbed]

  ITide

  (flow
   [_])

  (ebb
   [_]

   (if (not= output :not-started)
     (s/close! output))

   (if on-ebbed
      (on-ebbed))))

(defrecord Source [title output sieve on-ebbed]
  
  ITide 
  
  (flow 
    [_])
  
  (ebb 
    [_]
    
    (if (not= output :not-started)
      (s/close! output))
    
    (if on-ebbed
      (on-ebbed))))

(defrecord Estuary [title tributaries output sieve on-ebbed]
  
  ITide 
  
  (flow 
    [_])
  
  (ebb 
    [_]
    
    (if on-ebbed
      (on-ebbed))))

(defn start-in-order
  
  [system]

  (let [order (start-order system)
        
        tributaries (mapv (comp :tributaries system) order)]

    (reduce-kv (fn [started cardinal cur]               
               
                 (let [r (cur system)]  
                   
                   (assoc started cur (if (= (type r) watershed.core.Source)
                     
                                       ((:sieve r))
                     
                                       ((:sieve r) (map started (tributaries cardinal)))))))
             
               {}
                           
               (vec order))))

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
       
       (remove nil? (map (fn [y] (ebb (y system)) (let [riv (y system)] (if (= (type riv) watershed.core.Estuary) {y (:output riv)}))) x)))))

  ITide

  (flow
    [_]
    
    @(l/run-pipeline
       
    system
    
    (fn [system]
      
      (start-in-order system))
    
    (fn [started-system]     
      
        (reduce           
          
          (fn [x [k v]] (assoc-in x [:system k :output] v)) 
           
          _ started-system))))

  (ebb [_]
                 
    (apply merge (mapcat (fn [x] (ebb-river _ x)) (reduce-kv (fn [x y z] (if (empty? z) (conj x y) x)) [] (zipmap (keys system) (map :tributaries (vals system))))))))

(defn watershed []
  (->Watershed {}))

(defn river 
  
  [title tributaries sieve & {:keys [on-ebbed] :or {on-ebbed nil}}]    
    
  (->River title tributaries :not-started sieve on-ebbed))

(defn source 

  [title sieve & {:keys [on-ebbed] :or {on-ebbed nil}}]
    
  (->Source title ::not-started sieve on-ebbed))

(defn estuary 

  [title tributaries sieve & {:keys [on-ebbed] :or {on-ebbed nil}}]
  
    (->Estuary title tributaries ::not-started sieve on-ebbed ))

















