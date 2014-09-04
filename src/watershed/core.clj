(ns watershed.core
  (:use [clojure.pprint])
  (:require [manifold.deferred :as d]
            [watershed.graph :as g]
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
  [state & {:keys [active]}] 
  
  (letfn [(helper 
            [state current-order]
            (if (empty? state)
              current-order
              (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? current-order z) (contains-many? active z)) (conj x y) x)) [] (zipmap (keys state) (map :tributaries (vals state))))]
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

(defrecord Eddy [title tributaries output sieve on-ebbed initial]
  
  ITide 
  
  (flow 
    [_])
  
  (ebb 
    [_]
    
    (if on-ebbed
      (on-ebbed))))

;Clean up start-in-order and handle-cycles 

(defn start-in-order
  
  [system started]

  (let [order (start-order system :active (keys started))
        
        tributaries (mapv (comp :tributaries system) order)]

    (reduce-kv (fn [started cardinal cur]                     
               
                 (let [r (cur system)]  
                   
                   (assoc started cur (if (= (type r) watershed.core.Source)
                     
                                       ((:sieve r))
                     
                                       (apply (:sieve r) (map started (tributaries cardinal)))))))
             
               started
                           
               (vec order))))


(defn handle-cycles 
  
  [system] 
    
  (let [pre-allocated (some->> 
    
                        (g/cycles (reduce merge (map (fn [x] {x {:edges (dependents system x)}}) (keys system)))

                                  (zipmap (keys system) (map (fn [x] {:edges (:tributaries x)}) (vals system))))
      
                        flatten
    
                        (map 
      
                          (fn [cyclic] 
                                   
                            (let [val (cyclic system)
                                  
                                  input (repeatedly (count (:tributaries val)) s/stream)
                                  
                                  output (s/stream)]
                              
                              (s/connect (apply (:sieve val) input) output)         
        
                              {cyclic {:input input :output output}})))
      
                        (apply merge))
        
        all-allocated (start-in-order (reduce dissoc system (keys pre-allocated))
    
                                      (zipmap (keys pre-allocated) (map :output (vals pre-allocated))))]
    
    (doall (map (fn [cyclic] 
           
                  (doall (map (fn [x y] (s/connect x y)) (map all-allocated (:tributaries (cyclic system))) (:input (cyclic pre-allocated)))))
             
                (keys pre-allocated)))
    
    (doseq [c (keys pre-allocated)] 
      
      (s/put! (:output (c pre-allocated)) (:initial (c system))))
     
    all-allocated))

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
  
  [title tributaries sieve & {:keys [on-ebbed initial]}]    
    
  (->River title tributaries ::not-started sieve on-ebbed))

(defn source 

  [title sieve & {:keys [on-ebbed]}]
    
  (->Source title ::not-started sieve on-ebbed))

(defn estuary 

  [title tributaries sieve & {:keys [on-ebbed]}]
  
    (->Estuary title tributaries ::not-started sieve on-ebbed))

(defn eddy 

  [title tributaries sieve initial & {:keys [on-ebbed]}]
  
    (->Eddy title tributaries ::not-started sieve on-ebbed initial))

















