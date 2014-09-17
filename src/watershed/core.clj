(ns watershed.core
  (:use [clojure.pprint])
  (:require [manifold.deferred :as d]
            [watershed.graph :as g]
            [manifold.stream :as s]
            [lamina.core :as l]))

(declare compile*)

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
              (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? current-order z) (contains-many? active z)) (conj x y) x)) [] state)]
                (recur (reduce dissoc state possible)
                       (reduce conj current-order possible)))))]
    
    (reverse (helper state nil))))

(defrecord River [title tributaries output sieve on-ebbed]

  ITide

  (flow
   [_])

  (ebb
   [_]

   (if (not= output ::not-started)
     (s/close! output))

   (if on-ebbed
      (on-ebbed))))

(defrecord Source [title output sieve on-ebbed]
  
  ITide 
  
  (flow 
    [_])
  
  (ebb 
    [_]
    
    (if (not= output ::not-started)
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
    
    (if (not= output ::not-started)
      (s/close! output))
    
    (if on-ebbed
      (on-ebbed))))

(defrecord Dam [title tributaries sieve] 
  
  ITide 
  
  (flow [_])
  
  (ebb [_]))

(defn start-in-order
  
  [system started] 
  
  (println "START: " (keys system) (keys started))

  (let [order (start-order (zipmap (keys system) (map :tributaries (vals system))) :active (keys started))
        
        tributaries (mapv (comp :tributaries system) order)] 

    (reduce-kv (fn [started cardinal cur]                        
               
                 (let [r (cur system)]                                     
                   
                   (assoc started cur (cond 
                                                                              
                                        (= (type r) watershed.core.Source)
                     
                                        ((:sieve r))
                                                                              
                                        :else
               
                                        (apply (:sieve r) (map started (tributaries cardinal)))))))
             
               started
                           
               (vec order))))

(defprotocol IWatershed
  (add-river [_ river])
  (ebb-river [_ title])
  (supply-graph [_]))

(defrecord Watershed [system]

  IWatershed
  
  (supply-graph 
    [_] 
    
    (apply merge (map (fn [x] {x {:edges (dependents system x)}}) (keys system))))

  (add-river

   [_ river]

   (assoc-in _ [:system (:title river)] river))

  (ebb-river
   [_ title]

   @(l/run-pipeline

     title

     (fn
       [x]

       (loop [ebbed #{}

              to-ebb (dependents system x)]

         (if (empty? to-ebb)
           
           (conj ebbed title)

           (recur (clojure.set/union ebbed (set to-ebb)) (mapcat (fn [x] (dependents system x)) to-ebb)))))

     (fn [x]
       
       (remove nil? (map (fn [y] (ebb (y system)) (let [riv (y system)] (if (= (type riv) watershed.core.Estuary) {y (:output riv)}))) x)))))

  ITide

  (flow
    [_]
    
    (compile* _))

  (ebb [_]
                 
    (apply merge (remove nil? (map (fn [y] (ebb (y system)) (let [riv (y system)] (if (= (type riv) watershed.core.Estuary) {y (:output riv)}))) (keys system))))))

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

(defn dam 
  [title tributaries sieve] 
  
  (->Dam title tributaries sieve))

(defn compile*
  
  [^Watershed watershed] 
    
  (let [system (:system watershed)
        
        dams (reduce-kv (fn [coll k v] 
                   
                          (if (= (type v) watershed.core.Dam)
                     
                            (conj coll k)
                     
                            coll))
                 
                        [] system)
        
        possibly-cyclic (reduce-kv (fn [sys k v]  
                                     
                                     (let [t (type v)]
                                     
                                       (if (not (or (= t watershed.core.Estuary) (= t watershed.core.Source)))                                  
                                       
                                         (assoc sys k v) 
                                         
                                         sys)))                                 
                                   
                                   {} (reduce dissoc system dams))
        
        cycles-handled (some->>                      
                           
                         (let [graph (apply merge (map (fn [x] {x {:edges (dependents possibly-cyclic x)}}) (keys possibly-cyclic)))]
                           
                           ;(println (keys system))
                           
                           ;(println (keys possibly-cyclic))
                           
                           ;(println graph)
		                           
                           (println "SCC: " (g/strongly-connected-components graph

                                                                 (g/transpose graph)))

                           (g/strongly-connected-components graph

                                                            (g/transpose graph)))      
                                                
                         ;Remove singularities with no self-cyclic dependency
                            
                         (remove      
                             
                             (fn [x] 
        
                               (if (= (count x) 1)
          
                                 (let [val (first x)] 
                                
                                   (not (val (set (:tributaries (val possibly-cyclic)))))))))
      
                         flatten
                         
                         ;pre-allocate streams for nodes with cyclic dependencie(s)
                        
                         (map 
      
                           (fn [cyclic] 
                                   
                             (let [val (cyclic system)
                                  
                                   input (repeatedly (count (:tributaries val)) s/stream)
                                  
                                   output (s/stream)]
                              
                               (s/connect (apply (:sieve val) input) output)         
        
                               {cyclic {:input input :output output}})))                        
      
                         (apply merge))
        
        system-connected (start-in-order (reduce dissoc system (concat (keys cycles-handled) dams))
    
                                 (zipmap (keys cycles-handled) (map :output (vals cycles-handled))))]
    
    ;connect cyclic elements to non-cyclic elements
    
    (doall (map (fn [cyclic] 
           
                  (doall (map (fn [x y] (s/connect x y)) (map system-connected (:tributaries (cyclic system))) (:input (cyclic cycles-handled)))))
             
                (keys cycles-handled)))
    
    ;associate started rivers into watershed and attach dams!
      
    (let [watershed (reduce           
          
                      (fn [x [k v]] (assoc-in x [:system k :output] v)) 
           
                      watershed system-connected)]
      
      (doseq [dam (map system dams)]

        (apply (:sieve dam) (cons watershed (map system-connected (:tributaries dam)))))
        
      (doseq [c (keys cycles-handled)] 
      
        ;Get the system rolling...could probably do this more effectively.  For right now, just put initial values into cycles
		      
        (if-let [initial-value (:initial (c system))]
      
          (s/put! (:output (c cycles-handled)) initial-value)))
     
      watershed)))

















