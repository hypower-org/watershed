(ns watershed.core
  (:require [clojure.pprint :as p]    
            [clojure.core.match :as m]
            [manifold.deferred :as d]
            [watershed.graph :as g]
            [manifold.stream :as s]
            [lamina.core :as l]))

(set! *warn-on-reflection* true)

(defprotocol ITide
  (flow [_])
  (ebb [_]))

;TODO Implement dynamic add to watershed?  Not the best solution, but it may be required...

(defprotocol IWatershed
  (supply-graph [_])
  (dependents [_ title]))

(defrecord Waterway [tide title tributaries output]
  
  ITide
  
  (flow [_]    
    (flow ^ITide tide)) 
  
  (ebb [_]    
    (ebb ^ITide tide)))

(defmethod print-method Waterway [^Waterway o ^java.io.Writer w]
      
  (.write w
      
    (let [tributaries (:tributaries o)]
      
      (str 
        (if (empty? tributaries)
          ""
          (str "|" (.tributaries o) "| ->"))
        
        " " (.title o) 
        
        (str " -> " (pr-str (.output o)))))))

;Do some matching to create a very efficient waterway

(defn- emit-ebb 
  [output on-ebbed] 
  (m/match 
    [(s/stream? output) (fn? on-ebbed)] 
    [false false] (reify ITide (ebb [_] output))
    [true false] (reify ITide (ebb [_] (s/close! output)))
    [false true] (reify ITide (ebb [_] (on-ebbed) output))
    [true true] (reify ITide (ebb [_] (on-ebbed) (s/close! output)))))

(defn- waterway 
  [{:keys [title tributaries sieve streams on-ebbed]}] 
  
  (let [output (if (empty? streams) (sieve) (apply sieve streams))
        
        e (emit-ebb output on-ebbed)] 
    
    (->Waterway (reify ITide (flow [_] output) (ebb [_] (ebb ^ITide e))) title tributaries output)))

(defn- dam 
  [{:keys [title tributaries sieve streams watershed]}]
  
  (let [output (if (empty? streams) ((sieve watershed)) (apply sieve (cons watershed streams)))]  
    
    (->Waterway (reify ITide (flow [_] output) (ebb [_])) title tributaries output)))
            
;BEING DEVELOPED!

(defrecord Watershed [watershed] 
  
  ITide
  
  (flow [_])
  
  (ebb [_]              
    (let [ks (vec (keys watershed))]        
      (reduce-kv         
        (fn [m i v]          
          (let [ret (ebb v)] 
            (if ret 
              (assoc m (ks i) ret)
              m)))     
        {}        
        (vec (vals watershed)))))
  
  IWatershed
    
  (supply-graph 
    [_] 
    
    (apply merge (map (fn [x] {x {:edges (dependents _ x)}}) (keys watershed))))
  
  (dependents 
    [_ title] 
    
    (reduce-kv (fn [coll k v] 
               
                 (if (some #{title} (:tributaries v)) (cons coll k) coll)) '() watershed)))
  
(defn watershed 
  [system] 
  (->Watershed system))
 
(defn- start-order
 [state active]  
 (letfn [(helper 
           [state current-order]
           (if (empty? state)
             current-order
             (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (every? (set (concat current-order active)) z)) (conj x y) x)) [] state)]
               (recur (reduce dissoc state possible)
                      (reduce conj current-order possible)))))]    
   (reverse (helper state nil))))
 
(defn- start-in-order
  
 [system started] 

 (let [order (start-order (zipmap (keys system) (map :tributaries (vals system))) (keys started))
        
       tributaries (mapv (comp :tributaries system) order)] 

   (reduce-kv (fn [started cardinal cur]                        
               
                (let [r (cur system)]                                     
                   
                  (assoc started cur (waterway (merge r {:streams (map (comp :output started) (tributaries cardinal))})))))
             
              started
                           
              (vec order))))
 
(defn- dependents* 
  [system title] 
  (reduce-kv      
    (fn [coll k v]              
      (if (some #{title} (:tributaries v)) (cons k coll) coll)) '() system))
 
(defn assemble
  
 [system] 
 
 ;Handle some basic input errors here.  There could be more in the future...e.g., make sure rivers have tributaries
 
 (letfn [(handler [x] 
                  (case x 
                    :type "No type supplied"
                    :tributaries "No tributaries supplied"
                    :sieve "No sieve supplied"))]
   
   (doseq [v (vals system)] 
     (assert (:type v) (handler :type))
     (assert (:tributaries v) (handler :tributaries))
     (assert (:sieve v) (handler :sieve))))
    
   (let [dams (reduce-kv (fn [coll k v] 
                   
                           (if (= (:type v) :dam)
                     
                             (conj coll k)
                     
                             coll))
                 
                         [] system)
        
         possibly-cyclic (reduce-kv (fn [sys k v]  
                                     
                                      (let [t (:type v)]
                                     
                                        (if (not (or (= t :estuary) (= t :source)))                                  
                                       
                                          (assoc sys k v) 
                                         
                                          sys)))                                 
                                   
                                    {} (reduce dissoc system dams))
        
         cycles-handled (->>                      
                           
                          (let [graph (apply merge (map (fn [x] {x {:edges (dependents* system x)}}) (keys possibly-cyclic)))]

                            (g/strongly-connected-components graph

                                                             (g/transpose graph)))      
                                                
                          ;Remove singularities with no self-cyclic dependency
                            
                          (remove      
                             
                              (fn [x] 
        
                                (if (= (count x) 1)
          
                                  (let [val (first x)] 
                                
                                    (not (val (set (:tributaries (val possibly-cyclic)))))))))
      
                          flatten
                         
                          ;pre-allocate streams for nodes with cyclic dependencies
                        
                          (map 
      
                            (fn [cyclic] 
                                   
                              (let [val (cyclic system)
                                    
                                    ;Alias input streams
                                  
                                    streams (repeatedly (count (:tributaries val)) s/stream)
                                    
                                    ;I need to create an output so I can initialize it...unfortunate
                                   
                                    output (s/stream)
                                   
                                    eddy (waterway (merge val {:streams streams}))]    
                                
                                ;Causes side effects :'(
                               
                                (s/connect (:output eddy) output)
        
                                {cyclic {:streams streams :eddy (assoc eddy :output output)}})))                        
      
                          (apply merge))
        
         system-connected (start-in-order (reduce dissoc system (concat (keys cycles-handled) dams))
    
                                  (zipmap (keys cycles-handled) (map :eddy (vals cycles-handled))))]
    
     ;connect cyclic elements to non-cyclic elements
    
     (doall (map (fn [cyclic] 
           
                   (doall (map (fn [x y] (s/connect x y)) (map (comp :output system-connected) (:tributaries (cyclic system))) (:streams (cyclic cycles-handled)))))
             
                 (keys cycles-handled)))
     
     ;There's a potential problem here...there may some messages passed through the system before the dam gets access to them...should I fix this?
    
     (let [watershed (reduce-kv           
                       ;watershed cardinal dam 
                       (fn [w i d]     
                         (assoc-in
                           w [:watershed (dams i)]                      
                           (dam (merge d {:streams (map (comp :output system-connected) (:tributaries d)) :watershed w}))))         
                       (watershed system-connected)                          
                       (mapv system dams))]
      
       (doseq [c (keys cycles-handled)] 
      
         ;Get the system rolling...could probably do this more effectively.  For right now, just put initial values into cycles
		      
         (if-let [initial-value (:initial (c system))]
      
           (s/put! (:output (c system-connected)) initial-value)))
     
       watershed)))
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
        
         
         
           
           
           
           
           
           
                 