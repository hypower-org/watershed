(ns watershed.core
  (:require [clojure.pprint :as p]    
            [manifold.deferred :as d]
            [watershed.graph :as g]
            [manifold.stream :as s]
            [lamina.core :as l]))

(set! *warn-on-reflection* true)

(defprotocol ITide
  (flow [_])
  (ebb [_]))

(defprotocol IWatershed
  (supply-graph [_])
  (dependents [_ title]))

(defrecord Waterway [tide title tributaries output]
  
  ITide
  
  ;Should probably take this out...
  (flow [_]    
    (flow ^ITide tide)) 
  
  (ebb [_]    
    (ebb ^ITide tide)))

(defmethod print-method Waterway [^Waterway o ^java.io.Writer w]
      
  (.write w
      
    (let [tributaries (:tributaries o)]
      
      ;Make sure this works properly...might have to remove nils in string
      
      (str 
        (if (empty? tributaries)
          ""
          (str "|" (.tributaries o) "| ->"))
        
        " " (.title o) 
        
        (str " -> " (pr-str (.output o)))))))

;(defmacro functionize [macro]
;  `(fn [& args#] (eval (cons '~macro args#))))
;
;(defmacro apply-macro [macro args]
;   `(apply (functionize ~macro) ~args))

;apply-template might be useful!

(defn- emit-flow
  [sieve streams]   
  (reify ITide     
    (flow 
      [_]       
      (apply sieve streams))))

(defn- emit-ebb 
  [output on-ebbed] 
  (reify ITide 
    (ebb 
      [_] 
      (if on-ebbed 
        (on-ebbed))      
      (if (s/stream? output) 
        (s/close! output)
        output))))

(defn waterway 
  [title tributaries sieve streams on-ebbed] 
  
  (let [f (emit-flow sieve streams)
      
        output (flow f)
        
        e (emit-ebb output on-ebbed)]  
    
    (->Waterway (reify ITide (flow [_] (flow ^ITide f)) (ebb [_] (ebb ^ITide e))) title tributaries output)))

(defn dam 
  [watershed title tributaries sieve streams] 
  
  (let [f (emit-flow (fn [& args] (apply sieve (cons watershed args))) streams)
      
        output (flow f)
        
        e (emit-ebb output nil)]  
    
    (->Waterway (reify ITide (flow [_] (flow ^ITide f)) (ebb [_] (ebb ^ITide e))) title tributaries output)))
            
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
 
 ;(println "START ORDER") 
 
 ;(p/pprint state)
 
; (let [graph (zipmap (keys state) (map (fn [x] {:edges x}) (vals state)))] 
;   
;   (println "ACTIVE: " active)
;   (println (g/strongly-connected-components graph (g/transpose graph))))
 
 (letfn [(helper 
           [state current-order]
           ;(println "HELPER: " (keys state) current-order)
           (if (empty? state)
             current-order
             (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (every? (set (concat current-order active)) z)) (conj x y) x)) [] state)]
               (recur (reduce dissoc state possible)
                      (reduce conj current-order possible)))))]    
   (reverse (helper state nil))))
 
(defn start-in-order
  
 [system started] 
 
 ;(p/pprint system) 
 
 ;(p/pprint started)

 (let [order (start-order (zipmap (keys system) (map :tributaries (vals system))) (keys started))
        
       tributaries (mapv (comp :tributaries system) order)] 
   
   ;(p/pprint system) 
   
   ;(println order)
   
   ;(println tributaries)

   (reduce-kv (fn [started cardinal cur]                        
               
                (let [r (cur system)]                                     
                   
                  (assoc started cur (waterway cur (:tributaries r) (:sieve r) (map (comp :output started) (tributaries cardinal)) (:on-ebbed r)))))
             
              started
                           
              (vec order))))
 
(defn- dependents* 
  [system title] 
  (reduce-kv      
    (fn [coll k v]              
      (if (some #{title} (:tributaries v)) (cons k coll) coll)) '() system))
 
(defn compile*
  
 [system] 
    
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
                                  
                                  input (repeatedly (count (:tributaries val)) s/stream)
                                   
                                  output (s/stream)
                                   
                                  eddy (waterway cyclic (:tributaries val) (:sieve val) input (:on-ebbed val))]    
                               
                              (s/connect (:output eddy) output)
        
                              {cyclic {:input input :river (assoc eddy :output output)}})))                        
      
                        (apply merge))
        
       system-connected (start-in-order (reduce dissoc system (concat (keys cycles-handled) dams))
    
                                (zipmap (keys cycles-handled) (map :river (vals cycles-handled))))]
    
   ;connect cyclic elements to non-cyclic elements
    
   (doall (map (fn [cyclic] 
           
                 (doall (map (fn [x y] (s/connect x y)) (map (comp :output system-connected) (:tributaries (cyclic system))) (:input (cyclic cycles-handled)))))
             
               (keys cycles-handled)))
    
   ;associate started rivers into watershed and attach dams!
    
   (let [watershed (reduce-kv           
                     ;watershed cardinal dam 
                     (fn [w i d]     
                       (let [ts (:tributaries d)
                             title (dams i)]
                         (assoc-in
                           w [:watershed title]                      
                           (dam w title ts (:sieve d) (map (comp :output system-connected) ts)))))         
                     (watershed system-connected)                          
                     (mapv system dams))]
      
     (doseq [c (keys cycles-handled)] 
      
       ;Get the system rolling...could probably do this more effectively.  For right now, just put initial values into cycles
		      
       (if-let [initial-value (:initial (c system))]
      
         (s/put! (:output (c system-connected)) initial-value)))
     
     watershed)))
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
        
         
         
           
           
           
           
           
           
               