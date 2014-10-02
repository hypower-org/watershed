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
  (isolate [_ reference])
  (dependents [_ title]))

(defrecord Waterway [tide title group tributaries output]
  
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
          (str "|" (:tributaries o) "| ->"))        
        " " (:title o)       
        "<" (:group o) ">"       
        (str " -> " (pr-str (:output o)))))))

;Do some matching to create a very efficient waterway.  This function is probably unnecessary lol

(defn- emit-ebb 
  [output on-ebbed] 
  (m/match 
    [(s/stream? output) (fn? on-ebbed)] 
    [false false] (reify ITide (ebb [_] output))
    [true false] (reify ITide (ebb [_] (s/close! output)))
    [false true] (reify ITide (ebb [_] (on-ebbed) output))
    [true true] (reify ITide (ebb [_] (on-ebbed) (s/close! output)))))

(defn- waterway 
  [{:keys [title group tributaries sieve streams on-ebbed]}] 
  
  (let [output (if (empty? streams) (sieve) (apply sieve streams))
        
        e (emit-ebb output on-ebbed)] 
    
    (->Waterway (reify ITide (flow [_] output) (ebb [_] (ebb ^ITide e))) title group tributaries output)))

(defn- dam 
  "A special case of the waterway."
  [{:keys [title group tributaries sieve streams watershed]}]
  
  (let [output (if (empty? streams) ((sieve watershed)) (apply sieve (cons watershed streams)))]  
    
    (->Waterway (reify ITide (flow [_] output) (ebb [_])) title group tributaries output)))
            
;BEING DEVELOPED!

(defn expand-dependencies
  [groups dependencies]
  (vec (flatten (map (fn [dependency]                                     
                       (if (vector? dependency)                      
                         (let [[id op & args] dependency] 
                           (case op 
                             :only (vec (filter (set args) (id groups)))
                             :without (vec (remove (set args) (id groups)))
                             (id groups)))                                                  
                         dependency))               
                     dependencies))))

(defn- ebb-helper 
  [watershed] 
  (let [ks (vec (keys watershed))]        
      (reduce-kv         
        (fn [m i v]          
          (if (record? v)                
            (let [ret (ebb v)] 
              (if ret 
                (assoc m (ks i) ret)
                m))
            (ebb-helper v)))  
        {}        
        (vec (vals watershed)))))  

(defrecord Watershed [watershed] 
  
  ITide
  
  (ebb [_]              
    (ebb-helper watershed))
  
  
  IWatershed
  
  (isolate 
    [_ reference] 
    
    (if (vector? reference) 
      (let [[group op & args] reference]       
        (case op 
          :without (vec (remove (set args) (group watershed)))
          :only (vec (filter (set args) (group watershed)))
          (group watershed))
      (reference watershed))))
  
  
  
  )
  
(defn watershed 
  [system] 
  (->Watershed system))
 
;(defn- start-order
; [state active]  
; (letfn [(helper 
;           [state current-order]
;           (if (empty? state)
;             current-order
;             (let [possible (reduce-kv (fn [x y z] (if (or (empty? z) (every? (set (concat current-order active)) z)) (conj x y) x)) [] state)]
;               (recur (reduce dissoc state possible)
;                      (reduce conj current-order possible)))))]    
;   (reverse (helper state nil))))
 
(defn- dependents* 
  [groups system title] 
  (reduce-kv      
    (fn [coll k v]              
      (if (some #{title} (expand-dependencies groups (:tributaries v))) (cons k coll) coll)) '() system))
  
(defn assemble
  
 [outline & {:keys [pre-existing]}] 
 
 ;Handle some basic input errors here.  There could be more in the future...e.g., make sure rivers have tributaries0
 
 (letfn [(handler [x] 
                  (case x 
                    :type "No type supplied"
                    :tributaries "No tributaries supplied"
                    :sieve "No sieve supplied"))]
   
   (doseq [v (vals outline)] 
     (assert (:type v) (handler :type))
     (assert (:tributaries v) (handler :tributaries))
     (assert (:sieve v) (handler :sieve))))
    
   (let [groups (reduce-kv 
                  (fn [m k v]       
                    (let [group (:group v)]                        
                      (if group               
                        (update-in m [group] (fn [x] (conj x k)))
                        m)))     
                  {}       
                  outline)         
         
         all-deps (reduce (fn [m k] (update-in m [k] #(expand-dependencies groups (:tributaries %)))) outline (keys outline))
         
         dams (reduce-kv (fn [coll k v]                   
                           (if (= (:type v) :dam)                   
                             (conj coll k)                   
                             coll))               
                         [] outline)          
        
         cycles-handled (->>                      
                           
                          (let [graph (apply merge (map (fn [x] {x {:edges (dependents* groups outline x)}})                                                       
                                                        (reduce (fn [coll [k v]]                                      
                                                                  (let [t (:type v)]
                                                                    (if (not (or (= t :estuary) (= t :source)))                                  
                                                                      (conj coll k)        
                                                                      coll)))                                                      
                                                                [] 
                                                                (reduce dissoc outline dams))))]

                            (g/strongly-connected-components graph (g/transpose graph)))      
                                                
                          ;Remove singularities with no self-cyclic dependency
                            
                          (remove                                  
                            (fn [x]       
                              (if (= (count x) 1)       
                                (let [val (first x)]                              
                                  (not (val (set (val all-deps))))))))
                          
                          ;Flatten all the trees.  They're uncessary for what we want to do...maybe...
      
                          flatten
                         
                          ;pre-allocate streams for nodes with cyclic dependencies
                        
                          (map 
      
                            (fn [cyclic] 
                                   
                              (let [val (cyclic outline)
                                    
                                    ;Alias input streams
                                  
                                    streams (repeatedly (count (cyclic all-deps)) s/stream)
                                    
                                    ;I need to create an output so I can initialize it...unfortunate
                                   
                                    output (s/stream)
                                   
                                    eddy (waterway (merge val {:streams streams :title cyclic}))]    
                                
                                ;Causes side effects :'(
                               
                                (s/connect (:output eddy) output)
        
                                {cyclic {:streams streams :eddy (assoc eddy :output output)}})))                        
      
                          (apply merge))
         
         cycles-handled-ks (keys cycles-handled)
        
         system-connected (let [active (concat cycles-handled-ks dams (keys pre-existing))
                                
                                all-deps (reduce dissoc all-deps active)
                                
                                outline (reduce dissoc outline active)
                                
                                started (merge (reduce (fn [m k] (update-in m [k] :eddy)) cycles-handled cycles-handled-ks) pre-existing)
                                
                                start-order (fn [state current-order]
                                                (if (empty? state)
                                                  current-order
                                                  (let [possible (reduce-kv 
                                                                   (fn [x y deps]                                               
                                                                     (if (or (empty? deps) (every? (set (concat current-order active)) deps)) (conj x y) x)) [] state)]
                                                    (recur (reduce dissoc state possible)
                                                           (reduce conj current-order possible)))))]
   
                            (reduce (fn [started cur]    
                                      (let [r (cur outline)]       
                                        (assoc started cur (waterway (merge r {:title cur :streams (map (comp :output started) (cur all-deps))})))))            
                                    started                          
                                    (vec (reverse (start-order all-deps nil)))))]
    
     ;connect cyclic elements to non-cyclic elements.  Side effecty. Ewww
    
     (doall (map (fn [cyclic]           
                   (doall (map (fn [x y] (s/connect x y)) 
                               (map (comp :output system-connected)                                                               
                                    (cyclic all-deps)) (:streams (cyclic cycles-handled)))))            
                 cycles-handled-ks))
     
     ;There's a potential problem here...there may some messages passed through the system before the dam gets access to them...should I fix this?
    
     (let [watershed (reduce-kv           
                       ;watershed cardinal dam 
                       (fn [w i d]   
                         (let [dam-key (dams i)]
                           (assoc-in
                             w [:watershed dam-key]                      
                             (dam (merge d {:title dam-key :streams (map (comp :output system-connected) (dam-key all-deps)) :watershed w})))))         
                       (watershed              
                         (reduce-kv 
                           (fn [m k v]   
                             (let [group (:group v)]
                               (if group
                                 (assoc-in m [group k] v)
                                 (assoc m k v))))                
                           {}
                           system-connected))                          
                       (mapv outline dams))]
      
       (doseq [c cycles-handled-ks] 
      
         ;Get the system rolling...could probably do this more effectively.  For right now, just put initial values into cycles
		      
         (if-let [initial-value (:initial (c outline))]
      
           (s/put! (:output (c system-connected)) initial-value)))
     
       watershed)))
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
        
         
         
           
           
           
           
           
           
                 