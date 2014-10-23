(ns watershed.core-dev
  (:require [clojure.pprint :as p]    
            [manifold.deferred :as d]
            [watershed.graph :as g]
            [criterium.core :as c]
            [clojure.core.async :as async]
            [clojure.set :as st]
            [manifold.stream :as s]
            [lamina.core :as l]))

(set! *warn-on-reflection* true)

;What does a channel transducer look like...?

(defn ^:private base-xform 
  "The idea here is to slot the channel creation function into the transducer..."
  [step] 
  (fn    
    ([] (step))  
    ([result] (step result))    
    ([result input]
      (if input
        (step result input)
        (step result)))))

(defn manifold-step 
  ([] (s/stream))
  ([s] (s/close! s))
  ([s input] (s/put! s input)))

(defn manifold-connect 
  [in out] 
  (s/connect in out {:upstream? true}))

(defmulti parse-outline 
  (fn [env outline step con ]
    (:type outline)))

#_{:type :source :stream << ... >>, :title :data}

(defmethod parse-outline :cyclic
  [env {:keys [title sieve]} step _]
  (assoc env title (step)))

(defmethod parse-outline :source
  [env {:keys [title sieve]} step _]
  (assoc env title (step)))

#_{:type :estuary :sieve (fn [& streams]) :title :end-of-shed}

(defmethod parse-outline :estuary
  [env {:keys [title sieve tributaries]} _ _] 
  (assoc env title (apply sieve (map env tributaries))))

#_{:type :river :sieve (fn [& streams]) :gen s/map :title :transform!}

(defmethod parse-outline :river
  [env {:keys [title sieve tributaries]} _ _]  
  (assoc env title (apply sieve (map env tributaries))))

(defmethod parse-outline nil 
  [env _ _ _ ]
  env)

#_{:type :aliased :sieve (fn [& streams])}

;Horrible side effects...would be better to find a solution to this!

(defmethod parse-outline :aliased 
  [env {:keys [title sieve tributaries]} _ con] 
  (con (apply sieve (map env tributaries)) (title env))
  env)

(def test-outline 
  [{:tributaries [:a [:b-group]]
    :title :a
    :sieve (fn 
             ([] [0])
             ([& streams] (println streams) (s/map (fn [[a]] a) (apply s/zip streams))))
    :type :river}   
       
    {:title :d 
     :sieve (fn [a] a)
     :tributaries [:c]
     :type :river}
    
    {:title :f
     :sieve (fn [a] (s/consume #(println "f: " %) (s/map identity a)))
     :tributaries [:e]
     :type :estuary}
        
    {:title :e 
     :sieve (fn [a] a)
     :tributaries [:d]
     :type :river}
   
   {:title :c 
    :sieve (fn [a] a)
    :type :river
    :tributaries [:a]}
   
   {:sieve (fn [] (s/periodically 1000 (fn [] [0])))
    :title :b
    :group :b-group
    :type :source}])

(defn- expand-dependencies
  [groups dependencies]
  (vec (flatten (map (fn [dependency]                
                       (if (vector? dependency)                      
                         (let [[id op & args] dependency] 
                           (case op                            
                             :only (vec (filter (set args) groups))
                             :without (vec (remove (set args) groups))
                             (id groups)))                                                  
                         dependency))               
                     dependencies))))

(defn- dependents
  [outlines t] 
  (reduce      
    (fn [coll {:keys [title tributaries]}]              
      (if (some #{t} tributaries) (conj coll title) coll)) #{} outlines))
    
(defn assemble 
  [step con & outlines] 
  
  ;COMPILE ORDER: rivers/estuaries/dams -> sources
  
  (let [        
        
        ;TODO. type inference!!!! 
        
        ;#### Expand dependencies... #### 
        
        with-deps (let [groups (reduce (fn [m {:keys [title group]}] 
                                         (if group 
                                           (update-in m [group] (fn [x] (conj x title))) 
                                           m)) 
                                       {} outlines)]
                        
                        (map (fn [o] (assoc o :tributaries (expand-dependencies groups (:tributaries o)))) outlines))
        
        compiler (fn [env o] (parse-outline env o step con))                 
                   
        ;#### Resolve cycles...I only need to worry about rivers here! ####
        
        ;TODO: do better picking of sccs!
        
        sccs (let [graph (reduce (fn [m {:keys [title]}]                      
                                          (assoc m title {:edges (dependents with-deps title)}))                     
                                        {} (filter #(= (:type %) :river) with-deps))]
               (->>
                                           
                   ;#### Gather strongly-connected components and remove singularities! ####
      
                   (g/strongly-connected-components graph (g/transpose graph))
      
                   (remove        
                     (fn [vals]
                       (if (= (count vals) 1)       
                         (let [val (vals 0)]                              
                           (not (val (:edges (val graph))))))))))
        
        ;#### Tag components in cycles... ####
        
        with-deps (let [pred (apply (comp set concat) sccs)]                     
                      (map (fn [o]                            
                             (if ((:title o) pred)
                               (assoc o :type :cyclic)
                               o))                                        
                           with-deps))      
             
        ;#### Get the sources and cycles for future reference! ####
        
        sources (filter #(= (:type %) :source) with-deps)
        
        cycles (filter #(= (:type %) :cyclic) with-deps)
        
        ;#### First compiler pass... ####
        
        env (let [graph (-> 
                           
                          ;#### Create graph... ####
                           
                          (reduce (fn [m {:keys [title]}]                      
                                               (assoc m title {:edges (dependents with-deps title)}))                     
                                             {} with-deps)
                           
                          ;#### Create starting point.  Essentially, where all the sources in the graph point ####
                           
                          (assoc ::init {:edges (reduce (fn [coll o] (if (= (:type o) :source) (conj coll (:title o)) coll)) #{} with-deps)}))
                   
                  helper (fn this [env current]                 
                           (let [o (some (fn [x] (if (= (:title x) current) x)) with-deps)]                         
                             (if (current env)
                               env
                               (if (or (= (:type o) :source) (= (:type o) :cyclic))
                                 (reduce this (compiler env o) (:edges (current graph)))
                                 (if (every? (set (keys env)) (:tributaries o))
                                   (reduce this (compiler env o) (:edges (current graph)))
                                   env)))))]
                             
              ;#### Second, and final, compilation pass!
              
              (reduce compiler (helper {} ::init) (map (fn [o] (assoc o :type :aliased)) (concat sources cycles))))] 
          
    ;#### Next, I need to start all of the cycles.  Ooo, side effects! ####
      
    (doseq [o (mapcat              
            (fn [scc-group]         
              (let [max-deps (reduce (fn [max o]                   
                               (if (> (count (:tributaries o)) (count (:tributaries max)))
                                 o
                                 max))                            
                             (filter (comp (set scc-group) :title) cycles))]        
                (filter (comp (set (:tributaries max-deps)) :title) cycles)))          
            sccs)]    
        (step ((:title o) env) ((:sieve o))))
    
    ;#### Associate streams back into the outlines! ####
      
    (map (fn [o] (assoc o :stream ((:title o) env))) outlines)))     
  
  
  
  
  
  
  
  
  
  
  
  
  
        
         
         
           
           
           
           
           
           
                 