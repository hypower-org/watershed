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

(defmethod parse-outline :cyclic
  [env {:keys [title sieve]} step _]
  (assoc env title (step)))

(defmethod parse-outline :source
  [env {:keys [title sieve]} step _]
  (assoc env title (step)))

(defmethod parse-outline :estuary
  [env {:keys [title sieve tributaries]} _ _] 
  (assoc env title (apply sieve (map env tributaries))))

(defmethod parse-outline :river
  [env {:keys [title sieve tributaries]} _ _]  
  (println env)
  (println tributaries)
  (assoc env title (apply sieve (map env tributaries))))

(defmethod parse-outline nil 
  [env _ _ _ ]
  env)

;Horrible side effects...would be better to find a solution to this!

(defmethod parse-outline :aliased 
  [env {:keys [title sieve tributaries]} _ con] 
  (con (apply sieve (map env tributaries)) (title env))
  env)

(def ^:private outline {:title nil :tributaries nil :sieve nil})

(defn make-outline
  ([title tributaries sieve] (make-outline title tributaries sieve nil))
  ([title tributaries sieve group]
    (if group 
      (assoc outline :title title :tributaries tributaries :sieve sieve :group group)
      (assoc outline :title title :tributaries tributaries :sieve sieve))))

(def test-outline 
  [(make-outline :a [:a [:b-group]] (fn 
                                      ([] [0])
                                      ([& streams] (println streams) (s/map (fn [[a]] a) (apply s/zip streams)))))
   
   (make-outline :d [:c] (fn [a] a))
   
   (make-outline :f [:e] (fn [a] (s/consume #(println "f: " %) (s/map identity a))))
   
   (make-outline :e [:d]  (fn [a] a))
   
   (make-outline :c [:a] (fn [a] a))

   (make-outline :b [] (fn [] (s/periodically 1000 (fn [] [0]))) :b-group)])

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

(defn- make-graph 
  [outlines] 
  (reduce (fn [m {:keys [title]}]                      
            (assoc m title {:edges (dependents outlines title)}))                     
          {} outlines))
    
(defn assemble 
  [step con & outlines] 
  
  (let [compiler (fn [env o] (parse-outline env o step con))   
             
        ;#### Expand dependencies and infer types! ####
        
        [sccs with-deps] (let [groups (reduce (fn [m {:keys [title group]}] 
                                                (if group 
                                                  (update-in m [group] (fn [x] (conj x title))) 
                                                  m)) 
                                              {} outlines)
                        
                               deps-expanded (map (fn [o] (assoc o :tributaries (expand-dependencies groups (:tributaries o)))) outlines) 
                        
                               graph (make-graph deps-expanded)
                        
                               transpose (g/transpose graph)
                        
                               sccs (->>
                                      
                                      (g/strongly-connected-components graph (g/transpose graph))                                                                      
                                                                       
                                      (remove
                                        (fn [vals]
                                          (if (= (count vals) 1)
                                            (let [val (vals 0)]
                                            (not (val (:edges (val graph)))))))))
                        
                               pred (apply (comp set concat) sccs)]   
                    
                           [sccs (map (comp                           
                                   
                                        ;#### Tag components in cycles... ####   
                           
                                        (fn [o]                            
                                          (if ((:title o) pred)
                                            (assoc o :type :cyclic)
                                            o))
                           
                                        ;#### Infer graph types! ####
                           
                                        (fn [o]                        
                                          (let [title (:title o)                                
                                                graph-es (:edges (title graph))                               
                                                transpose-es (:edges (title transpose))]                            
                                            (if (empty? graph-es)
                                              (if (empty? transpose-es)
                                                (throw (IllegalArgumentException. "You have a node with no dependencies and no dependents..."))
                                                (assoc o :type :estuary))
                                              (if (empty? transpose-es)
                                                (assoc o :type :source) 
                                                (assoc o :type :river)))))) 
                         
                                      deps-expanded)])                                 
             
        ;#### Get the sources and cycles for future reference! ####
        
        sources (filter #(= (:type %) :source) with-deps)
        
        cycles (filter #(= (:type %) :cyclic) with-deps)
                   
        ;#### First compiler pass... ####
              
        env (reduce compiler {} (concat 
          
                                  sources 
                
                                  cycles 
                
                                  ;#### Do a topological sort on the remaining nodes #### 
                                  
                                  (let [non-cyclic (into {} (map (fn [o] [(:title o) o]) 
                                                                 (remove (fn [o]                 
                                                                           (let [type (:type o)]
                                                                             (or (= type :cyclic) (= type :source))))                  
                                                                           with-deps)))]
                                          
                                    (->> 
                                            
                                      (make-graph (vals non-cyclic))
                                            
                                      g/kahn-sort
                                            
                                      (map non-cyclic)))
                                           
                                  (map (fn [o] (assoc o :type :aliased)) (concat sources cycles))))]
       
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
  
  
  
  
  
  
  
  
  
  
  
  
  
        
         
         
           
           
           
           
           
           
                 