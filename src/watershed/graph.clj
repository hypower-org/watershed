(ns watershed.graph
  (:require [clojure.set :as s]))

(defn- incoming? 
  [graph vertex] 
  (if vertex
    (some (comp vertex :edges) (vals graph))
    true))

(defn kahn-sort  
  "Topologically sorts a graph using the kahn algorithm, where graph is a DAG graph in the style {:vertex {:edges #{...}}}, l is the resulting order, and s is the nodes to be traversed." 
  ([graph] (kahn-sort graph [] (remove #(incoming? graph %) (keys graph))))
  ([graph l s]
    (if (empty? s)
      (if (every? (comp empty? :edges) (vals graph)) 
        l)  
      (let [n (first s)            
            m (:edges (n graph))         
            graph' (reduce (fn [g v] (update-in g [n :edges] disj v)) graph m)]     
        (recur graph' (conj l n) (concat (rest s) (filter (comp #(not (incoming? graph' %)) m) (keys graph'))))))))

(defn transpose 
  [graph]  
  (let [nodes (keys graph)] 
    (apply merge (map (fn [node]        
                         {node {:edges (reduce (fn [edges k]                                
                                                 (if (node (:edges (k graph)))                             
                                                   (conj edges k)                                          
                                                   edges))                            
                                               #{} nodes)}})                    
                      nodes))))

(defn- dfs-visit 
  ([graph node time] (dfs-visit graph node time nil))
  ([graph node time tree] 
    (if tree 
      (conj! tree node))   
    (->       
      (reduce (fn [graph vertex]                 
                (if-not (or (:start-time (vertex graph)) (:end-time (vertex graph)))           
                  (dfs-visit graph vertex time tree)              
                  graph))       
              (assoc-in graph [node :start-time] (swap! time inc))            
              (:edges (node graph)))     
      (assoc-in [node :end-time] (swap! time inc)))))

(defn dfs  
  [graph] 
  (let [time (atom 0)]
    (reduce (fn [graph vertex]            
              (if-not (or (:start-time (vertex graph)) (:end-time (vertex graph)))        
                (dfs-visit graph vertex time nil)             
                graph))            
            graph           
            (keys graph))))

(defn dfs-tree  
  [graph & {:keys [order] :or {order (vec (keys graph))}}] 
  (let [time (atom 0)      
        trees (vec (repeatedly (count order) (comp transient vector)))]   
    (reduce-kv (fn [graph cardinal vertex]            
                 (if-not (or (:start-time (vertex graph)) (:end-time (vertex graph)))            
                   (dfs-visit graph vertex time (trees cardinal))           
                   graph))            
               graph            
               order)    
    (map persistent! trees)))

(defn- get-unmarked 
  [graph] 
  (reduce 
    (fn [ret v]
      (if (:end-time (v graph))
        ret
        (reduced v)))
    nil
    (keys graph)))
              
(defn- tally-dfs   
  [dfs-result]   
  (->>   
    (map (fn [[k v]]            
           [k (:end-time v)])         
         dfs-result)    
    (sort-by second)   
    (map first)    
    reverse))
  
(defn strongly-connected-components  
  [graph transpose-graph]  
  (->>  
    (->   
      (dfs graph)  
      tally-dfs     
      vec)
    (dfs-tree transpose-graph :order)))

(defn- outgoing 
  [graph vertex] 
  (:edges (vertex graph)))

(defn- incoming 
  [graph vertex] 
  (reduce-kv 
    (fn [coll k v] 
      (if (contains? (:edges v) vertex)
        (conj coll k)
        coll))
    []
    graph))

(defn- sink? 
  [graph vertex] 
  (and (>= (count (incoming graph vertex)) 0) (<= (count (outgoing graph vertex)) 0)))

(defn- source? 
  [graph vertex] 
  (and (<= (count (incoming graph vertex)) 0) (> (count (outgoing graph vertex)) 0)))

(defn- degree 
  [graph vertex] 
  (+ (count (outgoing graph vertex)) (count (incoming graph vertex))))

(defn- highest-degree 
  [graph]
  (let [result (atom nil)]
    (reduce-kv 
      (fn [max k v]         
        (let [deg (degree graph k)]
          (if (> deg max) 
            (do
              (reset! result k)
              deg)
            max)))
      -1
      graph)
    @result))

(defn- remove-vertex 
  [graph vertex] 
  (let [graph' (dissoc graph vertex)] 
    (reduce
        (fn [m k]          
          (update-in m [k] (fn [v] (update-in v [:edges] (fn [es] (disj es vertex))))))
        graph'
        (keys graph')))) 

(defn prune 
   [graph pred] 
   (let [ks (keys graph)     
         to-remove (reduce 
                     (fn [coll k] 
                       (if (pred graph k)
                         (conj coll k) 
                         coll))
                     #{}
                     ks)]   
     (reduce 
       remove-vertex
       graph 
       to-remove)))


(defn remove-sinks 
  "Recursively removes sinks in a graph until none remain"
  [graph] 
  (let [helper (fn this 
                 [g]
                 (let [result (prune g (fn [g v] (sink? g v)))]
                   (if (= result g)
                     g 
                     #(this result))))]
    (trampoline helper graph)))

(defn remove-sources 
  "Recursively removes sources in a graph until none remain"
  [graph]
  (let [helper (fn this 
                 [g]
                 (let [result (prune g (fn [g v] (source? g v)))]
                   (if (= result g)
                     g 
                     #(this result))))]
    (trampoline helper graph)))

(defn to-graph 
  [graph f] 
  (reduce
        (fn [m k]          
          (update-in m [k] f k))
        graph
        (keys graph)))

(defn fvs 
  "Calculates an fvs given a graph by implementing a slightly altered version of the algorithm described in http://www.sciencedirect.com/science/article/pii/002001909390079O"
  [graph]
  (let [helper (fn this [coll graph] 
                 (let [graph++ (-> 
                                 (prune graph (fn [g v] (:marked (g v))))
                                 remove-sinks
                                 remove-sources)]     
                   (if (empty? graph++)
                     coll
                     (let [vertex (highest-degree graph++)]                                     
                       #(this (conj coll vertex) (-> 
                                                   
                                                   (to-graph graph++ (fn [v k] 
                                                                       (if (and (contains? (:edges v) vertex) (contains? (:edges (vertex graph)) k))
                                                                         (assoc v :marked true)
                                                                         v)))
                                                   
                                                   (remove-vertex vertex)))))))
        
        result (trampoline helper [] graph)]
    
    ;Ensure that removal of vertices makes a DAG
  
    (when (kahn-sort (reduce remove-vertex graph result))
      result)))
  
  











