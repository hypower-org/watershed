(ns watershed.graph)

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

(defn dfs-visit 
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

(defn get-unmarked 
  [graph] 
  (reduce 
    (fn [ret v]
      (if (:end-time (v graph))
        ret
        (reduced v)))
    nil
    (keys graph)))
              
(defn tally-dfs   
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

(defn incoming? 
  [graph vertex] 
  (if vertex
    (some (comp vertex :edges) (vals graph))
    true))

(defn kahn-sort  
  "Topologically sorts a graph using the kahn algorithm.  Graph is a DAG graph in the style {:vertex {:edges ... }}, l is the resulting order, and s is the nodes to be traversed." 
  ([graph] (kahn-sort graph [] (remove #(incoming? graph %) (keys graph))))
  ([graph l s]
    (if (empty? s)
      (if (every? (comp empty? :edges) (vals graph)) 
        l)  
      (let [n (first s)            
            m (:edges (n graph))         
            graph' (reduce (fn [g v] (update-in g [n :edges] disj v)) graph m)]     
        (recur graph' (conj l n) (concat (rest s) (filter (comp #(not (incoming? graph' %)) m) (keys graph'))))))))
  
  











