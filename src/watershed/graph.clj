(ns watershed.graph)

(defn transpose 
  [graph]  
  (let [nodes (keys graph)] 
    (apply merge (map (fn [node]        
                         {node {:edges (reduce (fn [edges k]                                
                                                 (if (node (set (:edges (k graph))))                             
                                                   (conj edges k)                                          
                                                   edges))                            
                                               [] nodes)}})                    
                      nodes))))

(defn dfs-visit 
  [graph node time tree]
  
  (if tree 
    (conj! tree node))
    
  (-> 
      
    (reduce (fn [graph vertex]      
           
              (if-not (or (:start-time (vertex graph)) (:end-time (vertex graph)))
              
                (dfs-visit graph vertex time tree)
                
                graph))
          
            (assoc-in graph [node :start-time] (swap! time inc))
              
            (:edges (node graph)))
      
    (assoc-in [node :end-time] (swap! time inc))))

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











