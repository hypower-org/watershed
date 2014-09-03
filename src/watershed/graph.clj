(ns watershed.graph)

(defn dfs-visit 
  [graph node time tree]
  
  (if tree 
    (swap! tree conj node))
    
  (-> 
      
    (reduce (fn [graph vertex]      
           
              (if-not (or (:start-time (vertex graph)) (:end-time (vertex graph)))
              
                (dfs-visit graph vertex time tree)
                
                graph))
          
            (assoc-in graph [node :start-time] (swap! time inc))
              
            (:edges (node graph)))
      
    (assoc-in [node :end-time] (swap! time inc))))

(defn dfs
  
  [graph ]
  
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
        
        trees (vec (repeatedly (count order) (comp atom vector)))]
    
    (reduce-kv (fn [graph cardinal vertex] 
            
                 (if-not (or (:start-time (vertex graph)) (:end-time (vertex graph)))
            
                   (dfs-visit graph vertex time (trees cardinal))
              
                   graph))
            
               graph
            
               order)
    
    (map deref trees)))

(defn tally-dfs 
  
  [dfs-result] 
  
  (reverse (map first 
       
            (sort-by :result 
                      
                     (reduce merge 
          
                         (map (fn [[k v]] 
            
                                {k (dissoc (assoc v :result (/ (:start-time v) (:end-time v))) :start-time :end-time)})
          
                                 dfs-result))))))

(defn cycles
  
  [graph transpose-graph] 
  
  (->> 
    
    (-> 
    
      (dfs graph)
    
      tally-dfs
      
      vec)
    
    (dfs-tree transpose-graph :order)
    
    (remove #(< (count %) 2))))
