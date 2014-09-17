(ns net.networking
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.faucet :as f]
            [clojure.pprint :as p]
            [watershed.graph :as gr]
            [net.geyser :as g]
            [watershed.utils :as u]
            [manifold.stream :as s]))

;Move this to some sort of stream utils...

(defn selector 
  
  [f stream] 
  
  (let [s (s/map identity stream)
             
        output (s/stream)] 
         
         (d/loop 
           
           [v (s/take! s)]
           
           (d/chain v (fn [x] (if (s/closed? output) 
                                
                                (s/close! s) 
                                
                                (let [result (f x)]
                                  
                                  (if result (s/put! output result)) 
                                
                                  (d/recur (s/take! s)))))))
         
         output))

(defn- acc-fn 
  
  [[accumulation new]] 
  
  (merge accumulation new))

(defn- watch-fn 
  
  [watershed accumulation expected]
  
  (if (>= (count (keys accumulation)) expected)
    
    (w/ebb watershed)))
  
(defn- elect-leader 
  
  [neighbors & {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
  
  (let [leader (atom nil)
        
        g @(-> 
        
             (g/geyser port (gloss/string :utf-8))
            
             w/flow)       
        
        watershed (-> 
  
                    (w/watershed)
  
                    (w/add-river (w/source :broadcast (fn [] (s/periodically interval (fn [] {:host "10.10.10.255" :port port :message (str (u/cpu-units))})))))
  
                    (w/add-river (w/river :geyser [:broadcast] (fn [x] (s/connect x (:source g)) (s/map (fn [y] {(keyword (:host y)) y}) (:sink g)))))
  
                    (w/add-river (w/estuary :result [:geyser] (fn [x] (s/reduce merge (s/map identity x)))))             
           
                    (w/add-river (w/eddy :accumulator [:accumulator :geyser] (fn [& streams] (s/map acc-fn (apply s/zip streams))) {}))
           
                    (w/add-river (w/dam :watch [:accumulator] (fn [w stream] (s/consume #(watch-fn w % neighbors) stream))))
                                                                    
                    w/flow)]
    
    (reduce-kv (fn [max k v] (let [cpu-power (read-string (:message v))] 
                             
                               (if (> cpu-power max)       
                               
                                 (do 
                                   (reset! leader k)
                                   cpu-power)
                               
                                 max)))
                               
               0
                               
               @(:output (:result (:system watershed))))
  
    @leader))

(defn- make-key 
  
  [append k] 
  
  (keyword (str append (name k))))

(defn monitor 
  
  [graph & {:keys [port] :or {port 10000}}] 
  
  (let [graph (gr/transpose graph)
        
        agents (keys graph)        
        
        aqueduct (a/aqueduct (vec (keys graph)) port (gloss/string :utf-8 :delimiters ["\r\n"]))]
    
    (d/let-flow [server (w/flow aqueduct)]   
      
      (let [aq (:aqueduct aqueduct)]
        
        (->
    
          (reduce (fn [x y] (w/add-river x y)) (w/watershed) (mapcat (fn [x] [(w/source (make-key "sink-" x) (fn [] (:sink (x aq))))
                                                
                                                                              (w/estuary (make-key "source-" x) (mapv #(make-key "sink-" %) (:edges (x graph))) 
                                                                                         
                                                                                         (fn [& streams] (doall (map #(s/connect % (:source (x aq))) streams))))]) agents))
        
          w/flow)
        
        ))))
    
(defn cpu 
  
  [ip graph neighbors & {:keys [port requires provides] :or {port 10000 requires [] provides []}}] 
  
  (let [chosen (elect-leader neighbors)]
    
    (println chosen)
  
    (if (= chosen ip)
    
        (monitor graph))
  
    (d/let-flow [faucet (w/flow (f/faucet ip (name chosen) port (gloss/string :utf-8 :delimiters ["\r\n"])))]
      
      (->
      
        (reduce (fn [w r] (w/add-river w r)) 
              
                (w/watershed) 
              
                (map (fn [x] (w/source x (fn [] (selector (fn [y] (x (read-string y))) (:sink faucet))))) requires))
        
                  
        (#(reduce (fn [w r] (w/add-river w r)) % 
                  
                  (map (fn [x] (w/estuary (make-key "providing-" x) [x] 
                                         
                                          (fn [stream] (s/connect (s/map (fn [data] (str {x data})) stream) (:source faucet))))) provides))))           
      
        )))

  
  

