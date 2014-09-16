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
            [manifold.stream :as s]))
  
(defn elect-leader 
  
  [& {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
  
  (let [leader (atom nil)
        
        g @(-> 
        
        (g/geyser port (gloss/string :utf-8))
            
        w/flow)       
        
       watershed 

         (-> 
  
           (w/watershed)
  
           (w/add-river (w/source :broadcast  
                           
                                  (fn [] (s/periodically interval (fn [] {:host "10.10.10.255" :port port :message (str (u/cpu-units))})))))
  
           (w/add-river (w/river :geyser [:broadcast] 
                          
                                 (fn [x] (s/connect x (:source g)) (s/map (fn [x] {(keyword (:host x)) x}) (:sink g)))))
  
           (w/add-river (w/estuary :result [:geyser] 
                            
                                   (fn [x] (s/reduce merge x))))
    
           w/flow)]
  
    (Thread/sleep duration)
    
    (reduce-kv (fn [max k v] (let [cpu-power (read-string (:message v))] 
                             
                               (if (> cpu-power max)       
                               
                                 (do 
                                   (reset! leader k)
                                   cpu-power)
                               
                                 max)))
                               
               0
                               
               @(:result (w/ebb watershed)))
  
    @leader))

(defn- make-key 
  
  [append k] 
  
  (keyword (str append (name k))))

(defn- generate-system 
  
  "Generates a physicloud framework for a given network topology.  Expects a graph in the form of 
   
   {:CPU-1 [CPU-2] :CPU-2 [CPU-3] :CPU-3 []}"
  
  [aqueduct graph] 
  
  (let [agents (keys graph)
        
        system (w/watershed)
        
        server (:aqueduct aqueduct)]
    
    ;Attach agents to each other 
    
    (reduce (fn [x y] (w/add-river x y)) system (mapcat (fn [x] [(w/source (make-key "sink-" x) (fn [] (s/map (fn [y] (str {x y})) (:sink (x server)))))
                                                
                                                                 (w/estuary (make-key "source-" x) (mapv #(make-key "sink-" %) (x graph)) (fn [& streams] (doall (map #(s/connect % (:source (x server))) streams))))]) agents))))

(defn monitor 
  
  [graph & {:keys [port] :or {port 10000}}] 
  
  (let [agents (keys graph)        
        
        aqueduct (a/aqueduct (vec (keys graph)) port (gloss/string :utf-8 :delimiters ["\r\n"]))]
    
    (d/let-flow [server (w/flow aqueduct)]   
      
      (let [aq (:aqueduct aqueduct)]
      
        (generate-system aqueduct graph)
        
        (->
    
          (reduce (fn [x y] (w/add-river x y)) (w/watershed) (mapcat (fn [x] [(w/source (make-key "sink-" x) (fn [] (s/map (fn [y] (str {x y})) (:sink (x aq)))))
                                                
                                                                     (w/estuary (make-key "source-" x) (mapv #(make-key "sink-" %) (x graph)) (fn [& streams] (doall (map #(s/connect % (:source (x aq))) streams))))]) agents))
        
          w/flow)
        
        ))))
    
(defn cpu 
  
  [ip graph] 
  
  (if (= (elect-leader) ip)
    
      (monitor graph))
  
  (w/flow (f/faucet ip "10.10.10.5" 10000 (gloss/string :utf-8 :delimiters ["\r\n"]))))
  
  
  

