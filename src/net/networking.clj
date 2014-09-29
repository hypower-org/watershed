(ns net.networking
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [physicloud.quasi-descent :as q]
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
  
  (when (>= (count (keys accumulation)) expected)
    
    (Thread/sleep 5000) ;Do something better than this...
    
    (w/ebb watershed)))
  
(defn- elect-leader 
  
  [neighbors & {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
  
  (let [leader (atom nil)
        
        g @(-> 
        
             (g/geyser port (gloss/string :utf-8))
            
             w/flow)       
        
        watershed (-> 
                    
                    {:broadcast {:tributaries [] 
                                 :sieve (fn [] (s/periodically interval (fn [] {:host "10.10.10.255" :port port :message (str (u/cpu-units))})))
                                 :type :source}
                     
                    :geyser {:tributaries [:broadcast] 
                             :sieve (fn [x] (s/connect x (:source g)) (s/map (fn [y] {(keyword (:host y)) y}) (:sink g)))
                             :type :river}                   
                    
                    :result {:tributaries [:geyser] 
                             :sieve (fn [x] (s/reduce merge (s/map identity x)))
                             :type :estuary}
                    
                    :accumulator {:tributaries [:accumulator :geyser] 
                                  :sieve (fn [& streams] (s/map acc-fn (apply s/zip streams)))
                                  :initial {}
                                  :type :river}
                    
                    :watch {:tributaries [:accumulator] 
                            :sieve (fn [w stream] (s/consume #(watch-fn w % neighbors) (s/map identity stream)))                           
                            :type :dam}}         
                                                                    
                    (w/assemble))]
    
    (reduce-kv (fn [max k v] (let [cpu-power (read-string (:message v))] 
                             
                               (if (> cpu-power max)       
                               
                                 (do 
                                   (reset! leader k)
                                   cpu-power)
                               
                                 max)))
                               
               0
                               
               @(:output (:result (:watershed watershed))))
  
    {:leader @leader :respondents (keys @(:output (:result (:watershed watershed))))}))

(defn- make-key 
  
  [append k] 
  
  (keyword (str append (name k))))

(defn monitor 
  
  [graph & {:keys [port] :or {port 10000}}] 
  
  (println graph)
  
  (let [graph (gr/transpose graph)
        
        agents (keys graph)        
        
        aqueduct (a/aqueduct (vec (keys graph)) port (gloss/string :utf-8 :delimiters ["\r\n"]))
        
        server (w/flow aqueduct)
        
        aq (:aqueduct aqueduct)] 
        
    (->
          
      ;rewrite this to use reduce...
          
      (reduce (fn [m agent] 
                    
                (assoc (assoc m (make-key "sink-" agent) {:tributaries [] :sieve (fn [] (:sink (agent aq))) :type :source
                                                          :on-ebbed (fn [] (@server) (w/ebb aqueduct))})
                           
                       (make-key "source-" agent) {:tributaries (mapv #(make-key "sink-" %) (:edges (agent graph))) 
                                                        :sieve (fn [& streams] (doall (map #(s/connect % (:source (agent aq))) streams)))
                                                        :type :estuary})) 
                  
              {} agents)
        
      w/assemble)))

(defn kernel 
  
  [ip neighbors & {:keys [port max-power target-power] :or {port 10000}}]  
  
  (let [discovery-data (elect-leader neighbors)
        
        leader (:leader discovery-data)
        
        respondents (set (:respondents discovery-data))
        
        num-respondents (count respondents)
        
        without-leader (disj respondents leader)
        
        network (if (= leader ip) 
                  (monitor                              
                    (reduce (fn [m r] (assoc m r {:edges [leader]}))                                    
                            {ip {:edges without-leader}} without-leader)))
        
        faucet (f/faucet ip (name leader) port (gloss/string :utf-8 :delimiters ["\r\n"]))
        
        connected (w/flow faucet) ;Do error handling in the future!!!!
        
        ]
    
    (println respondents)
    (println without-leader)
    (println leader)
        
    (if (= leader ip)
          
      ;###########################THE MONITOR##################################
          
      (->
            
        ;Get network data from agents...
            
        (reduce (fn [m r]                   
                  (assoc m r 
                         {:tributaries [] 
                          :sieve (fn [] (selector (fn [y] (r (read-string y))) (:sink faucet)))
                          :type :source}))                     
                  {} without-leader)
            
        ;Add in monitor functionality + providing monitor data
            
        (merge 
              
          {ip 
         
           {:tributaries [ip :monitor]         
            :sieve (fn [& streams] (s/map q/agent-fn (apply s/zip streams)))         
            :initial {:state (vec (repeat num-respondents 0)) :control (vec (repeat (inc num-respondents) 0))                          
                      :id (.indexOf (vec respondents) ip) :max max-power :tar target-power}     
            :type :river}
                            
           :monitor 
               
           {:tributaries respondents
            :sieve (fn [& streams] (s/map q/cloud-fn (apply s/zip streams)))
            :type :river}
               
           :providing-monitor 
               
           {:tributaries [:monitor] 
            :sieve (fn [stream] (s/connect (s/map (fn [data] (str {:monitor data})) stream) (:source faucet)))
            :type :estuary}}))
          
      ;###########################NOT THE MONITOR##################################
          
      ;Get monitor data over the network 
          
      {:monitor 
           
       {:tributaries [] :sieve (fn [] (selector (fn [y] (:monitor (read-string y))) (:sink faucet)))
        :type :source}
           
       ;Use monitor data to computer next state
           
       ip 
         
       {:tributaries [ip :monitor]         
        :sieve (fn [& streams] (s/map q/agent-fn (apply s/zip streams)))         
        :initial {:state (vec (repeat num-respondents 0)) :control (vec (repeat (inc num-respondents) 0))                          
                  :id (.indexOf (vec respondents) ip) :max max-power :tar target-power}          
        :type :river}
         
       ;Send computed data back over the network
           
       (make-key "providing-" ip)
         
       {:tributaries [ip] 
        :sieve (fn [stream] (s/connect (s/map (fn [data] (str {ip data})) stream) (:source faucet)))
        :type :estuary}
           
           
       ;TODO add in a dam to check for "closeness" to a solution
           
           
       })))
               
(defn cpu 
  
  [ip graph neighbors & {:keys [port requires provides] :or {port 10000 requires [] provides []}}] 
  
  (let [chosen (:leader (elect-leader neighbors))]
    
    (d/let-flow [network (if (= chosen ip) (monitor graph))
                 
                 on-ebbed (if network (fn [] (w/ebb network)))
                 
                 faucet (w/flow (f/faucet ip (name chosen) port (gloss/string :utf-8 :delimiters ["\r\n"])))]
      
      (->
      
        (apply merge               
              
                (map (fn [x] 
                       
                       {x {:tributaries [] :sieve (fn [] (selector (fn [y] (x (read-string y))) (:sink faucet)))
                           :type :source                                                   
                           :on-ebbed on-ebbed}}) 
                    
                     requires))       
                  
        (#(apply merge % 
                  
                  (map (fn [x] 
                         
                         {(make-key "providing-" x) {:tributaries [x] 
                                                     :sieve (fn [stream] (s/connect (s/map (fn [data] (str {x data})) stream) (:source faucet)))
                                                     :type :estuary                                                                            
                                                     :on-ebbed on-ebbed}}) 
                       provides)))))))

  
  

