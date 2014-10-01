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

(defprotocol IClient 
  (source [_])
  (sink [_])
  (disconnect [_]))

(defprotocol IServer
  (client-source [_ id])
  (client-sink [_ id])
  (close [_]))

(defn make-key 
  
  [append k] 
  
  (keyword (str append (name k))))

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

;######################UDP################################################

(defn- acc-fn 
  
  [[accumulation new]] 
  
  (merge accumulation new))

(defn- watch-fn 
  
  [watershed accumulation expected]
  
  (when (>= (count (keys accumulation)) expected)
    
    (Thread/sleep 5000) ;Do something better than this...
    
    (w/ebb watershed)))
  
(defn elect-leader 
  
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

;TODO rewrite monitor, kernel, and CPU to use general network connection protocols

(defn emit-monitor-outline 
  [server graph] 
  
  (reduce (fn [m agent] 
                    
          (assoc (assoc m (make-key "sink-" agent) {:tributaries [] :sieve (fn [] (client-sink server agent)) :type :source
                                                    :on-ebbed (fn [] (close server))})
                           
                 (make-key "source-" agent) {:tributaries (mapv #(make-key "sink-" %) (:edges (agent graph))) 
                                                  :sieve (fn [& streams] (doall (map #(s/connect % (client-source server agent)) streams)))
                                                  :type :estuary})) 
                  
        {} (keys graph)))

(defn emit-kernel-outline 
  [client ip respondents leader initial-data]   
  (let [num-respondents (count respondents)]
    
    (->
    
      (if (= leader ip)
      
        (->           
            
          (reduce (fn [m r]                   
                    (assoc m r 
                           {:tributaries [] 
                            :sieve (fn [] (selector (fn [y] (r (read-string y))) (sink client)))
                            :type :source}))                     
                    {} (disj respondents leader))
            
          (assoc 
              
            :monitor 
               
            {:tributaries respondents
             :sieve (fn [& streams] (s/map q/cloud-fn (apply s/zip streams)))
             :type :river}
               
            :providing-monitor 
               
            {:tributaries [:monitor] 
             :sieve (fn [stream] (s/connect (s/map (fn [data] (str {:monitor data})) stream) (source client)))
             :type :estuary}))        
          
        {:monitor 
           
         {:tributaries [] :sieve (fn [] (selector (fn [y] (:monitor (read-string y))) (sink client)))
          :type :source}
         
         ;Send computed data back over the network
           
         (make-key "providing-" ip)
         
         {:tributaries [ip] 
          :sieve (fn [stream] (s/connect (s/map (fn [data] (str {ip data})) stream) (source client)))
          :type :estuary}}        
           
         ;TODO add in a dam to check for "closeness" to a solution
                     
         )
      
      (assoc ip {:tributaries [ip :monitor]         
                 :sieve (fn [& streams] (s/map q/agent-fn (apply s/zip streams)))       
                 :on-ebbed (fn [] (disconnect client))
                 :initial initial-data       
                 :type :river}))))             

  
  

