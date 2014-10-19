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
                                  
                                  (println "Selector: " x)
                                  
                                  (println "Selector result: " result)
                                  
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

(defn- result-fn
  [watershed final-states] 
  
  (when (every? :final (mapcat vals final-states))
    
    ;going to be something like {:10.10.10.5 {:final 5 :bcps 1000000 :idle 3 :max 8})
    ;Just do best fit for now.  BFD (best-fit decreasing)
    
    (println "I should be bin packing!")
    (w/ebb watershed)))

(defn- aggregator-fn 
  [states] 
  (apply merge states))

(defn- final-state-fn 
  [ip [last-state new-agent-state]] 
  (println "Final State: " new-agent-state)
  (let [new ((:state new-agent-state) (:id new-agent-state))
        last (get-in last-state [ip :old])] 
    (if (get-in last-state [ip :final])
      last-state
      (if (< (incanter.core/abs (- new last)) 0.00001)
        (assoc-in last-state [ip :final] new)
        (assoc-in last-state [ip :old] new)))))

(defn emit-kernel-outline 
  [client ip respondents leader initial-data idle-power max-power bcps]   
  (let [num-respondents (count respondents)]
    
    (->
    
      (if (= leader ip)
      
        (->           
            
          (reduce (fn [m r]                   
                    (assoc m r 
                           {:tributaries [] 
                            :sieve (fn [] (selector (fn [y] (println "Gettings stuff:" y) (r (read-string y))) (sink client)))
                            :type :source}))                     
                    {} (disj respondents leader))
          
          ;Getting final states from networked agents...
          
          (merge (reduce (fn [m r]                   
                           (assoc m (make-key "final-state-" r) 
                                  {:tributaries [] 
                                   :sieve (fn [] (selector (fn [y] ((make-key "final-state-" r) (read-string y))) (sink client)))
                                   :group :final-states 
                                   :type :source}))                     
                           {} (disj respondents leader)))
            
          (assoc 
              
            :monitor 
               
            {:tributaries respondents
             :sieve (fn [& streams] (s/map q/cloud-fn (apply s/zip streams)))
             :type :river}
            
            :data-gatherer 
            
            {:tributaries [ip]
             :sieve (fn [stream] (s/reduce concat (s/map identity stream)))
             :type :estuary}
               
            :providing-monitor 
               
            {:tributaries [:monitor] 
             :sieve (fn [stream] (s/connect (s/map (fn [data] (println "Providing monitor: " data) (pr-str {:monitor data})) (s/map identity stream)) (source client)))
             :type :estuary}          
            
            :aggregator 
            
            {:tributaries [[:final-states]] 
             :sieve (fn [& streams] (s/map aggregator-fn (apply s/zip streams)))
             :type :river}
            
            :result 
            
            {:tributaries [:aggregator]
             :sieve (fn [stream] (s/reduce merge (s/map identity stream)))
             :type :estuary}
            
            ))        
          
        {:monitor 
           
         {:tributaries [] :sieve (fn [] (selector (fn [y] (:monitor (read-string y))) (sink client)))
          :type :source}       
         
         (make-key "providing-" (make-key "final-state-" ip))
         
         {:tributaries [(make-key "final-state-" ip)] 
          :sieve (fn [stream] (s/connect (s/map (fn [data] (pr-str {(make-key "final-state-" ip) data})) stream) (source client)))
          :type :estuary}
         
         ;Send computed data back over the network
           
         (make-key "providing-" ip)
         
         {:tributaries [ip] 
          :sieve (fn [stream] (s/connect (s/map (fn [data] (pr-str {ip data})) stream) (source client)))
          :type :estuary}})
      
      (assoc ip 
             
             {:tributaries [ip :monitor]         
              :sieve (fn [& streams] (s/map q/agent-fn (apply s/zip streams)))       
              :initial initial-data       
              :type :river}
             
             (make-key "final-state-" ip)
         
             {:tributaries [(make-key "final-state-" ip) ip]
              :sieve (fn [& streams] (s/map (partial final-state-fn ip) (apply s/zip streams)))
              :initial {ip {:old 1 :bcps bcps :idle idle-power :max max-power}}
              :group :final-states
              :type :river}        
             
             :watch 
         
             {:tributaries [[:final-states]]
              :sieve (fn [w & streams] (s/consume (partial result-fn w) (apply s/zip streams)))      
              :type :dam}
             
             ))))          







(defn emit-task-assignment-outline 
  [client ip task-assignment]
  
  {:monitor 
   
   {:tributaries [] 
    :sieve (fn [] (s/periodically 1000 (fn [] task-assignment)))
    ;:on-ebbed (fn [] (disconnect client))
    :type :source}
   
   :watch 
   
   {:tributaries [:monitor] 
    :sieve (fn [w stream] (s/consume (fn [x] (if (ip x) (w/ebb w))) stream))
    :type :dam}
   
   :aggregator 
   
   {:tributaries [:monitor] 
    :sieve (fn [stream] (s/reduce merge (s/map (fn [x] (println "ID:" x) x) stream)))
    :type :estuary}
   
   :providing-monitor 
   
   {:tributaries [:monitor] 
    :sieve (fn [stream] (s/connect (s/map (fn [data] (str {:monitor data})) stream) (source client)))
    :type :estuary}})

(defn emit-task-reception-outline 
  [client ip] 
  
  {:monitor 
           
   {:tributaries [] :sieve (fn [] (selector (fn [y] (:monitor (read-string y))) (sink client)))
    ;:on-ebbed (fn [] (disconnect client))
    :type :source} 
   
   :aggregator 
   
   {:tributaries [:monitor] 
    :sieve (fn [stream] (s/reduce merge (s/map identity stream)))
    :type :estuary}
   
   :watch 
   
   {:tributaries [:monitor] 
    :sieve (fn [w stream] (s/consume (fn [x] (if (ip x) (w/ebb w))) stream))
    :type :dam}})
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

