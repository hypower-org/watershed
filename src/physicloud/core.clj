(ns physicloud.core
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [watershed.graph :as gr]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.networking :as net]
            [net.faucet :as f]
            [watershed.utils :as u]
            [clojure.pprint :as p]
            [manifold.stream :as s]))

;add in kernel creation

(defn monitor 
  
  [graph & {:keys [port] :or {port 10000}}] 
  
  (println graph)
  
  (let [graph (gr/transpose graph)     
        
        aqueduct (a/aqueduct (vec (keys graph)) port (gloss/string :utf-8 :delimiters ["\r\n"]))
        
        server (w/flow aqueduct)
        
        aq (:aqueduct aqueduct)] 
        
    (->
          
      (net/emit-monitor-outline (reify net/IServer 
                                  (client-sink [_ id] (:sink (id aq)))
                                  (client-source [_ id] (:source (id aq)))
                                  (close [_] (@server) (w/ebb aqueduct)))
                              
                    graph)
        
      w/assemble)))
    
;Move this to physicloud core...

(defn kernel 
  
  [ip neighbors & {:keys [port target-power max-power idle-power bcps] :or {port 10000}}]  
  
  (let [discovery-data (net/elect-leader neighbors)
        
        leader (:leader discovery-data)
        
        respondents (set (:respondents discovery-data))
        
        num-respondents (count respondents)
        
        without-leader (disj respondents leader)
        
        network (if (= leader ip) 
                  (monitor (reduce (fn [m r] (assoc m r {:edges [leader]}))                                    
                                   {ip {:edges without-leader}} without-leader)))
        
        faucet (f/faucet ip (name leader) port (gloss/string :utf-8 :delimiters ["\r\n"]))
        
        connected (w/flow faucet) ;Do error handling in the future!!!!
        
        ]
    
    (println "Chosen leader: " leader)
    
    (net/emit-kernel-outline (reify net/IClient 
                               (net/source [_] (:source faucet))
                               (net/sink [_] (:sink faucet))
                               (net/disconnect [_] (w/ebb faucet)))     
                 
                         ip respondents leader 
                 
                         {:state (vec (repeat num-respondents 0)) :control (vec (repeat (inc num-respondents) 0))                          
                          :id (.indexOf (vec respondents) ip)
                          :tar target-power :max max-power}
                         
                         idle-power
                         
                         max-power
                         
                         bcps)))

;(defn cpu 
;  
;  [ip graph neighbors & {:keys [port requires provides] :or {port 10000 requires [] provides []}}] 
;  
;  (let [chosen (:leader (net/elect-leader neighbors))]
;    
;    (d/let-flow [network (if (= chosen ip) (monitor graph))
;                 
;                 on-ebbed (if network (fn [] (w/ebb network)))
;                 
;                 faucet (w/flow (f/faucet ip (name chosen) port (gloss/string :utf-8 :delimiters ["\r\n"])))]
;      
;      (->
;      
;        (apply merge               
;              
;                (map (fn [x] 
;                       
;                       {x {:tributaries [] :sieve (fn [] (net/selector (fn [y] (x (read-string y))) (:sink faucet)))
;                           :type :source                                                   
;                           :on-ebbed on-ebbed}}) 
;                    
;                     requires))       
;                  
;        (#(apply merge % 
;                  
;                  (map (fn [x] 
;                         
;                         {(net/make-key "providing-" x) {:tributaries [x] 
;                                                         :sieve (fn [stream] (s/connect (s/map (fn [data] (str {x data})) stream) (:source faucet)))
;                                                         :type :estuary                                                                            
;                                                         :on-ebbed on-ebbed}}) 
;                       provides)))))))















