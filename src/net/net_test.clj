(ns net.net-test
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [watershed.core :as w]
            [aleph.udp :as udp]
            [watershed.utils :as u]
            ))


;(def server (tcp/start-server (fn [a b] (s/connect a a)) {:port 10000}))
;
;(def client @(tcp/client {:host "localhost" :port 10000}))
;
;(s/consume #(println (b/convert % String)) client)
;
;(s/put! client (pr-str {:hi 1}))

(defn- acc-fn  
  [[accumulation new]] 
  (merge accumulation new))

(defn- watch-fn   
  [streams accumulation expected] 
  (when (>= (count (keys accumulation)) expected)   
    (Thread/sleep 5000) ;Do something better than this...
    (doall (map #(if (s/stream? %) (s/close! %)) streams))))

(defn elect-leader 
  [number-of-neighbors & {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
  
  (d/let-flow [leader (atom nil)
        
               socket (udp/socket {:port port :broadast true})]
    
    (w/assemble w/manifold-step
                
                w/manifold-connect 
                
                (w/make-outline :broadcast [] (fn [] (s/periodically interval (fn [] (pr-str (u/cpu-units))))))
                
                (w/make-outline :socket [:broadcast] (fn [stream] (s/connect stream socket)))
                
                (w/make-outline :result [:broadcast] (fn [x] (s/reduce merge (s/map identity x))))
                
                (w/make-outline :accumulator [:accumulator :socket] 
                                
                                (fn 
                                  ([] {})
                                  ([& streams] (s/map acc-fn (apply s/zip streams)))))
                
                {:title :watch
                 :tributaries [:accumulator]
                 :sieve (fn [streams stream] (s/consume #(watch-fn streams % number-of-neighbors) (s/map identity stream))) 
                 :type :dam})))                                                      

;(defn elect-leader 
;  
;  [neighbors & {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
;  
;  (let [leader (atom nil)
;        
;        g @(-> 
;        
;             (g/geyser port (gloss/string :utf-8))
;            
;             w/flow)       
;        
;        watershed (-> 
;                    
;                    {:broadcast {:tributaries [] 
;                                 :sieve (fn [] (s/periodically interval (fn [] {:host "10.10.10.255" :port port :message (str (u/cpu-units))})))
;                                 :type :source}
;                     
;                    :geyser {:tributaries [:broadcast] 
;                             :sieve (fn [x] (s/connect x (:source g)) (s/map (fn [y] {(keyword (:host y)) y}) (:sink g)))
;                             :type :river}                   
;                    
;                    :result {:tributaries [:geyser] 
;                             :sieve (fn [x] (s/reduce merge (s/map identity x)))
;                             :type :estuary}
;                    
;                    :accumulator {:tributaries [:accumulator :geyser] 
;                                  :sieve (fn [& streams] (s/map acc-fn (apply s/zip streams)))
;                                  :initial {}
;                                  :type :river}
;                    
;                    :watch {:tributaries [:accumulator] 
;                            :sieve (fn [w stream] (s/consume #(watch-fn w % neighbors) (s/map identity stream)))                           
;                            :type :dam}}         
;                                                                    
;                    (w/assemble))]
;    
;    (reduce-kv (fn [max k v] (let [cpu-power (read-string (:message v))] 
;                             
;                               (if (> cpu-power max)       
;                               
;                                 (do 
;                                   (reset! leader k)
;                                   cpu-power)
;                               
;                                 max)))
;                               
;               0
;                               
;               @(:output (:result (:watershed watershed))))
;  
;    {:leader @leader :respondents (keys @(:output (:result (:watershed watershed))))}))

