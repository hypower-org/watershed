(ns net.net-test
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [watershed.core :as w]
            [aleph.udp :as udp]
            [watershed.utils :as u]))

;(def server (tcp/start-server (fn [a b] (s/connect a a)) {:port 10000}))
;
;(def client @(tcp/client {:host "localhost" :port 10000}))
;
;(s/consume #(println (b/convert % String)) client)
;
;(s/put! client (pr-str {:hi 1}))
;
;(def test-sock (udp/socket {:port 8999 :broadcast? true}))
;
;(def test-sock @test-sock)
;
;(s/put! test-sock {:message "hi" :host "localhost" :port 8999})


(defn- acc-fn  
  [[accumulation new]] 
  (merge accumulation new))

(defn- watch-fn   
  [streams accumulation expected] 
  (when (>= (count (keys accumulation)) expected)   
    (Thread/sleep 5000) ;Do something better than this in the future...
    (doall (map #(if (s/stream? %) (s/close! %)) streams))))

(defn elect-leader 
  [ip number-of-neighbors & {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
  
  (let [leader (atom nil)
        
        socket @(udp/socket {:port port :broadast? true})
        
        system (w/assemble w/manifold-step
                
                           w/manifold-connect 
                
                           (w/outline :broadcast [] (fn [] (s/periodically interval (fn [] {:message (pr-str (u/cpu-units)) :port port :host ip}))))
                
                           (w/outline :connect [:broadcast] (fn [stream] (s/connect stream socket)))
                         
                           (w/outline :socket [] (fn [] (s/map (fn [x] (hash-map (:host x) x)) socket)))
                
                           (w/outline :result [:socket] (fn [x] (s/reduce merge (s/map identity x))))
                
                           (w/outline :accumulator [:accumulator :socket] 
                                
                                           (fn 
                                             ([] {})
                                             ([& streams] (s/map acc-fn (apply s/zip streams)))))
                
                           {:title :watch
                            :tributaries [:accumulator]
                            :sieve (fn [streams stream] (s/consume #(watch-fn streams % number-of-neighbors) (s/map identity stream))) 
                            :type :dam})]
    
    (reduce (fn [max [k v]]  
              (let [v' (read-string (b/convert (:message v) String))]
                 (if (> v' max)
                   (do 
                     (reset! leader k)
                     v'))
                 max))            
            -1            
            @(apply w/output :result system))
    @leader))       

