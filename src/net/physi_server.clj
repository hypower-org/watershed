(ns net.physi-server
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [watershed.core :as w]
            [taoensso.nippy :as nippy]
            [aleph.udp :as udp]
            [watershed.utils :as u])
  (:use [gloss.core]
        [gloss.io]))

(def ^:private B-ary (Class/forName "[B"))
(def ^:private delimiter "|!|")
(def ^:private frame (string :utf-8 :delimiters [delimiter]))

(defn ^String delimit 
  [^String s]
  (str s delimiter))

(defn encode' 
  [msg]
  (encode frame (delimit (pr-str msg))))
  
(defn defrost 
  [msg] 
  (nippy/thaw (b/convert msg B-ary)))

(defn- handler 
  [f clients ch client-info]
  (let [index (.indexOf clients (:remote-addr client-info))]
    (if (> index -1)
      (do
        ;Debug, take out
        (println "Client: " client-info " connected.")
        (f index ch))
      (throw (IllegalStateException. (str "Unexpected client, " client-info ", tried to connect to the server."))))))

(defn physi-client 
  [{:keys [host port interval] :or {port 10000 interval 2000}}]
  
  (assert (some? host) "A host IP address must be specified.")  
  
  (let [c-data {:host host :port port :insecure? true :sll? true}]
    (d/loop [c (->                         
                 (tcp/client c-data)                         
                 (d/timeout! interval nil))]          
       (d/chain
         c
         (fn [x] 
           (if x
             x    
             (do 
               (println "Connecting...")
               (d/recur (-> 
                          (tcp/client c-data)
                          (d/timeout! interval nil))))))))))

(defn physi-server  
  "Creates a PhysiCloud server that waits for the given clients to connect."
  [{:keys [port] :or {port 10000}} & clients] 
  (let [clients (into [] clients)      
        ds (into [] (repeatedly (count clients) d/deferred))     
        f (fn [i x] (d/success! (ds i) x))                     
        server (tcp/start-server #(handler f clients % %2) {:port port})]    
    (d/chain (apply d/zip ds) (fn [x] (->                                                         
                                        (zipmap clients x)                                                         
                                        (assoc ::cleanup server))))))

(defn make-key   
  [append k]   
  (keyword (str append (name k))))

(defn selector  
  [pred stream]   
  (let [s (s/map identity stream)           
        output (s/stream)]         
         (d/loop           
           [v (s/take! s)]          
           (d/chain v (fn [x]                 
                        (if (s/closed? output)                                
                          (s/close! s)                                                              
                          (if (nil? x)
                            (s/close! output)
                            (do
                              (let [result (pred x)]                                  
                                (if result (s/put! output result))) 
                              (d/recur (s/take! s))))))))        
         output))

(defn take-within 
  [fn' stream timeout default] 
  (let [s (s/map identity stream)
        output (s/stream)]    
    (d/loop
      [v (d/timeout! (s/take! s) timeout default)]
      (d/chain v (fn [x] 
                   (if (s/closed? output)
                     (s/close! s)
                     (if (nil? x)
                       (do
                         (s/put! output default)
                         (s/close! output))
                       (if (= x default)
                         (do                        
                           (s/put! output default)
                           (s/close! s)
                           (s/close! output))                        
                         (do
                           (s/put! output (fn' x)) 
                           (d/recur (d/timeout! (s/take! s) timeout default)))))))))
    output))

(defn- acc-fn  
  [[accumulation new]] 
  (merge accumulation new))

(defn- watch-fn   
  [streams accumulation expected] 
  (when (>= (count (keys accumulation)) expected)   
    ;EW. Do something better than this in the future...
    (future
      (Thread/sleep 5000)
      (doall (map #(if (s/stream? %) (s/close! %)) streams)))))

(defn elect-leader 
  
  "Creates a UDP network to elect a leader.  Returns the leader and respondents [leader respondents]"
  
  [ip number-of-neighbors & {:keys [duration interval port] :or {duration 5000 interval 1000 port 8999}}]
  
  (let [leader (atom nil)
        
        socket @(udp/socket {:port port :broadcast? true})
        
        msg {:message (nippy/freeze (u/cpu-units)) :port port 
             :host (let [split (butlast (clojure.string/split "10.10.10.5" #"\."))]
                     (str (clojure.string/join (interleave split (repeat (count split) "."))) "255"))}
        
        system (w/assemble w/manifold-step
                
                           w/manifold-connect 
                
                           (w/outline :broadcast [] (fn [] (s/periodically interval (fn [] msg))))
                
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
              (let [v' (defrost (:message v))]
                 (if (> v' max)
                   (do 
                     (reset! leader k)
                     v')
                   max)))            
            -1            
            @(apply w/output :result system))
    [@leader (map name (keys @(apply w/output :result system)))]))      

(defn find-first
  [pred coll] 
  (first (filter pred coll)))

;Check to see if network is still cyclic...

(defn cleanup 
  [system]
  ((:server (::cleanup system))))

(defn cpu 
  [{:keys [requires provides ip port neighbors] :or {port 10000}}]
  {:pre [(some? requires) (some? provides) (some? neighbors)]}
  
  (let [[leader respondents] (elect-leader ip neighbors :port port) 
        
        client (physi-client {:host leader :port port})     
        
        server (if (= leader ip) @(apply physi-server ip respondents))
        
        client @client]    
    
    (s/put! client (pr-str [requires provides ip]))  
    
    (if server      
      (let [woserver (dissoc server ::cleanup)        
            cs (keys woserver)
            ss (vals woserver)]         
   
        @(d/chain (apply d/zip (map s/take! ss)) (fn [responses] 
                                                   (let [connections (doall (map (fn [r] (apply hash-map (doall (interleave [:requires :provides :ip] 
                                                                                                                            (read-string (b/convert r String))))))                                                                
                                                                                   responses))]       
                                                     #_(println "responses: " responses)                                                    
                                                     #_(println connections)            
                                                     
                                                       (doall (map (fn [c connected-to]                                                                        
                                                                     (doall (map (fn [x]
                                                                                   (when-not (or (= (:ip x) leader) (= leader c))
                                                                                     (println "Connecting " connected-to " to " c)
                                                                                     (s/connect (get server (:ip x)) (get server c)))) 
                                                                                 
                                                                                 connected-to)))  
                                                                   
                                                                   cs                      
                                                                   
                                                                   (reduce (fn [coll c]   
                                                                             #_(println c)
                                                                             (conj coll (filter (fn [x] 
                                                                                                  (some (set (:requires (find-first #(= c (:ip %)) connections))) (:provides x)))                          
                                                                                                connections)))                                                      
                                                                           []                                                          
                                                                           cs))))))
        
        (doseq [c cs]
          (when-not (= c ip)
            (s/connect (get server ip) (get server c))
            (s/connect (get server c) (get server ip))))
        
        (doseq [c cs]
          (when-not (= c ip)          
            (s/put! (get server c) (pr-str ::connected))))
        
        )
      ;Add in more complex checks here in the future
      (println (b/convert @(s/take! client) String)))     
    
      (-> 
      
        (let [ret {:client client}]
          (if server
            (assoc ret :server server)
            ret))
        
        (assoc :system 
               
               (let [hb-vector [:heartbeat]
                     
                     hb-r-vector [:heartbeat-received]
                     
                     status-map {:connection-status ::connected}
                     
                     rs (mapv (fn [r] (w/outline r [:client] (fn [stream] 
                                                               (selector (fn [packet]                                                                                              
                                                                           (let [[sndr val] packet]
                                                                             ;(println "sele: " sndr)
                                                                             (if (= sndr r) val))) stream)))) 
                             requires)
                     
                     ps (mapv (fn [p] (w/outline (make-key "providing-" p) [p]                                       
                                                 (fn [stream] (s/map (fn [x] #_(println "PROVIDING: " x) [p x]) stream))                                     
                                                 :data-out)) 
                   
                             provides)
                     
                     hb-resp (if (= leader ip)
                               [(w/outline :heartbeat-respond [:client]                                       
                                           (fn [stream] (selector (fn [packet]                                                                          
                                                                    (let [[sndr] packet]
                                                                      (if (= sndr :heartbeat)                                                                   
                                                                        (do
                                                                          (println "Got heartbeat on server!")
                                                                          hb-r-vector)))) stream))                               
                                           :data-out)]
                               [])
                     
                     hb-cl (if (= leader ip)
                             []                           
                             [(w/outline :heartbeat []
                                 (fn [] (s/periodically 5000 (fn [] hb-vector)))
                                 :data-out)
                              
                              (w/outline :heartbeat-receive 
                                         [:client]
                                         (fn [stream] 
                                           (selector (fn [packet]                                                                                              
                                                       (let [[sndr] packet]
                                                         (println "HBR: " sndr)
                                                         (if (= sndr :heartbeat-received)                                                                   
                                                           (do
                                                             (println "Got heartbeat on client!")
                                                             status-map)))) stream)))
                              
                              (w/outline 
                                :heartbeat-status 
                                [:heartbeat-receive]                      
                                (fn [stream] (take-within identity stream 20000 {:connection-status ::disconnected})))
                              
                              {:title :heartbeat-watch
                               :tributaries [:heartbeat-status]
                               :sieve (fn [streams stream] 
                                        (s/consume (fn [x] 
                                                     #_(println "HBW: " x)
                                                     (if (= (:connection-status x) ::disconnected)
                                                       (doall (map #(if (s/stream? %) (s/close! %)) streams)))) 
                                                   (s/map identity stream)))
                               :type :dam}
                              
                              (w/outline
                                 :system-status
                                 ;Change this to get a bunch of data...
                                 [:heartbeat-status]
                                 (fn [stream] (s/reduce merge (s/map identity stream))))
                              
                              ])
                     
                     ]               
                 
                 (->>
                   
                   (concat rs ps hb-resp hb-cl)
                   
                   (cons (w/outline :client-converted [] (fn [] (s/map #(b/convert % java.nio.Buffer) client))))
                   
                   (cons (w/outline :client [:client-converted] (fn [stream] 
                                                                  
                                                                  (->> 
                                                                    
                                                                    (decode-stream stream frame)
                                                                    
                                                                    (s/filter not-empty)
                                                                  
                                                                    (s/map (fn [x] 
                                                                             (println x) 
                                                                             (read-string x)))))))                   
                                    
                   (cons (w/outline :out 
                                    [[:data-out]] 
                                    (fn 
                                      [& streams] 
                                      (doseq [s streams]                                               
                                        (s/connect-via
                                          s                                           
                                          (fn [x]                                                                                                    
                                            (if-not (= (type x) java.lang.String)
                                              (do
                                                ;(println "data: " x)
                                                ;(Thread/sleep 10)
                                                (apply d/zip (doall (map #(s/put! client %) (encode' x)))))
                                              
                                              (println "SPURIOUS")
                                              ))  
                                          client)))))
                   
                                                                                        
                   
                   ))))))

  
  
  
  


















