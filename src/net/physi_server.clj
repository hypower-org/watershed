(ns net.physi-server
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [watershed.core :as w]
            [taoensso.nippy :as nippy]
            [aleph.udp :as udp]
            [watershed.utils :as u]))

(def ^:private B-ary (Class/forName "[B"))

(defn- defrost 
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
  [{:keys [host port interval] :or {port 10000 interval 750}}]
  (assert (some? host) "A host IP address must be specified.")  
  (let [c-data {:host host :port port}]
    (d/loop [c (->                         
              (tcp/client c-data)                         
              (d/timeout! (+ interval 500) nil))]          
       (d/chain
         c
         (fn [x] 
           (if x
             x    
             (do 
               (println "Connecting...")
               (d/recur (-> 
                          (tcp/client c-data)
                          (d/timeout! (+ interval 500) nil)))))))))  )

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
           (d/chain v (fn [x] (if (s/closed? output)                                
                                (s/close! s)                              
                                (let [result (pred x)]                                  
                                  (if result (s/put! output result)) 
                                  (d/recur (s/take! s)))))))        
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
        
        msg (do 
              
              (println(let [split (butlast (clojure.string/split "10.10.10.5" #"\."))]
                       (str (clojure.string/join (interleave split (repeat (count split) "."))) "255")))
              
              {:message (nippy/freeze (u/cpu-units)) :port port 
               :host (let [split (butlast (clojure.string/split "10.10.10.5" #"\."))]
                       (str (clojure.string/join (interleave split (repeat (count split) "."))) "255"))})
        
        system (w/assemble w/manifold-step
                
                           w/manifold-connect 
                
                           (w/outline :broadcast [] (fn [] (s/periodically interval (fn [] {:message (nippy/freeze (u/cpu-units)) 
                                                                                            :host "10.10.10.255"
                                                                                            :port port}))))
                
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
                (println v')
                (println max)
                 (if (> v' max)
                   (do 
                     (println "reset on " v')
                     (reset! leader k)
                     v')
                   max)))            
            -1            
            @(apply w/output :result system))
    [@leader (map name (keys @(apply w/output :result system)))]))      

(defn- find-first
  [pred coll] 
  (first (filter pred coll)))

(defn cpu 
  [{:keys [requires provides ip port neighbors] :or {port 10000}}]
  {:pre [(some? requires) (some? provides) (some? neighbors)]}
  
  (let [[leader respondents] (elect-leader ip neighbors :port port) 
        
        client (physi-client {:host leader :port port})     
        
        server (if (= leader ip) @(apply physi-server ip respondents))
        
        client @client]    
    
    (s/put! client (nippy/freeze [requires provides ip]))  
    
    (if server      
      (let [woserver (dissoc server ::cleanup)        
            cs (keys woserver)
            ss (vals woserver)]    
        @(d/chain (apply d/zip (map s/take! ss)) (fn [responses] 
                                                   (let [connections (doall (map (fn [r] (apply hash-map (doall (interleave [:requires :provides :ip] 
                                                                                                                            (defrost r)))))                                                                
                                                                                   responses))]       
                                                     #_(println "responses: " responses)                                                    
                                                     #_(println connections)                                                    
                                                       (doall (map (fn [c connected-to]      
                                                                     (println "Connecting " connected-to " to " c)
                                                                     (doall (map #(s/connect (get server (:ip %)) (get server c)) connected-to)))  
                                                                   
                                                                   cs                      
                                                                   
                                                                   (reduce (fn [coll c]   
                                                                             #_(println c)
                                                                             (conj coll (filter (fn [x] 
                                                                                                  (some (set (:requires (find-first #(= c (:ip %)) connections))) (:provides x)))                          
                                                                                                connections)))                                                      
                                                                           []                                                          
                                                                           cs))))))))     
    
      (-> 
      
        (let [ret {:client client}]
          (if server
            (assoc ret :server server)
            ret))
        
        (assoc :system 
               
               (let [rs (let [rs' (map (fn [r] (w/outline r [:client] (fn [stream] (selector (fn [packet]                                                                                              
                                                                                                (let [[sndr val] (defrost packet)]
                                                                                                  (println "selec: " sndr val)
                                                                                                  (if (= sndr r) val))) stream)))) 
                                        requires)]
                          (if (empty? rs') 
                            rs'
                            (cons (w/outline :client [] (fn [] client)) rs')))
                     
                     ps (let [ps' (map (fn [p] (w/outline (make-key "providing-" p) [p]                                       
                                                           (fn [stream] (s/map (fn [x] (nippy/freeze [p x])) stream))                                     
                                                           :data-out)) 
                   
                                        provides)]
                          (if (empty? ps')
                            ps' 
                            (cons (w/outline :out [[:data-out]] (fn 
                                                                 [& streams] 
                                                                 (doseq [s streams] 
                                                                   (s/connect s client)))) 
                                  ps')))]
                 
                 (concat rs ps))))))

  
  
  
  


















