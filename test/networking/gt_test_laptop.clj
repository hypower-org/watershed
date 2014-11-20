(ns networking.gt-test-laptop
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [net.physi-server :as n]
            [watershed.core :as w]
            [taoensso.nippy :as nippy]
            [aleph.udp :as udp]
            [watershed.utils :as u]))

(use 'incanter.core 'gloss.core 'gloss.io)

(def data-stream (s/stream))

(def vrpn-data-stream (->>
                        
                        (s/filter
                          not-empty                        
                          (decode-stream (s/map 
                                           #(b/convert % java.nio.Buffer)
                                           data-stream) 
                                         n/frame))                        
                        (s/map (fn [x] (mapv read-string (remove empty? (clojure.string/split x #"\s+")))))))

(def data-1 (s/stream))
(def data-2 (s/stream))

(defn parse-incoming-vrpn
  [[_ _ _ id :as msg]]
  (case id   
    1 (s/put! data-1 msg)
    2 (s/put! data-2 msg)))

(s/consume 
  parse-incoming-vrpn
  vrpn-data-stream)

(defn handler 
  [ch client-info]
  (println client-info)
  (s/connect ch data-stream))

(def vrpn-server (tcp/start-server handler {:port 8999}))

(defn dot-mult
  
  [m v] 
  
  (mapv #(* % v) m))

(defn ebe-mult
  
  [m1 m2]
  
  (mapv * m1 m2))

(defn dot-prod 
  
  [m1 m2]
  
  (reduce + (map * m1 m2)))

(defn ebe-add 
  
  [m1 m2] 
  
  (mapv + m1 m2))

(defn ebe-sub 
  [m1 m2]  
  (mapv - m1 m2))


(def system-outline
 
  [(w/outline :cloud [:agent-1 :agent-2] (fn [& streams] (s/map (fn [x] 
                                                                  
                                                                  (println "Cloud got: " x)
                                                                  
                                                                  (Thread/sleep 100)
                                                                  
                                                                  :cloud) (apply s/zip streams))))
   
   (w/outline :vrpn-1 [] #_(fn [] data-1) (fn [] (s/periodically 100 (fn [] [0 0 0 1]))))
   
   (w/outline :vrpn-2 [] #_(fn [] data-2) (fn [] (s/periodically 100 (fn [] [0 0 0 2]))))])

(defn -main 
  [ip]
  (let [t-sys (n/cpu {:ip ip :neighbors 3 :requires [:agent-1 :agent-2] :provides [:vrpn-1 :vrpn-2 :cloud]})
      
        sys (:system t-sys)]
    
    (apply w/assemble w/manifold-step w/manifold-connect (concat sys system-outline))))
















