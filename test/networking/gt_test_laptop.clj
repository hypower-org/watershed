(ns networking.gt-test-laptop
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [net.physi-server :as n]
            [watershed.core :as w]
            [taoensso.nippy :as nippy]
            [aleph.udp :as udp]
            [networking.gt-test-math :as math]
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
(def data-3 (s/stream))
(def data-4 (s/stream))
(def data-5 (s/stream))
(def data-6 (s/stream))

(def pos-3 (atom []))
(def pos-4 (atom []))
(def pos-5 (atom []))
(def pos-6 (atom []))

(defn parse-incoming-vrpn
  [[_ _ _ id :as msg]]
  (case id   
    1 (s/put! data-1 msg)
    2 (s/put! data-2 msg)
    3 (do (swap! pos-3 conj msg) (s/put! data-3 msg))
    4 (do (swap! pos-4 conj msg) (s/put! data-4 msg))
    5 (do (swap! pos-5 conj msg) (s/put! data-5 msg))
    6 (do (swap! pos-6 conj msg) (s/put! data-6 msg))))

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
 
  [(w/outline :cloud [:one :two :three :four :five :six] (fn [& streams] (s/map math/cloud-fn (apply s/zip streams))))

   (w/outline :one [:one :cloud] 
              (fn 
                ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 1])
                ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
   
   (w/outline :two [:two :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 2])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams))))) 
      
   #_(w/outline :three [:three :cloud] 
                  (fn 
                    ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 3])
                    ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
   
   #_(w/outline :four [:four :cloud] 
                  (fn 
                    ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 4])
                    ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
   
   #_(w/outline :five [:five :cloud] 
                  (fn 
                    ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 5])
                    ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
   
   (w/outline :six [:six :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 6])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
   
   #_(w/outline :vrpn-1 [] (fn [] data-1))
   
   #_(w/outline :vrpn-2 [] (fn [] data-2))
   
   (w/outline :vrpn-3 [] (fn [] data-3))
   
   (w/outline :vrpn-4 [] (fn [] data-4))
   
   (w/outline :vrpn-5 [] (fn [] data-5))
   
   #_(w/outline :vrpn-6 [] (fn [] data-6))])

(defn -main 
  [ip]
  (let [t-sys (n/cpu {:ip ip :neighbors 4
                      :requires [:three :four :five] 
                      :provides [:vrpn-3 :vrpn-4 :vrpn-5 :cloud]})    
        sys (:system t-sys)]   
    (apply w/assemble w/manifold-step w/manifold-connect (concat sys system-outline))))
















