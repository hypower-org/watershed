(ns networking.gt-test-bbb
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [net.physi-server :as n]
            [watershed.core :as w]
            [taoensso.nippy :as nippy]
            [aleph.udp :as udp]
            [watershed.utils :as u])
  (:gen-class))

(use 'incanter.core 'gloss.core 'gloss.io)

(defn sample 
  [period & streams]
  (let [clones (mapv #(s/map identity %) streams)
        vs (atom (into [] (repeat (count clones) nil)))
        output (s/stream)]
    (d/chain 
      (apply d/zip (doall (map s/take! clones)))
      (fn [vals]
        (reduce (fn [idx v] (swap! vs assoc idx v) (inc idx)) 0 vals)
        )
      (fn [x] 
        (reduce (fn [idx s] (s/consume #(swap! vs assoc idx %) (clones idx)) (inc idx)) 0 streams)               
        (s/connect (s/periodically period (fn [] @vs)) output)))
    output)) 
    
    
;(def data-stream (s/stream))
;
;(def vrpn-data-stream (->>
;                        
;                        (s/filter
;                          not-empty                        
;                          (decode-stream (s/map 
;                                           #(b/convert % java.nio.Buffer)
;                                           data-stream) 
;                                         n/frame))
;                        
;                        (s/map (fn [x] (mapv read-string (remove empty? (clojure.string/split x #"\s+")))))))
;
;(defn handler 
;  [ch client-info]
;  (println client-info)
;  (s/connect ch data-stream))
;
;(def vrpn-server (tcp/start-server handler {:port 8999}))

;left = (v - e w) / R right = (v + e q) / R
;e is half the idstance between the left and right wheels.  R is the radius of the wheels

(def max-speed 10)

(defn calculate-left-pwm
  [v w e r]
  (/ (/ (- v (* e w)) r) max-speed))

(defn calculate-right-pwm 
  [v w e r]
  (/ (/ (+ v (* e w)) r) max-speed))

(defn eucl-dis-sq 
  [m1 m2]
  (reduce + (map #(- %2 %) m1 m2)))

(defn go-to-goal
  [x-d x y-d y theta-d theta & {:keys [kw kv] :or {kw -13 kv -10.5}}]
  [(* kv (+ (* (- x x-d) (cos theta)) (* (- y y-d) (sin theta)))) (* kw (let [v (- theta theta-d)]
                                                                          (atan2 (sin v) (cos v))))])
(defn pid-fn 
  [x-d x y-d y theta]  
  (go-to-goal x-d x y-d y (atan2 (- y-d y) (- x-d x)) theta))

(defn controller-fn 
  [v w]
  (str "$PWM="(int (calculate-left-pwm v w 0.12192 0.0254)) "," (int (calculate-right-pwm v w 0.12192 0.0254))"*\n"))

(defn system-outline  
  [ip id x-d y-d]
  [#_(w/outline :vrpn [] (fn [] vrpn-data-stream))
   
   (w/outline (keyword (str "agent-" id)) [(keyword (str "agent-" id)) :cloud] (fn 
                                                                                 ([] (keyword (str "agent-" id)))
                                                                                 ([& streams] (s/map (fn [x] (keyword (str "agent-" id))) (apply s/zip streams)))))
  
   (w/outline :position-data [] (fn [] (s/periodically 1000 (fn [] [x-d y-d]))))
   
   (w/outline :desired-position [:position-data] (fn [stream] (sample 100 stream))) 
  
   (w/outline :pid [(keyword (str "vrpn-" id)) :desired-position] 
              (fn 
                [& streams]                            
                (s/map (fn [[[x y theta] [[x-d y-d]]]] (println "VRPN: " x y theta) (pid-fn x-d x y-d y theta)) (apply s/zip streams))))
  
   (w/outline :controller [:pid]
              (fn [stream]           
                (s/map #(apply controller-fn %) stream)))
  
   (w/outline :udp-socket [:controller]
              (fn [stream]              
                (def udp-socket @(udp/socket {:port 7889 :broadcast? true}))
                (s/consume #(do (println "UDP: " %) (s/put! udp-socket {:message % :port 5005 :host ip})) stream)))
   ])

(defn -main 
  [ip id neighbors x y]
  (let [t-sys (n/cpu {:ip ip :neighbors (read-string neighbors) :requires [(keyword (str "vrpn-" id)) :cloud] :provides [(keyword (str "agent-" id))]})
        
        sys (:system t-sys)]
    
    (apply w/assemble w/manifold-step w/manifold-connect (concat (system-outline ip id (read-string x) (read-string y)) sys))))














