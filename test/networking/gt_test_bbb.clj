(ns networking.gt-test-bbb
  (:require [aleph.tcp :as tcp]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [byte-streams :as b]
            [net.physi-server :as n]
            [watershed.core :as w]
            [taoensso.nippy :as nippy]
            [aleph.udp :as udp]
            [networking.gt-test-math :as math]
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
  [(* kv (+ (* (- x x-d) (cos theta-d)) (* (- y y-d) (sin theta-d)))) (* kw (let [v (- theta theta-d)]
                                                                              (atan2 (sin v) (cos v))))])
(defn pid-fn 
  [x-d x y-d y theta]  
  (go-to-goal x-d x y-d y (atan2 (- y-d y) (- x-d x)) theta))

(defn controller-fn 
  [v w]
  (str "$PWM="(int (calculate-left-pwm v w 0.12192 0.0254)) "," (int (calculate-right-pwm v w 0.12192 0.0254))"*\n"))

(defn emit-agent-outline
  [id]
  (case id
    1 (w/outline :one [:one :cloud]
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 1])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
                                      
    2 (w/outline :two [:two :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 2])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
    3 (w/outline :three [:three :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 3])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
    4 (w/outline :four [:four :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 4])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
    
    5 (w/outline :five [:five :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 5])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))
    6 (w/outline :six [:six :cloud] 
                 (fn 
                   ([] [[0.0 0.5 0.5 0.0 -0.5 -0.5] [0.5 0.5 -0.5 -0.5 -0.5 0.5] [-1 -1 -1 -1] 6])
                   ([& streams] (s/map #(apply math/agent-fn %) (apply s/zip streams)))))))

(defn emit-agent-id 
  [id]
  (case id
    1 :one
    2 :two
    3 :three
    4 :four
    5 :five
    6 :six))

(defn system-outline  
  [ip id]
  (let [agent-outline (emit-agent-outline id)]
    [agent-outline
  
     (w/outline :position-data [(:title agent-outline)] (fn [stream] (s/map (fn [[x y _ id]] [(x id) (y id)]) stream)))
     
     (w/outline :position [:position-data] (fn [stream] (sample 100 stream)))
     
     (w/outline :pid [(keyword (str "vrpn-" id)) :position] 
                (fn 
                  [& streams]                            
                  (s/map (fn [[[x y theta] [[x-d y-d]]]] #_(println "VRPN: " x y theta x-d y-d) (pid-fn x-d x y-d y theta)) (apply s/zip streams))))
  
     (w/outline :controller [:pid]
                (fn [stream]           
                  (s/map #(apply controller-fn %) stream)))
  
     (w/outline :udp-socket [:controller]
                (fn [stream]              
                  (def udp-socket @(udp/socket {:port 7889 :broadcast? true}))
                  (s/consume #(do #_(println "UDP: " %) (s/put! udp-socket {:message % :port 5005 :host ip})) stream)))
     ]))

(defn -main 
  [ip id neighbors]
  (let [id (read-string id)
        t-sys (n/cpu {:ip ip :neighbors (read-string neighbors) 
                      :requires [(keyword (str "vrpn-" id)) :cloud] 
                      :provides [(emit-agent-id id)]})
        
        sys (:system t-sys)]    
    (apply w/assemble w/manifold-step w/manifold-connect (concat (system-outline ip id) sys))))














