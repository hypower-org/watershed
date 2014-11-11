(ns networking.kobuki-client
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [taoensso.nippy :as nippy]
            [manifold.deferred :as d])
  (:import [edu.ycp.robotics KobukiRobot])
  (:gen-class))

(defn -main
  [ip]
  
  ;Provide initial data.
  
  (def robot (KobukiRobot. "/dev/ttyUSB0"))
  
  (loop [t-sys (n/cpu {:ip ip :neighbors 2 :requires [] :provides []})
         
         sys (cons   
               (w/outline :controller [:control-data] (fn [stream] (s/consume (fn [[v w]] (.control robot v w)) stream)))    
               (:system t-sys))
        
         c-sys (->>
      
                 sys
      
                 (apply w/assemble w/manifold-step w/manifold-connect))]
    
    
    (let [status (n/find-first #(= (:title %) :system-status) c-sys)]
      
      (when status
        (println status))
      
      (when (and status (= (:connection-status @(:output status)) :net.physi-server/disconnected))
        (println "Connection lost!  Reconnecting...")
        (let [t-sys (n/cpu {:ip ip :neighbors 2 :requires [] :provides []})
              sys (cons   
                    (w/outline :controller [:control-data] (fn [stream] (s/consume (fn [[v w]] (.control robot v w)) stream)))    
                    (:system t-sys))]
          (recur t-sys
          
                 sys
               
                 (->>
      
                   sys
      
                   (apply w/assemble w/manifold-step w/manifold-connect))))))))