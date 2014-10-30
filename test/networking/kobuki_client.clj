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
  (def robot (KobukiRobot. "/dev/ttyUSB0"))
  (def kobuki-client 
    (apply w/assemble w/manifold-step  w/manifold-connect       
           (cons   
             (w/outline :controller [:control-data] (fn [stream] (s/consume (fn [[v w]] (.control robot v w)) stream)))    
             (:system (n/cpu {:requires [:control-data] :provides [] :ip ip :neighbors 2}))))))




