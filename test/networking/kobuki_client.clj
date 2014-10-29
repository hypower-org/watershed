(ns networking.kobuki-client
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [taoensso.nippy :as nippy]
            [manifold.deferred :as d])
  (:import edu.ycp.robotics KobukiRobot))

(def robot (KobukiRobot. "/dev/ttyUSB0"))

(def kobuki-client 
  (apply w/assemble w/manifold-step  w/manifold-connect       
         (cons   
           (w/outline :controller [:control-data] (fn [stream] (s/consume (fn [[v w]] (control. robot (short v) (short w))) stream)))    
           (n/cpu {:requires [:control-data] :provides [] :ip "10.10.10.6" :neighbors 2}))))

