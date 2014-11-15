(ns networking.overload-client
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d])
  (:gen-class))

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn -main
  [ip neighbors]
  
  ;Provide initial data.
  
  (loop [t-sys (n/cpu {:ip ip 
                       :neighbors (let [t (type neighbors)]
                                    (if (= t java.lang.Long)
                                      neighbors
                                      (parse-int neighbors)))
                       :requires [] :provides [:overload]})
         
         sys (:system t-sys)
        
         c-sys (->>
      
                 sys
                 
                 (cons (w/outline :overload [] (fn [] (s/periodically 1 (fn [] [ip :data!])))))
      
                 (apply w/assemble w/manifold-step w/manifold-connect))]
    
    
    (let [status (n/find-first #(= (:title %) :system-status) c-sys)]
      
      (when (and status (= (:connection-status @(:output status)) :net.physi-server/disconnected))
        (println "Connection lost!  Reconnecting...")
        (let [t-sys (n/cpu {:ip ip :neighbors (let [t (type neighbors)]
                                                (if (= t java.lang.Long)
                                                  neighbors
                                                  (parse-int neighbors))) 
                            :requires [] :provides []})
              sys (:system t-sys)]
          
          (recur t-sys
          
                 sys
               
                 (->>
      
                   sys
                   
                   (cons (w/outline :overload [] (fn [] (s/periodically 1 (fn [] [ip :data!])))))
      
                   (apply w/assemble w/manifold-step w/manifold-connect))))))))
