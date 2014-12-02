(ns networking.barebones-net-test
  (:require [watershed.core :as w]
            [physicloudr.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

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
                       :requires [] :provides []})
         
         sys (:system t-sys)
        
         c-sys (->>
      
                 sys
      
                 (apply n/assemble-phy))]
    
    
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
      
                   (apply n/assemble-phy))))))))
    

