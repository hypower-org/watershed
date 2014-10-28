(ns networking.new-new-test
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

(defn start
  [requires provides ip neighbors]
  
  (def test-cpu (n/cpu {:requires requires :provides provides :ip ip :neighbors neighbors}))
    
  (apply w/assemble w/manifold-step w/manifold-connect (->>
      
                                                         (:system test-cpu)
	    
                                                         (cons (w/outline (first provides) [] (fn [] (s/periodically 1000 (fn [] :system-one)))))
                                                         (cons (w/outline :getting [(first requires)] (fn [stream] (s/consume #(println "getting: " %) stream))))
                                                         
                                                         )))

