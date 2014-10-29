(ns networking.new-new-test
  (:require [watershed.core :as w]
            [net.physi-server :as n]
            [manifold.stream :as s]
            [aleph.udp :as udp]
            [manifold.deferred :as d]))

(defn start
  [requires provides ip neighbors]
  
  (def test-cpu (n/cpu {:requires requires :provides provides :ip ip :neighbors neighbors}))
  
  (let [rs (if (empty? requires) requires [(w/outline :getting [(first requires)] (fn [stream] (s/consume #(println "getting: " %) (s/map identity stream))))])
        ps (if (empty? provides) provides [(w/outline (first provides) [] (fn [] (s/periodically 1000 (fn [] :system-one))))])]
    
    (apply w/assemble w/manifold-step w/manifold-connect (concat (:system test-cpu) rs ps))))