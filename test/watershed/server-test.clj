(ns server-test
  (:require [lamina.core :as lamina]
          [aleph.udp :as aleph-udp]
          [gloss.core :as gloss]
          [aleph.tcp :as aleph]
          [manifold.deferred :as d]
          [watershed :as w]
          [manifold.stream :as s]))

(def test-aqueduct (aqueduct [:reef :coral]))

(def server (flow test-aqueduct))

(def reef-client (lamina/wait-for-result (aleph/tcp-client {:host "localhost",
                                                :port 10000,
                                                :frame (gloss/string :utf-8 :delimiters ["\r\n"])})))

;Client connects to server as :reef.

(lamina/enqueue reef-client ":reef")

(defn client-stream
  [client streams]
  
  (println streams)
  
  (doseq [s streams]
  
    (s/connect s client)))

(def kernel (-> 
                        
              (w/watershed)            
                                             
              (w/add-river (w/estuary :coral-client [:reef] (fn [x] (client-stream (:source (:coral (:aqueduct test-aqueduct))) x)) (fn [] (println "coral-client removed"))))
              
              (w/add-river (w/source :coral (fn [] (s/map (fn [y] (str {:coral y})) (:sink (:coral (:aqueduct test-aqueduct))))) (fn [] (println "coral removed"))))
              
              (w/add-river (w/source :reef (fn [] (s/map (fn [y] (str {:reef y})) (:sink (:reef (:aqueduct test-aqueduct))))) (fn [] (println "reef removed"))))
              
              (w/add-river (w/estuary :reef-client [:coral] (fn [] (s/map (fn [y] (str {:reef y})) (:sink (:reef (:aqueduct test-aqueduct))))) (fn [] (println "reef-client removed"))))
              
              ))

(w/flow networked-system)

(lamina/receive-all reef-client #(println "Reef client: " %))

;(lamina/enqueue reef-client "1")