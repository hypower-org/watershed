(ns watershed.server-test
  (:require [lamina.core :as lamina]
            [aleph.udp :as aleph-udp]
            [gloss.core :as gloss]
            [aleph.tcp :as aleph]
            [manifold.deferred :as d]
            [watershed.core :as w]
            [net.aqueduct :as a]
            [net.faucet :as f]
            [clojure.pprint :as p]
            [manifold.stream :as s]))

(def test-aqueduct (a/aqueduct [:reef :coral] 10000 (gloss/string :utf-8 :delimiters ["\r\n"])))

(def server (w/flow test-aqueduct))

(def reef-client (f/faucet :reef "10.10.10.5" 10000 (gloss/string :utf-8 :delimiters ["\r\n"])))

(w/flow reef-client)

(defn client-stream
  [client streams]
  
  (doseq [s streams]
  
    (s/connect s client)))

(def test-network (-> 
                        
                    (w/watershed)            
                                             
                    (w/add-river (w/estuary :coral-client [:reef] (fn [x] (client-stream (:source (:coral (:aqueduct test-aqueduct))) x)) (fn [] (println "coral-client removed"))))
              
                    (w/add-river (w/source :coral (fn [] (s/map (fn [y] (str {:coral y})) (:sink (:coral (:aqueduct test-aqueduct))))) (fn [] (println "coral removed"))))
              
                    (w/add-river (w/source :reef (fn [] (s/map (fn [y] (str {:reef y})) (:sink (:reef (:aqueduct test-aqueduct))))) (fn [] (println "reef removed"))))
              
                    (w/add-river (w/estuary :reef-client [:coral] (fn [x] (client-stream (:source (:reef (:aqueduct test-aqueduct))) x)) (fn [] (println "reef-client removed"))))
              
                    ))

(w/flow test-network)

(s/consume #(println "Reef client: " %) (:sink reef-client))

(s/put! (:source reef-client) "1")











