(ns client-test
  (:require [lamina.core :as lamina]
          [aleph.udp :as aleph-udp]
          [gloss.core :as gloss]
          [aleph.tcp :as aleph]
          [manifold.deferred :as d]
          [watershed :as w]
          [manifold.stream :as s]))

(def coral-client (lamina/wait-for-result (aleph/tcp-client {:host "10.10.10.5",
                                                 :port 10000,
                                                 :frame (gloss/string :utf-8 :delimiters ["\r\n"])})))

(lamina/enqueue coral-client ":coral")

(lamina/receive-all coral-client #(println "Coral client: " %))

;(lamina/enqueue coral-client "1")



