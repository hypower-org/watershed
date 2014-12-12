(ns watershed.utils
  (:use [clojure.java.shell :only [sh]])
  (:require [no.disassemble :as d]))

(defn | 
  [init & fns] 
  ((apply comp (reverse fns)) init))

(defn time-now 
  []
  "Returns the current time"
  (. System (nanoTime)))

(defn time-passed
  [start-time]
  "Returns the time passed"
  (/ (double (- (. System (nanoTime)) start-time)) 1000000.0))

(defn ^double cpu-units
  []
  (let [^String result (filter identity (map (comp last #(if % (clojure.string/split % #"\s+")) #(re-matches #"CPU.*\d" %)) (clojure.string/split (:out (sh "lscpu")) #"\n")))]
    
    (* (read-string (first result)) (read-string (last result)))))

