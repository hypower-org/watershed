(ns watershed.time)

(defn time-now 
  []
  "Returns the current time"
  (. System (nanoTime)))

(defn time-passed
  [start-time]
  "Returns the time passed"
  (/ (double (- (. System (nanoTime)) start-time)) 1000000.0))