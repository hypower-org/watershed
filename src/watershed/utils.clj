(ns watershed.utils)

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