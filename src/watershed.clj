(ns watershed
  (:use [lamina.core])
  (:require [manifold.deferred :as d]
            [manifold.stream :as s])
  (:import [lamina.core.channel Channel]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent Executors]
           [java.util.concurrent ScheduledThreadPoolExecutor]))

;(defn periodical
;  [streams opts]
;
;  (let [val (atom (vec (map (fn [x] nil) streams)))]
;
;   (loop [s streams]
;
;      (let [index (dec (count s))]
;
;       (s/consume (fn [x] (swap! val assoc index x)) (last s) )
;
;       (if (> index 0)
;
;          (recur (butlast s)))))
;
;    (s/map (:fn opts) (s/periodically (:period opts) (fn [] @val)))))


;(def a (s/stream))

;(def b (periodical [a] {:period 1000 :fn inc}))


(defn- dependents
  [system title]

  (list (reduce-kv

         (fn [x y z]

           (let [tributaries (:tributaries z)]

             (if (some (fn [w] (= title w)) tributaries)

               (conj x y)
               x)))

         #{} system)))

(defprotocol IWatershed
  (river [_ title tributaries stream flow dam])
  (dam [_ title]))

;Title is the name of the rivers
;Tributaries: The dependencies of a river (i.e., the things that compose it)
;Stream: the output of the river.
;Flow: How the tributaries flow into the river
;Dam: what happens when the river closes

(defrecord Watershed [system]

  IWatershed

  (river

   [_ title tributaries stream flow dam]

   (let [sources (map @system tributaries)

         not-avilable (reduce-kv (fn [x y z] (if (nil? z) (conj x y) x)) [] (zipmap tributaries sources))]

     (if (empty? not-avilable)

       (swap! system assoc title {:tributaries tributaries :stream stream :flow flow :dam dam})

       (throw (Exception. (str not-avilable " were not available")))))
   _)

  (dam
   [_ title]

   (let [dammed (atom [])]

     (loop [to-dam (dependents @system title)]

       (when-not (empty? to-dam)

         (swap! dammed conj to-dam)

         (recur (flatten (map (fn [x] (dependents @system x)) to-dam)))))

     (swap! dammed conj [title])

     ;close streams of dammed rivers and dissoc...

     (doseq [k (keys @system)]
       ((:dam (k @system)))
       (swap! system dissoc k))

     (set (flatten @dammed))))

  )

(defn watershed []
  (->Watershed (atom {})))



;TEST



(def a (watershed))

(river a :reef [] nil nil (fn [] ))

(river a :coral [:reef] nil nil (fn [] ))
(river a :pond [:coral] nil nil (fn [] ))
(river a :lake [:coral] nil nil (fn [] ))
(river a :carrot [:lake] nil nil (fn [] ))
(river a :beans [:lake] nil nil (fn [] ))
































































