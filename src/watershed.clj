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

(defprotocol IWater
  (ebb [_])
  (dam [_]))

(defn- dependents
  [system title]

  (reduce-kv

   (fn [x y z]

     (let [tributaries (keys (:tributaries z))]

       (if (some (fn [w] (= title w)) tributaries)

         (conj x y)
         x)))

   #{} system))

(defn- contains-many?
  [coll query-coll]

  (every? (fn [x] (some (fn [y] (= y x)) coll)) query-coll))

(defrecord River [title tributaries stream flow dam]

  IWater

  (ebb
   [_]

   (flow (vals tributaries)))

  (dam
   [_]

   (dam)))

(defprotocol IWatershed
  (add-river [_ river])
  (dam-river [_ title]))

;Title is the name of the rivers
;Tributaries: The dependencies of a river (i.e., the things that compose it)
;Stream: the output of the river.
;Flow: How the tributaries flow into the river
;Dam: what happens when the river closes


;Create system to start everything...


(defrecord Watershed [system]

  IWatershed

  (add-river

   [_ river]

   (assoc-in _ [:system (:title river)] river))

  (dam-river
   [_ title]

   @(run-pipeline

     title

     (fn
       [x]

       (loop [dammed []

              to-dam (dependents system x)]

         (if (empty? to-dam)

           (distinct (conj (mapcat identity dammed) title))

           (recur (conj dammed to-dam) (mapcat (fn [x] (dependents system x)) to-dam)))))

     (fn [x]

       (reduce (fn [y z] (dam (z system)) (dissoc y z)) system x)
       x)))

  IWater

  (ebb

   [_]

   ;(let [sources (map system tributaries)

   ;     not-avilable (reduce-kv (fn [x y z] (if (nil? z) (conj x y) x)) [] (zipmap tributaries sources))]

   ;(if (empty? not-avilable)

   ; (assoc-in _ [:system title] {:tributaries tributaries :stream stream :flow flow :dam dam})

   ;(throw (Exception. (str not-avilable " were not available")))))

   @(run-pipeline

     system

     ;In the future, parallelize starting

     (fn [system]

       (loop [possible (reduce-kv (fn [x y z] (if (empty? z) (conj x y) x)) [] (zipmap (keys system) (map keys (map :tributaries (vals system)))))

              start-order possible

              state (reduce dissoc system start-order)]

         (if (empty? state)

           start-order

           (let [new-possible (reduce-kv (fn [x y z] (if (or (empty? z) (contains-many? start-order z)) (conj x y) x)) [] (zipmap (keys state) (map keys (map :tributaries (vals system)))))]

             (recur new-possible

                    (reduce conj start-order new-possible)

                    (reduce dissoc state new-possible))))))

     (fn [x] (println x) x)

     (fn [start-order]

       ;Connect watershed streams to river streams

       (map ebb (map system start-order))

       _)

     )

   )

  (dam [_]))

(defn watershed []
  (->Watershed {}))

(defn river [title tributaries stream flow dam]
  (->River title (zipmap tributaries (repeatedly (count tributaries) s/stream)) stream flow dam))


;TEST######################

(def test-system (->

                  (watershed)

                  (add-river (river :reef [] nil nil (fn [] (println "reef removed :("))))
                  (add-river (river :coral [:reef] nil nil (fn [] (println "coral removed :("))))
                  (add-river (river :pond [:coral] nil nil (fn [] (println "pond removed :("))))
                  (add-river (river :lake [:coral] nil nil (fn [] (println "lake removed :("))))
                  (add-river (river :carrot [:lake] nil nil (fn [] (println "carrot removed :("))))
                  (add-river (river :beans [:lake] nil nil (fn [] (println "beans removed :("))))))

(dam-river test-system :reef)

























