(ns watershed
  (:use [lamina.core])
  (:require [manifold.deferred :as d]
            [manifold.stream :as s])
  (:import [lamina.core.channel Channel]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent Executors]
           [java.util.concurrent ScheduledThreadPoolExecutor]))

(defprotocol ITide
  (flow [_])
  (ebb [_]))

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

(defrecord River [title tributaries stream sieve on-dammed]

  ITide

  (flow
   [_]

   (s/connect (sieve (vals tributaries)) stream))

  (ebb
   [_]

   (doseq [s (vals tributaries)]
     (s/close! s))

   (s/close! stream)

   (on-dammed)))

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

       (reduce (fn [y z] (ebb (z system)) (dissoc y z)) system x))))

  ITide

  (flow

   [_]

   ;(let [sources (map system tributaries)

   ;     not-avilable (reduce-kv (fn [x y z] (if (nil? z) (conj x y) x)) [] (zipmap tributaries sources))]

   ;(if (empty? not-avilable)

   ; (assoc-in _ [:system title] {:tributaries tributaries :stream stream :flow flow :dam dam})

   ;(throw (Exception. (str not-avilable " were not available")))))

   @(run-pipeline

     system

     ;In the future, parallelize starting sequence

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

     (fn [x] (println "Starting order: " x) x)

     (fn [start-order]

       (doseq [riv start-order]

         (mapv s/connect (map :stream (map system (keys (:tributaries (riv system))))) (vals (:tributaries (riv system))))

         (flow (riv system)))

       _)))

  (ebb [_]))

(defn watershed []
  (->Watershed {}))

(defn river [title tributaries sieve on-dammed]
  (->River title (zipmap tributaries (repeatedly (count tributaries) s/stream)) (s/stream) sieve on-dammed))

(defn periodical
  [streams period fnc]

  (let [val (atom (vec (map (fn [x] nil) streams)))]

    (if (empty? @val)

      (s/periodically period fnc)

      (do
        (loop [s streams]

          (let [index (dec (count s))]

            (s/consume (fn [x] (swap! val assoc index x)) (last s))

            (if (> index 0)

              (recur (butlast s)))))

        (s/map (fn [x] (if-not (empty? x) (fnc x))) (s/periodically period (fn [] (mapcat identity @val))))))))

;TEST######################

(def test-fn (fn [x] (periodical x 1000 identity)))

(def test-system (->

                  (watershed)

                  (add-river (river :reef [] (fn [x] (periodical x 1000 (fn [] [1]))) (fn [] (println "reef removed :("))))
                  (add-river (river :coral [:reef] test-fn (fn [] (println "coral removed :("))))
                  (add-river (river :pond [:coral] test-fn (fn [] (println "pond removed :("))))
                  (add-river (river :lake [:coral] test-fn (fn [] (println "lake removed :("))))
                  (add-river (river :stream [:lake] test-fn (fn [] (println "stream removed :("))))
                  (add-river (river :creek [:lake] test-fn (fn [] (println "creek removed :("))))))

(flow test-system)

;(s/consume #(println "Beans: " %) (:stream (:creek (:system test-system))))

;(s/consume println (:stream (:reef (:system test-system))))

;(s/consume #(println "coral: " %) (:stream (:coral (:system test-system))))

;(dam-river test-system :reef)


