(ns watershed.watershed-test 
  (:use [watershed.core]))

(def test-fn (fn [x] (periodical x 1000 identity)))

(def test-system (->

                 (watershed)

                 (add-river (source :reef (fn [] (periodical [] 1000 (fn [] [1]))) (fn [] (println "reef removed :("))))
                 (add-river (river :coral [:reef] test-fn (fn [] (println "coral removed :("))))
                 (add-river (river :pond [:coral] test-fn (fn [] (println "pond removed :("))))
                 (add-river (river :lake [:coral] test-fn (fn [] (println "lake removed :("))))
                 (add-river (river :stream [:lake] test-fn (fn [] (println "stream removed :("))))
                 (add-river (estuary :creek [:lake] (fn [x] (s/consume println (apply s/zip x))) (fn [] (println "creek removed :("))))))

(flow test-system)

