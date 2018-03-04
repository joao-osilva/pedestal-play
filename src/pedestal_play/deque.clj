(ns pedestal_play.deque)

(defrecord Deque [head tail])

(def empty-deque (Deque. [] []))

(defn insert-head-first
  [coll e]
  (assoc coll :head (->(:head coll)
                       rseq
                       vec
                       (conj e)
                       rseq
                       vec)))

(defn insert-head-last
  [coll e]
  (assoc coll :head (conj (:head coll) e)))

(defn insert-tail-first
  [coll e]
  (assoc coll :tail (->(:tail coll)
                       rseq
                       vec
                       (conj e)
                       rseq
                       vec)))

(defn insert-tail-last
  [coll e]
  (assoc coll :tail (conj (:tail coll) e)))

(defn remove-head-first
  [coll]
  (if (empty? (:head coll))
    coll
    (assoc coll :head (->(:head coll)
                         rseq
                         vec
                         pop
                         rseq
                         vec))))

(defn remove-head-last
  [coll]
  (if (empty? (:head coll))
    coll
    (assoc coll :head (pop (:head coll)))))

(defn remove-tail-first
  [coll]
  (if (empty? (:tail coll))
    coll
    (assoc coll :tail (->(:tail coll)
                         rseq
                         vec
                         pop
                         rseq
                         vec))))

(defn remove-tail-last
  [coll]
  (if (empty? (:tail coll))
    coll
    (assoc coll :tail (pop (:tail coll)))))

(defn get-head-current-idx
  [coll]
  (count (:head coll)))

(defn get-seq
  [coll]
  (into (:head coll) (:tail coll)))









