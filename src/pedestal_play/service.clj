(ns pedestal_play.service
  (:require [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]
            [io.pedestal.http.body-params :as body-params]
            [ring.util.response :as ring-resp]
            [clojure.walk :as walk]
            [pedestal_play.deque :as dq]
            [clojure.string :as clj-str]))

(def agents (atom {}))
(def available-jobs (ref dq/empty-deque))
(def assigned-jobs (ref []))
(def completed-jobs (ref []))

;(def v [{:id 1 :urgent true :type "bills-questions"}
;              {:id 2 :urgent false :type "rewards-question"}
;              {:id 3 :urgent false :type "bills-questions"}
;              {:id 4 :urgent true :type "bills-questions"}
;              {:id 5 :urgent true :type "rewards-question"}])


;(save-job available-jobs (v 0))
;(save-job available-jobs (v 1))
;(save-job available-jobs (v 2))
;(save-job available-jobs (v 3))
;(save-job available-jobs (v 4))

;(deref available-jobs)
;(deref assigned-jobs)

;(assign-job available-jobs assigned-jobs (v 0) (@a 0))

;(def a (atom [{:id 123 :primary_skillset ["abc"] :secondary_skillset ["rewards-question"]}
;              {:id 456 :primary_skillset ["bills-questions" "abc"] :secondary_skillset ["rewards-question"]}]))


(defn get-agent-jobs-performed
  [ag]
  (get-in ag [:jobs-performed]))

(defn get-agent-stats
  [request]
  (let [agent-id (get-in request [:path-params :agent-id])]
    (http/json-response (get-agent-jobs-performed ((keyword agent-id) @agents)))))

(defn create-jobs-performed-att
  [primary secondary]
  (-> (into primary secondary)
      distinct
      (zipmap (repeat 0))
      walk/keywordize-keys))

(defn build-agent
  [ag]
  (assoc {} :name (:name ag)
            :primary_skillset (:primary_skillset ag)
            :secondary_skillset (:secondary_skillset ag)
            :jobs-performed (create-jobs-performed-att (:primary_skillset ag) (:secondary_skillset ag))))

(defn save-agent
  [ag]
  (swap! agents assoc (keyword (:id ag)) (build-agent ag)))

(defn is-agent-request-body-ok?
  [request-body]
  (if request-body
    (let [_id (:id request-body)
          _name (:name request-body)
          _primary-skillset (:primary_skillset request-body)
          _secondary-skillset (:secondary_skillset request-body)]
      (and (and (string? _id) (not (empty? (clj-str/trim _id))))
           (and (string? _name) (not (empty? (clj-str/trim _name))))
           (and (instance? clojure.lang.PersistentVector _primary-skillset) (every? string? _primary-skillset))
           (and (instance? clojure.lang.PersistentVector _secondary-skillset) (every? string? _secondary-skillset))))))

(defn agent-exists?
  [id agents]
  (if ((keyword id) agents)
    true
    false))

(defn add-agent
  [request]
  (if (is-agent-request-body-ok? (:json-params request))
    (if (agent-exists? (:id (:json-params request)) @agents)
      (-> (ring-resp/response "")
          (ring-resp/content-type "application/json")
          (ring-resp/status 409))
      (let [saved-agent (save-agent (:json-params request))]
      (ring-resp/created (str "http://localhost:8080/agent/" (:id (:json-params request))) saved-agent)))
    (-> (ring-resp/response "")
        (ring-resp/content-type "application/json")
        (ring-resp/status 403))))

(defn save-job
  [job-queue job]
  (dosync
    (if (:urgent job)
      (do
        (alter job-queue dq/insert-head-last job) job)
      (do
        (alter job-queue dq/insert-tail-last job) job))))

(defn is-job-request-body-ok?
  [request-body]
  (if request-body
    (let [_id (:id request-body)
          _type (:type request-body)
          _urgent (:urgent request-body)]
      (and (and (string? _id) (not (empty? (clj-str/trim _id))))
           (and (string? _type) (not (empty? (clj-str/trim _type))))
           (and (instance? Boolean _urgent))))))

(defn job-exists?
  [job-id available-jobs assigned-jobs completed-jobs]
  (or (some #(= (get-in % [:id]) job-id) available-jobs)
      (some #(= (get-in % [:job_id]) job-id) assigned-jobs)
      (some #(= (get-in % [:job_id]) job-id) completed-jobs)))

(defn add-job
  [request]
  (if (is-job-request-body-ok? (:json-params request))
    (if (job-exists? (:id (:json-params request)) (dq/get-seq @available-jobs) @assigned-jobs @completed-jobs)
      (-> (ring-resp/response "")
          (ring-resp/content-type "application/json")
          (ring-resp/status 409))
      (let [saved-job (save-job available-jobs (:json-params request))]
        (ring-resp/created (str "http://localhost:8080/job/" (:id (:json-params request))) saved-job)))
    (-> (ring-resp/response "")
        (ring-resp/content-type "application/json")
        (ring-resp/status 403))))

(defn format-queue-state
  [available-queue assigned-queue completed-queue]
  (assoc {} "available_jobs" available-queue
            "assigned_jobs"  assigned-queue
            "completed_jobs" completed-queue))

(defn get-queue-state
  [request]
  (http/json-response (format-queue-state (dq/get-seq @available-jobs) @assigned-jobs @completed-jobs)))

(defn contains-type?
  [coll tp]
  (some #(= tp %) coll))

(defn get-fittest-job-by-skillset
  [jobs ag skillset]
  (first (filter #(contains-type? (skillset ag) (:type %)) jobs)))

(defn create-assignment
  [job ag]
  (assoc {} :job_id (:id job) :job_type (:type job) :agent_id (:id ag)))

(defn assign-job
  [from-job-queue to-job-queue job ag]
  (dosync
    (if (:urgent job)
      (do
        (alter from-job-queue (fn [curr to-be-removed]
                                  (assoc curr :head
                                     (vec
                                       (remove #(= (:id %) (:id to-be-removed)) (:head curr))))) job)
        (let [assignment (create-assignment job ag)]
          (alter to-job-queue conj assignment)
            assignment))
      (do
        (alter from-job-queue (fn [curr to-be-removed]
                                  (assoc curr :tail
                                    (vec
                                      (remove #(= (:id %) (:id to-be-removed)) (:tail curr))))) job)
        (let [assignment (create-assignment job ag)]
          (alter to-job-queue conj assignment)
            assignment)))))

(defn find-new-job
  [from-job-queue to-job-queue ag]
  (let [primary-match (get-fittest-job-by-skillset (dq/get-seq @from-job-queue) ag :primary_skillset)]
    (if primary-match
      (assign-job from-job-queue to-job-queue primary-match ag)
      (let [secondary-match (get-fittest-job-by-skillset (dq/get-seq @from-job-queue) ag :secondary_skillset)]
        (if secondary-match
          (assign-job from-job-queue to-job-queue secondary-match ag))))))

(defn update-agent-jobs-performed
  [jobs agents ag-id]
  (map #(swap! agents update-in [(keyword (str ag-id)) :jobs-performed (keyword %)] inc) (mapv #(:job_type %) jobs)))

(defn complete-agent-jobs
  [from-job-queue to-job-queue agents ag-id]
  (let [ag-assigned-jobs (vec (filter #(= (:agent_id %) ag-id) @from-job-queue))]
    (when (not-empty ag-assigned-jobs)
      (dosync
        (alter to-job-queue (fn [curr ag]
                                (-> ag-assigned-jobs
                                    (conj curr)
                                    flatten
                                    vec)) ((keyword ag-id) @agents))
        (alter from-job-queue (fn [curr to-be-removed]
                                  (vec (remove #(= (:agent_id %) (:id to-be-removed)) curr))) ((keyword ag-id) @agents))
        (update-agent-jobs-performed ag-assigned-jobs agents ag-id)))))

(defn agent-not-found
  [agent-id]
  (assoc {} :error_msg (str "agent [id=" agent-id  "] not found!")))

(defn request-job
  [request]
  (let [ag-id (:agent_id (:json-params request))
        ag ((keyword ag-id) @agents)]
    (if ag
      (do
        (complete-agent-jobs assigned-jobs completed-jobs @agents ag-id)
        (let [job (find-new-job available-jobs assigned-jobs ag)]
          (if job
            ()
            ())))
      (http/json-response (agent-not-found (:agent_id (:json-params request)))))))

(def common-interceptors [(body-params/body-params) http/json-body])

(def routes #{["/agent" :post (conj common-interceptors `add-agent)]
              ["/agent-stats/:agent-id" :get (conj common-interceptors `get-agent-stats)]
              ["/job" :post (conj common-interceptors `add-job)]
              ["/queue-state" :get (conj common-interceptors `get-queue-state)]
              ["/request-job" :post (conj common-interceptors `request-job)]})

;; Map-based routes
;(def routes `{"/" {:interceptors [(body-params/body-params) http/html-body]
;                   :get home-page
;                   "/about" {:get about-page}}})

;; Terse/Vector-based routes
;(def routes
;  `[[["/" {:get home-page}
;      ^:interceptors [(body-params/body-params) http/html-body]
;      ["/about" {:get about-page}]]]])


;; Consumed by pedestal-play.server/create-server
;; See http/default-interceptors for additional options you can configure
(def service {:env :prod
              ;; You can bring your own non-default interceptors. Make
              ;; sure you include routing and set it up right for
              ;; dev-mode. If you do, many other keys for configuring
              ;; default interceptors will be ignored.
              ;; ::http/interceptors []
              ::http/routes routes

              ;; Uncomment next line to enable CORS support, add
              ;; string(s) specifying scheme, host and port for
              ;; allowed source(s):
              ;;
              ;; "http://localhost:8080"
              ;;
              ;;::http/allowed-origins ["scheme://host:port"]

              ;; Tune the Secure Headers
              ;; and specifically the Content Security Policy appropriate to your service/application
              ;; For more information, see: https://content-security-policy.com/
              ;;   See also: https://github.com/pedestal/pedestal/issues/499
              ;;::http/secure-headers {:content-security-policy-settings {:object-src "'none'"
              ;;                                                          :script-src "'unsafe-inline' 'unsafe-eval' 'strict-dynamic' https: http:"
              ;;                                                          :frame-ancestors "'none'"}}

              ;; Root for resource interceptor that is available by default.
              ::http/resource-path "/public"

              ;; Either :jetty, :immutant or :tomcat (see comments in project.clj)
              ;;  This can also be your own chain provider/server-fn -- http://pedestal.io/reference/architecture-overview#_chain_provider
              ::http/type :jetty
              ;;::http/host "localhost"
              ::http/port 8080
              ;; Options to pass to the container (Jetty)
              ::http/container-options {:h2c? true
                                        :h2? false
                                        ;:keystore "test/hp/keystore.jks"
                                        ;:key-password "password"
                                        ;:ssl-port 8443
                                        :ssl? false}})
