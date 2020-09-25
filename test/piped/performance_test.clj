(ns piped.performance-test
  (:require [clojure.test :refer :all])
  (:require [piped.sweet :refer :all]
            [piped.core :as piped]
            [cognitect.aws.client.api :as aws]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [piped.sqs :as sqs]
            [piped.utils :as utils]
            [clojure.edn :as edn]))

(def queue-url "<SOME QUEUE URL>")

(defn send-message-batch [client queue-url messages]
  (let [op {:op :SendMessageBatch
            :request {:QueueUrl queue-url
                      :Entries (for [message messages]
                                 {:Id (str  (java.util.UUID/randomUUID))
                                  :MessageBody message})}}]
    (aws/invoke client op)))

(def acks (atom 0))

(defn spawn-acker
  "Acks messages in batches with ack tracking"
  [client input-chan]
  (async/go-loop []
    (when-some [batch (async/<! input-chan)]
      (let [response (async/<! (sqs/ack-many client batch))]
        (swap! acks (fn [a] (+ a (count (:Successful response)))))
        (when (utils/anomaly? response)
          (log/error "Error when trying to ack batch of messages." (pr-str response))))
      (recur))))

(def msg-id (atom 0))

(defn rand-msg []
  {:kind (rand-nth [:alert :warn])
   :message (swap! msg-id inc)})

(defn rand-msgs []
  (take 10 (repeatedly rand-msg)))

(def client (aws/client {:api :sqs}))

(comment
  (map #(send-message-batch client queue-url %)
       (take 5000 (repeatedly rand-msgs)))
  ; performance test here
  ; redef the acker to count
  (with-redefs-fn {#'piped.actions/spawn-acker spawn-acker}
    (fn []
      (defmultiprocessor proc [{:keys [Body]}]
        {:queue-url queue-url
         :consumer-parallelism 1000
         :transform #(update % :Body edn/read-string)}
        (:kind Body))

      (defmethod proc :alert [message]
        nil)

      (defmethod proc :warn [message]
        nil)
      (reset! acks 0)
      (piped/start #'proc)
      (Thread/sleep 5000)
      (let [i @acks]
        (piped/stop #'proc) i))))
