(ns piped.producers
  "Code relating to polling SQS messages from AWS and getting them onto channels."
  (:require [piped.utils :as utils]
            [piped.sqs :as sqs]
            [clojure.core.async :as async]
            [cognitect.aws.client.api.async :as api.async]
            [clojure.core.async.impl.protocols :as ap]))

; TODO: consider adding acceleration, not only velocity

(defn spawn-producer
  ([client queue-url return-chan]
   (spawn-producer client queue-url return-chan {}))

  ([client
    queue-url
    return-chan
    {:keys [MaxNumberOfMessages VisibilityTimeout]
     :or   {MaxNumberOfMessages 10 VisibilityTimeout 30}}]

   ; always start with a short poll, then we'll adjust
   ; our rate over time to align with the producer
   ; and we'll never exceed the rate of the consumer
   ; thanks to channel buffer backpressure
   (async/go-loop [WaitTimeSeconds 0]

     (if (ap/closed? return-chan)

       true

       (let [request
             {:op      :ReceiveMessage
              :request {:QueueUrl              queue-url
                        :MaxNumberOfMessages   MaxNumberOfMessages
                        :VisibilityTimeout     VisibilityTimeout
                        :WaitTimeSeconds       WaitTimeSeconds
                        :AttributeNames        ["All"]
                        :MessageAttributeNames ["All"]}}

             ; poll for messages
             {:keys [Messages] :or {Messages []}}
             (async/<! (api.async/invoke client request))

             ; messages either need to be acked, nacked, or extended
             ; by consumers before this deadline hits in order
             ; to avoid another worker gaining visibility
             deadline
             (async/timeout (- (* VisibilityTimeout 1000) 400))

             metadata
             {:deadline deadline :queue-url queue-url}

             Messages
             (mapv #(with-meta % metadata) Messages)

             abandoned
             (loop [[message :as messages] Messages]
               (if (not-empty messages)
                 (if (async/>! return-chan message)
                   (recur (rest messages))
                   messages)
                 []))]

         ; channel was closed with some received messages that won't be processed
         ; nack them so they become visible to others asap
         (if (not-empty abandoned)

           (do (async/<! (sqs/nack-many client abandoned)) true)

           (cond
             ; this set was empty, begin backing off the throttle
             (empty? Messages)
             (recur (utils/clamp 0 20 (dec WaitTimeSeconds)))

             ; this round was neither empty nor full, stay the course
             (< 0 (count Messages) MaxNumberOfMessages)
             (recur WaitTimeSeconds)

             ; this round was full, hit the gas!
             (= (count Messages) MaxNumberOfMessages)
             (recur (utils/clamp 0 20 (inc WaitTimeSeconds))))))))))