(ns leancloud-kafka-client.consumer
  (:import (cn.leancloud.kafka.client.consumer LcKafkaConsumerBuilder LcKafkaConsumer SafetyNetMessageHandler TriConsumer RetriableMessageHandler MessageHandler)))

(defn- create-builder [configs msg-handler {:keys [poll-timeout-ms
                                                   worker-pool
                                                   shutdown-worker-pool-on-stop
                                                   max-consecutive-async-commits
                                                   key-serializer
                                                   value-serializer]
                                            :or   {poll-timeout-ms               100
                                                   max-consecutive-async-commits 10}}]
  (let [builder (if (and key-serializer value-serializer)
                  (LcKafkaConsumerBuilder/newBuilder configs
                                                     msg-handler
                                                     key-serializer
                                                     value-serializer)
                  (LcKafkaConsumerBuilder/newBuilder configs msg-handler))]
    (.pollTimeoutMs builder poll-timeout-ms)
    (.messageHandler builder msg-handler)
    (when worker-pool
      (.workerPool builder worker-pool (or shutdown-worker-pool-on-stop true)))
    (.maxConsecutiveAsyncCommits builder (int max-consecutive-async-commits))
    builder))

(defn ^MessageHandler safety-net-message-handler
  ([handler] (SafetyNetMessageHandler. handler))
  ([handler error-consumer]
   (SafetyNetMessageHandler. handler (reify TriConsumer
                                       (accept [_ topic value throwable]
                                         (error-consumer topic value throwable))))))

(defn ^MessageHandler retriable-message-handler [handler max-retry-times]
  (RetriableMessageHandler. handler max-retry-times))

(defn to-message-handler [handler-fn]
  (reify MessageHandler
    (handleMessage [_ topic value]
      (handler-fn topic value))))

(defn ^LcKafkaConsumer create-sync-commit-consumer [kafka-configs msg-handler & opts]
  (.buildSync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-partial-sync-commit-consumer [kafka-configs msg-handler & opts]
  (.buildPartialSync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-async-commit-consumer [kafka-configs msg-handler & opts]
  (.buildAsync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-partial-async-commit-consumer [kafka-configs msg-handler & opts]
  (.buildPartialAsync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-auto-commit-consumer [kafka-configs msg-handler & opts]
  (.buildAuto (create-builder kafka-configs msg-handler opts)))

(defn subscribe [^LcKafkaConsumer consumer topic]
  (.subscribe consumer topic)
  consumer)

(defn close [^LcKafkaConsumer consumer]
  (.close consumer))