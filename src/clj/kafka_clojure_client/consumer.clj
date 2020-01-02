(ns kafka-clojure-client.consumer
  (:import (cn.leancloud.kafka.consumer LcKafkaConsumerBuilder LcKafkaConsumer
                                        CatchAllExceptionConsumerRecordHandler RetriableConsumerRecordHandler
                                        ConsumerRecordHandler)
           (java.util.function BiConsumer)))

(defn- ^LcKafkaConsumerBuilder create-builder [configs msg-handler {:keys [poll-timeout-ms
                                                                           worker-pool
                                                                           graceful-shutdown-timeout-ms
                                                                           recommit-interval-ms
                                                                           shutdown-worker-pool-on-stop
                                                                           max-pending-async-commits
                                                                           key-deserializer
                                                                           value-deserializer]
                                                                    :or   {poll-timeout-ms           100
                                                                           max-pending-async-commits 10}}]
  (let [builder (if (and key-deserializer value-deserializer)
                  (LcKafkaConsumerBuilder/newBuilder configs
                                                     msg-handler
                                                     key-deserializer
                                                     value-deserializer)
                  (LcKafkaConsumerBuilder/newBuilder configs msg-handler))]
    (.pollTimeoutMillis builder (long poll-timeout-ms))
    (when recommit-interval-ms
      (.recommitIntervalInMillis builder (long recommit-interval-ms)))
    (when graceful-shutdown-timeout-ms
      (.gracefulShutdownTimeoutMillis builder (long graceful-shutdown-timeout-ms)))
    (when worker-pool
      (.workerPool builder worker-pool (or shutdown-worker-pool-on-stop true)))
    (.maxPendingAsyncCommits builder (int max-pending-async-commits))
    builder))

(defn ^ConsumerRecordHandler catch-all-exception-record-handler
  ([handler] (CatchAllExceptionConsumerRecordHandler. handler))
  ([handler error-consumer]
   (CatchAllExceptionConsumerRecordHandler. handler (reify BiConsumer
                                                      (accept [_ record throwable]
                                                        (error-consumer record throwable))))))

(defn ^ConsumerRecordHandler retriable-record-handler [handler max-retry-times]
  (RetriableConsumerRecordHandler. handler max-retry-times))

(defn to-record-handler [handler-fn]
  (reify ConsumerRecordHandler
    (handleRecord [_ record]
      (handler-fn record))))

(defn to-value-only-record-handler [handler-fn]
  (reify ConsumerRecordHandler
    (handleRecord [_ record]
      (handler-fn (.value record)))))

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

(defn subscribe [^LcKafkaConsumer consumer topics]
  (let [topics (if (sequential? topics) topics [topics])]
    (.subscribe consumer topics)
    consumer))

(defn close [^LcKafkaConsumer consumer]
  (.close consumer))