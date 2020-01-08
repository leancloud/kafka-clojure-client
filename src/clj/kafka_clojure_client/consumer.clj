(ns kafka-clojure-client.consumer
  (:require [clojure.tools.logging :as log])
  (:import (cn.leancloud.kafka.consumer LcKafkaConsumerBuilder LcKafkaConsumer
                                        CatchAllExceptionConsumerRecordHandler RetriableConsumerRecordHandler
                                        ConsumerRecordHandler UnsubscribedStatus)
           (java.util.function BiConsumer Consumer)
           (java.util.concurrent CompletableFuture)
           (java.util Collection)
           (java.util.regex Pattern)
           (java.time Duration)))

(defn- ^LcKafkaConsumerBuilder create-builder [configs msg-handler {:keys [poll-timeout-ms
                                                                           worker-pool
                                                                           graceful-shutdown-timeout-ms
                                                                           recommit-interval-ms
                                                                           shutdown-worker-pool-on-stop
                                                                           max-pending-async-commits
                                                                           handle-record-timeout-ms
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
    (when handle-record-timeout-ms
      (.handleRecordTimeoutMillis builder (long handle-record-timeout-ms)))
    (when graceful-shutdown-timeout-ms
      (.gracefulShutdownTimeoutMillis builder (long graceful-shutdown-timeout-ms)))
    (when worker-pool
      (.workerPool builder worker-pool (or shutdown-worker-pool-on-stop false)))
    (.maxPendingAsyncCommits builder (int max-pending-async-commits))
    builder))

(defn ^ConsumerRecordHandler catch-all-exception-record-handler
  ([handler] (CatchAllExceptionConsumerRecordHandler. handler))
  ([handler error-consumer]
   (CatchAllExceptionConsumerRecordHandler. handler (reify BiConsumer
                                                      (accept [_ record throwable]
                                                        (error-consumer record throwable))))))

(defn ^ConsumerRecordHandler retriable-record-handler
  ([handler max-retry-times]
   (RetriableConsumerRecordHandler. handler max-retry-times))
  ([handler max-retry-times retry-interval-ms]
   (RetriableConsumerRecordHandler. handler max-retry-times (Duration/ofMillis retry-interval-ms))))

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

(defn ^LcKafkaConsumer subscribe-to-topics
  ([^LcKafkaConsumer consumer topics]
   (.thenAccept (.subscribe consumer ^Collection topics)
                (reify Consumer
                  (accept [_ status]
                    (when (not= status UnsubscribedStatus/CLOSED)
                      (log/errorf "Consumer for topics: {} exit unexpectedly with status: {}" topics status)))))
   consumer)
  ([^LcKafkaConsumer consumer topics on-unsubscribe-callback]
   (.thenAccept (.subscribe consumer ^Collection topics)
                (reify Consumer
                  (accept [_ status]
                    (on-unsubscribe-callback status))))
   consumer))

(defn ^LcKafkaConsumer subscribe-to-pattern
  ([^LcKafkaConsumer consumer ^Pattern pattern]
   (.thenAccept (.subscribe consumer pattern)
                (reify Consumer
                  (accept [_ status]
                    (when (not= status UnsubscribedStatus/CLOSED)
                      (log/errorf "Consumer for pattern: {} exit unexpectedly with status: {}" pattern status)))))
   consumer)
  ([^LcKafkaConsumer consumer ^Pattern pattern on-unsubscribe-callback]
   (.thenAccept (.subscribe consumer pattern)
                (reify Consumer
                  (accept [_ status]
                    (on-unsubscribe-callback status))))
   consumer))

(defn close [^LcKafkaConsumer consumer]
  (.close consumer)
  consumer)