(ns kafka-clojure-client.consumer
  (:require [clojure.tools.logging :as log])
  (:import (cn.leancloud.kafka.consumer LcKafkaConsumerBuilder LcKafkaConsumer
                                        CatchAllExceptionConsumerRecordHandler RetriableConsumerRecordHandler
                                        ConsumerRecordHandler UnsubscribedStatus)
           (java.util.function BiConsumer Consumer)
           (java.util Collection Map)
           (java.util.regex Pattern)
           (java.time Duration)))

(defn- ^LcKafkaConsumerBuilder create-builder
  "Allowed opts are:
  :poll-timeout-ms
  The pollTimeout is the maximum time spent waiting in polling data from kafka broker if data is not available in
  the buffer.
  If 0, poll operation will return immediately with any records that are available currently in the buffer,
  else returns empty.
  Must not be negative. And the default value is 100.

  :worker-pool
  The thread pool used by consumer to handle the consumed records from Kafka broker. If no worker pool is provided,
  the created LcKafkaConsumer will use ImmediateExecutorService to handle records in the records polling thread instead.
  When a worker pool is provided, after each poll, the polling thread will take one thread from this worker pool
  for each polled org.apache.kafka.clients.consumer.ConsumerRecord to handle the record. Please tune
  the \"max.poll.records\" in kafka configs to limit the number of records polled at each time do not
  exceed the max size of the provided worker thread pool. Otherwise, a java.util.concurrent.RejectedExecutionException
  will thrown when the polling thread submitting too much tasks to the pool. Then this exception will lead the only
  polling thread to exit.
  If you are using partial sync/async commit consumer by building LcKafkaConsumer with \"create-partial-sync-commit-consumer\"
  or \"create-partial-async-commit-consumer\", without a worker pool, they degrade to sync/async commit consumer as
  built with \"create-sync-commit-consumer\" or \"create-async-commit-consumer\".
  If no worker pool provided, you also need to tune \"max.poll.interval.ms\" in kafka configs, to ensure the
  polling thread can at least poll once within \"max.poll.interval.ms\" during handling consumed messages
  to prevent itself from session timeout or polling timeout.

  :shutdown-worker-pool-on-stop
  when :worker-pool is provided, please set this configuration to true to shutdown the input worker pool when
  this consumer is closed

  :graceful-shutdown-timeout-ms
  Sets the amount of time to wait after calling \"close\" for consumed records to handle before actually shutting down.
  The default value is 10_000.

  :recommit-interval-ms
  The interval to commit all partitions and it's completed offsets to broker on a non-automatic commit consumer.
  This configuration is only valid and is required on a non-automatic commit consumer build with
  \"create-sync-commit-consumer\", \"create-async-commit-consumer\", \"create-partial-sync-commit-consumer\"
  or \"create-partial-async-commit-consumer\".
  For these kind of consumers, usually they only commit offsets of a partition when there was records consumed from
  that partition and all these consumed records was handled successfully. But we must periodically commit those
  subscribed partitions who have had records but no new records for a long time too. Otherwise, after commit offset
  retention timeout, Kafka broker may forget where the current commit offset of these partition for the consumer
  are. Then, when the consumer crashed and recovered, if the consumer set \"auto.offset.reset\"
  configuration to \"earliest\", it may consume a already consumed record again. So please make sure
  that :recommit-interval-ms is within the limit set by \"offsets.retention.minutes\"
  on Kafka broker or even within 1/3 of that limit to tolerate some commit failures on async commit consumer.
  The default value is 1 hour.

  :max-pending-async-commits
  When using async consumer to commit offset asynchronously, this argument can force consumer to do a synchronous
  commit after there's already this (:max-pending-async-commits) many async commits on the fly without
  response from broker.
  The default value is 10.

  :handle-record-timeout-ms
  The maximum time spent in handling a single org.apache.kafka.clients.consumer.ConsumerRecord. If the handling
  time for any ConsumerRecord exceeds this limit, a java.util.concurrent.TimeoutException will be thrown which
  then will drag the LcKafkaConsumer to shutdown. This mechanism is to prevent the \"livelock\" situation where it
  seems the LcKafkaConsumer is OK, continuing on sending heartbeat and calling Consumer#poll(long),
  but no progress is being made.
  The default value is zero which means no limit on handling a ConsumerRecord.

  :sync-commit-retry-interval-ms
  Sets the amount of time in milli seconds to wait before retry a failed synchronous commit on calling KafkaConsumer#commitSync().
  or KafkaConsumer#commitSync(Map). Every synchronous commit may fail but most of times they are caused by
  org.apache.kafka.common.errors.RetriableException and we can retry commit on this kind of exception safely.
  This configuration set the interval between each retry.
  For those failures of asynchronous commit by calling KafkaConsumer#commitAsync() or
  KafkaConsumer#commitAsync(OffsetCommitCallback), we retry them by a synchronous commit automatically
  when we found any of them. So we only need configurations for synchronous commits.
  The default value is 1000.

  :max-attempts-for-each-sync-commit
  Sets the maximum attempt times for a synchronous commit by calling KafkaConsumer#commitSync().
  or KafkaConsumer#commitSync(Map). Every synchronous commit may fail but most of times they are caused by
  org.apache.kafka.common.errors.RetriableException and we can retry commit on this kind of exception safely.
  This configuration cap the maximum retry times. If attempts reach to `maxAttemptsForEachSyncCommit`, the cached
  org.apache.kafka.common.errors.RetriableException will be rethrown by then it will cause the Kafka Consumer
  to stop and quit.
  For those failures of asynchronous commit by calling KafkaConsumer#commitAsync() or
  KafkaConsumer#commitAsync(OffsetCommitCallback), we retry them by a synchronous commit automatically
  when we found any of them. So we only need configurations for synchronous commits.
  Please note that `maxAttemptsForEachSyncCommit multiplies `syncCommitRetryInterval` should far lower than
  `max.poll.interval.ms`, otherwise Kafka Consumer may encounter session timeout or polling timeout due to not
  calling KafkaConsumer#poll(long) for too long.
  The default value is 3.

  :key-deserializer
  the deserializer for key that implements org.apache.kafka.common.serialization.Deserializer

  :value-deserializer
  the deserializer for value that implements org.apache.kafka.common.serialization.Deserializer"
  [^Map configs ^ConsumerRecordHandler msg-handler
   {:keys [poll-timeout-ms
           worker-pool
           graceful-shutdown-timeout-ms
           recommit-interval-ms
           shutdown-worker-pool-on-stop
           max-pending-async-commits
           handle-record-timeout-ms
           sync-commit-retry-interval-ms
           max-attempts-for-each-sync-commit
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
    (when sync-commit-retry-interval-ms
      (.syncCommitRetryIntervalMillis builder (long sync-commit-retry-interval-ms)))
    (when max-attempts-for-each-sync-commit
      (.maxAttemptsForEachSyncCommit builder (int max-attempts-for-each-sync-commit)))
    (when worker-pool
      (.workerPool builder worker-pool (or shutdown-worker-pool-on-stop false)))
    (.maxPendingAsyncCommits builder (int max-pending-async-commits))
    builder))

(defn ^ConsumerRecordHandler catch-all-exception-record-handler
  "Create a CatchAllExceptionConsumerRecordHandler instance which is a wrapper over
   another ConsumerRecordHandler instance to catch and swallow all the exceptions
   thrown from the wrapped ConsumerRecordHandler instance when it failed to handle
   a consumed record.

   This handler seems good to improve the availability of the consumer because it can
   swallow all the exceptions on handling a record and carry on to handle next record.
   But it actually can compromise the consumer to prevent a livelock, where the application
   did not crash but fails to make progress for some reason.

   Please use it judiciously. Usually fail fast, let the polling thread exit on exception,
   is your best choice."
  ([handler] (CatchAllExceptionConsumerRecordHandler. handler))
  ([handler error-consumer]
   (CatchAllExceptionConsumerRecordHandler. handler (reify BiConsumer
                                                      (accept [_ record throwable]
                                                        (error-consumer record throwable))))))

(defn ^ConsumerRecordHandler retriable-record-handler
  "Create a RetriableConsumerRecordHandler instance which is a wrapper over another
   ConsumerRecordHandler instance to let the wrapped ConsumerRecordHandler instance
   try to handle a record in a limited times in case the handling process failed."
  ([handler max-retry-times]
   (RetriableConsumerRecordHandler. handler max-retry-times))
  ([handler max-retry-times retry-interval-ms]
   (RetriableConsumerRecordHandler. handler max-retry-times (Duration/ofMillis retry-interval-ms))))

(defn to-record-handler
  "Convert a single argument clojure function which used to handle the consumed
   Kafka record to a ConsumerRecordHandler instance."
  [handler-fn]
  (reify ConsumerRecordHandler
    (handleRecord [_ record]
      (handler-fn record))))

(defn to-value-only-record-handler
  "Convert a single argument clojure function which used to handle only the value
   of the consumed Kafka record to a ConsumerRecordHandler instance."
  [handler-fn]
  (reify ConsumerRecordHandler
    (handleRecord [_ record]
      (handler-fn (.value record)))))

(defn ^LcKafkaConsumer create-sync-commit-consumer
  "Build a consumer in which the polling thread always does a sync commit after
   all the polled records has been handled.

   Because it only commits after all the polled records handled, so the longer
   the records handling process，the longer the interval between each commits,
   the bigger of the possibility to repeatedly consume a same record when the
   consumer crash.

   This kind of consumer ensures to do a sync commit to commit all the finished
   records at that time when the consumer is shutdown or any partition was revoked.
   It requires the following kafka configs must be set, otherwise an IllegalArgumentException
   will be thrown:
   * max.poll.records
   * auto.offset.reset

   Though all of these configs have default values in kafka, we still require
   every user to set them specifically. Because these configs is vital for using
   this consumer safely.

   If you set \"enable.auto.commit\" to true, this consumer will set it to
   false by itself.

   Please refer to function \"create-builder\" to check allowed opts."
  [^Map kafka-configs ^ConsumerRecordHandler msg-handler & opts]
  (.buildSync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-partial-sync-commit-consumer
  "Build a consumer in which the polling thread does a sync commits whenever there's any handled consumer records. It
   commits often, so after a consumer crash, comparatively little records may be handled more than once. But also
   due to commit often, the overhead causing by committing is relatively high.

   This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
   consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
   otherwise an IllegalArgumentException will be thrown:
    * max.poll.records
    * auto.offset.reset

   Though all of these configs have default values in kafka, we still require every user to set them specifically.
   Because these configs is vital for using this consumer safely.

   If you set \"enable.auto.commit\" to true, this consumer will set it to false by itself.

   Please refer to function \"create-builder\" to check allowed opts."
  [^Map kafka-configs ^ConsumerRecordHandler msg-handler & opts]
  (.buildPartialSync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-async-commit-consumer
  "Build a consumer in which the polling thread always does a async commit after all the polled records has been handled.
   Because it only commits after all the polled records handled, so the longer the records handling process，
   the longer the interval between each commits, the bigger of the possibility to repeatedly consume a same record
   when the consumer crash.

   If any async commit is failed or the number of pending async commits is beyond the limit set by
   LcKafkaConsumerBuilder#maxPendingAsyncCommits(int), this consumer will do a sync commit to commit all the
   records which have been handled.

   This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
   consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
   otherwise an IllegalArgumentException will be thrown:
    * max.poll.records
    * auto.offset.reset

   Though all of these configs have default values in kafka, we still require every user to set them specifically.
   Because these configs is vital for using this consumer safely.

   If you set \"enable.auto.commit\" to true, this consumer will set it to false by itself.

   Please refer to function \"create-builder\" to check allowed opts."
  [^Map kafka-configs ^ConsumerRecordHandler msg-handler & opts]
  (.buildAsync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-partial-async-commit-consumer
  "* Build a consumer in which the polling thread does a async commits whenever there's any handled consumer records. It
   commits often, so after a consumer crash, comparatively little records may be handled more than once. It use
   async commit to mitigate the overhead causing by high committing times.

   If any async commit is failed or the number of pending async commits is beyond the limit set by
   LcKafkaConsumerBuilder#maxPendingAsyncCommits(int), this consumer will do a sync commit to commit all the
   records which have been handled.

   This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
   consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
   otherwise an IllegalArgumentException will be thrown:
    * max.poll.records
    * auto.offset.reset

   Though all of these configs have default values in kafka, we still require every user to set them specifically.
   Because these configs is vital for using this consumer safely.

   If you set \"enable.auto.commit\" to true, this consumer will set it to false by itself.

   Please refer to function \"create-builder\" to check allowed opts."
  [^Map kafka-configs ^ConsumerRecordHandler msg-handler & opts]
  (.buildPartialAsync (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer create-auto-commit-consumer
  "Build a consumer which commits offset automatically at fixed interval. It is both OK for with or without a
   worker thread pool. But without a worker pool, please tune the `max.poll.interval.ms` in
   Kafka configs as mentioned in LcKafkaConsumerBuilder#workerPool(ExecutorService, boolean).
   This kind of consumer requires the following kafka configs must be set, otherwise
   IllegalArgumentException will be thrown:
    * max.poll.interval.ms</code></li>
    * max.poll.records</code></li>
    * auto.offset.reset</code></li>
    * auto.commit.interval.ms</code></li>

   Though all of these configs have default values in kafka, we still require every user to set them specifically.
   Because these configs is vital for using this consumer safely.

   If you set \"enable.auto.commit\" to true, this consumer will set it to false by itself.

   Please refer to function \"create-builder\" to check allowed opts."
  [^Map kafka-configs ^ConsumerRecordHandler msg-handler & opts]
  (.buildAuto (create-builder kafka-configs msg-handler opts)))

(defn ^LcKafkaConsumer subscribe-to-topics
  "Subscribe some Kafka topics to consume records from them."
  ([^LcKafkaConsumer consumer topics]
   (.thenAccept (.subscribe consumer ^Collection topics)
                (reify Consumer
                  (accept [_ status]
                    (when (not= status UnsubscribedStatus/CLOSED)
                      (log/errorf "Consumer for topics: %s exit unexpectedly with status: %s" topics status)))))
   consumer)
  ([^LcKafkaConsumer consumer topics on-unsubscribe-callback]
   (.thenAccept (.subscribe consumer ^Collection topics)
                (reify Consumer
                  (accept [_ status]
                    (on-unsubscribe-callback status))))
   consumer))

(defn ^LcKafkaConsumer subscribe-to-pattern
  "Subscribe to all topics matching specified pattern to get dynamically assigned partitions
   The pattern matching will be done periodically against all topics existing at the time of check.
   This can be controlled through the `metadata.max.age.ms` configuration: by lowering
   the max metadata age, the consumer will refresh metadata more often and check for matching topics."
  ([^LcKafkaConsumer consumer ^Pattern pattern]
   (.thenAccept (.subscribe consumer pattern)
                (reify Consumer
                  (accept [_ status]
                    (when (not= status UnsubscribedStatus/CLOSED)
                      (log/errorf "Consumer for pattern: %s exit unexpectedly with status: %s" pattern status)))))
   consumer)
  ([^LcKafkaConsumer consumer ^Pattern pattern on-unsubscribe-callback]
   (.thenAccept (.subscribe consumer pattern)
                (reify Consumer
                  (accept [_ status]
                    (on-unsubscribe-callback status))))
   consumer))

(defn close
  "Close a LcKafkaConsumer."
  [^LcKafkaConsumer consumer]
  (.close consumer)
  consumer)