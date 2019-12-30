(ns kafka-clojure-client.admin
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata Consumer)
           (org.apache.kafka.common TopicPartition PartitionInfo)
           (org.apache.kafka.common.serialization StringDeserializer)
           (java.util Map)
           (java.io Closeable)))

(defn- get-partitions-for-topic [^Consumer consumer topic]
  (map #(.partition ^PartitionInfo %) (.partitionsFor consumer topic)))

(defn- get-committed-offset-for-partition [^Consumer consumer topic partition]
  (if-let [^OffsetAndMetadata o (.committed consumer (TopicPartition. topic partition))]
    (.offset o)
    0))

(defn- ^Map get-end-offsets-for-partitions [^Consumer consumer partitions]
  (try
    (.endOffsets consumer partitions)
    (catch Exception ex
      (log/warn ex "Get log end offset failed" partitions)
      (into {} (map #(vector % 0) partitions)))))

(defn- get-lags [^Consumer consumer topic]
  (when-let [partitions (not-empty (get-partitions-for-topic consumer topic))]
    (let [end-offsets (get-end-offsets-for-partitions consumer (map #(TopicPartition. topic %) partitions))]
      (apply sorted-map
             (interleave partitions
                         (map #(let [committed-offset (get-committed-offset-for-partition consumer topic %)
                                     log-end-offset   (get end-offsets (TopicPartition. topic %) 0)]
                                 (- log-end-offset committed-offset))
                              partitions))))))

(defprotocol ConsumerLagChecker
  (get-lags-for-topic [this topic]))

(deftype MonitorClient [consumer]
  ConsumerLagChecker
  (get-lags-for-topic [_ topic]
    (get-lags consumer topic))
  Closeable
  (close [_]
    (.close ^Consumer consumer)))

(defn create-monitor-client [bootstrap-servers group-id]
  (let [consumer (KafkaConsumer. ^Map {"bootstrap.servers"  bootstrap-servers
                                       "group.id"           group-id
                                       "enable.auto.commit" "false"
                                       "session.timeout.ms" "30000"
                                       "key.deserializer"   "StringDeserializer"
                                       "value.deserializer" "StringDeserializer"}
                                 (StringDeserializer.)
                                 (StringDeserializer.))]
    (MonitorClient. consumer)))

