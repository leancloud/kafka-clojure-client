(ns leancloud-kafka-client.producer
  (:refer-clojure :exclude [flush send])
  (:import (java.util.concurrent Future TimeUnit)
           (org.apache.kafka.clients.producer ProducerRecord KafkaProducer Producer Callback)
           (org.apache.kafka.common.serialization Serializer StringSerializer)
           (java.util Map)))

#_(defrecord KafkaBasedProducer [producer msg-encoder]
  Producer
  (enqueue [this queue-name msg]
    (enqueue this queue-name nil msg))
  (enqueue [this queue-name meta msg]
    (let [{:keys [partition key]} meta]
      (send producer (record queue-name partition key (msg-encoder msg)))))
  (close-producer [this]
    (.close ^KafkaProducer producer)))

#_(defn create-kafka-producer [producer-configs opts]
  (let [{:keys [key-serializer value-serializer msg-encoder]
         :or   {key-serializer   (StringSerializer.)
                value-serializer (StringSerializer.)
                msg-encoder      json/write-str}} opts]
    (KafkaBasedProducer. (producer producer-configs key-serializer value-serializer) msg-encoder)))

(defn ^Producer create-kafka-producer
  ([configs] (create-kafka-producer configs (StringSerializer.) (StringSerializer.)))
  ([^Map configs ^Serializer key-serializer ^Serializer value-serializer]
   {:pre [(contains? configs "bootstrap.servers")]}
   (KafkaProducer. configs key-serializer value-serializer)))

(defn record
  ([^String topic value]
   (ProducerRecord. topic value))
  ([^String topic key value]
   (ProducerRecord. topic key value))
  ([^String topic partition key value]
   (ProducerRecord. topic (int partition) key value))
  ([^String topic partition timestamp key value headers]
   (ProducerRecord. topic (int partition) ^long timestamp key value ^Iterable headers)))

(defn ^Future send
  ([producer record]
   (send producer record nil))
  ([^Producer producer ^ProducerRecord record call-back]
   (if call-back
     (if (instance? Callback call-back)
       (.send producer record call-back)
       (.send producer record (reify Callback
                                (onCompletion [_ metadata exception]
                                  (call-back metadata exception)))))
     (.send producer record nil))))

(defn partitions-for-topic [producer topic]
  (.partitionsFor ^Producer producer topic))

(defn metrics [producer]
  (.metrics ^Producer producer))

(defn flush [^Producer producer]
  (.flush producer))

(defn close
  ([producer] (.close ^Producer producer))
  ([producer timeout time-unit] (.close ^Producer producer timeout ^TimeUnit time-unit)))