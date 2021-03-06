(ns kafka-clojure-client.producer
  (:refer-clojure :exclude [flush send])
  (:import (java.util.concurrent Future TimeUnit)
           (org.apache.kafka.clients.producer ProducerRecord KafkaProducer Producer Callback)
           (org.apache.kafka.common.serialization Serializer)
           (java.util Map List)
           (org.apache.kafka.common.header Header)))

(defn ^Producer create-kafka-producer
  "Create a Kafka producer with configurations and serializers"
  ([configs]
   {:pre [(contains? configs "bootstrap.servers")]}
   (KafkaProducer. configs))
  ([^Map configs ^Serializer key-serializer ^Serializer value-serializer]
   {:pre [(contains? configs "bootstrap.servers")]}
   (KafkaProducer. configs key-serializer value-serializer)))

(defn- ^Header header
  ([[k v]] (header k v))
  ([k v]
   (reify Header
     (key [_] k)
     (value [_] v))))

(defn ^ProducerRecord record
  "Convert a map to ProducerRecord."
  [record]
  (ProducerRecord. (:topic record)
                   (some-> (:partition record)
                           int)
                   (:timestamp record)
                   (:key record)
                   (:value record)
                   (map header (:headers record))))

(defn ^Future send-record
  "Send a ProducerRecord with a producer."
  ([^Producer producer ^ProducerRecord record]
   (send-record producer record nil))
  ([^Producer producer ^ProducerRecord record call-back]
   (if call-back
     (.send producer record (reify Callback
                              (onCompletion [_ metadata exception]
                                (call-back metadata exception))))
     (.send producer record nil))))

(defn ^Future send
  "Send a Map which can be converted to ProducerRecord by calling function \"record\" with a producer."
  ([producer record-map]
   (send-record producer (record record-map) nil))
  ([^Producer producer record-map call-back]
   (send-record producer (record record-map) call-back)))

(defn ^List partitions-for-topic [producer topic]
  (.partitionsFor ^Producer producer topic))

(defn ^Map metrics [producer]
  (.metrics ^Producer producer))

(defn ^Producer flush
  [^Producer producer]
  (.flush producer)
  producer)

(defn close
  "Close a Kafka producer"
  ([producer]
   (.close ^Producer producer)
   nil)

  ([producer timeout time-unit]
   (.close ^Producer producer timeout ^TimeUnit time-unit)
   nil))