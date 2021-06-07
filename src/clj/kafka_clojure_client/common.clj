(ns kafka-clojure-client.common
  (:require [clojure.data.json :as json])
  (:import (org.apache.kafka.common.serialization StringSerializer Serializer Deserializer StringDeserializer)))


(defn json-serializer []
  (let [str-serializer (StringSerializer.)]
    (reify Serializer
      (configure [_ configs key?]
        (.configure str-serializer configs key?))
      (serialize [_ topic data]
        (.serialize str-serializer topic (json/write-str data)))
      (close [_]
        (.close str-serializer)))))

(defn json-deserializer []
  (let [str-deserializer (StringDeserializer.)]
    (reify Deserializer
      (configure [_ configs key?]
        (.configure str-deserializer configs key?))
      (deserialize [_ topic data]
        (json/read-str (.deserialize str-deserializer topic data) :key-fn keyword))
      (close [_]
        (.close str-deserializer)))))

