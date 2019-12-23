(ns leancloud-kafka-client.producer-test
  (:require [clojure.test :refer :all])
  (:require [leancloud-kafka-client.producer :refer [producer]])
  (:import (org.apache.kafka.clients.producer KafkaProducer)))

(deftest create-producer
  (KafkaProducer. {"bootstrap.servers" "localhost:9092"})
  )
