(defproject cn.leancloud/kafka-clojure-client "0.0.1"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev     {:dependencies [[junit/junit "4.12"]
                                      [org.apache.logging.log4j/log4j-core "2.12.1"]
                                      [org.apache.logging.log4j/log4j-api "2.12.1"]
                                      [org.apache.logging.log4j/log4j-slf4j-impl "2.12.1"]
                                      [org.assertj/assertj-core "3.13.2"]
                                      [org.awaitility/awaitility "4.0.1"]
                                      [org.mockito/mockito-core "3.0.0"]]}}
  :source-paths ["src/clj"]
  :test-paths ["test/clj" "test/java"]
  :java-source-paths ["src/java"]
  :jvm-opts ["-Dclojure.compiler.elide-meta='[:doc :added]'"]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.apache.kafka/kafka-clients "1.1.1"]
                 [com.google.code.findbugs/jsr305 "3.0.2"]]
  :repositories [["github" {:url   "https://maven.pkg.github.com/leancloud/kafka-clojure-client"
                            :creds :gpg}]]
  )