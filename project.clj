(defproject reboot-times "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 ;[cljc.java-time "0.1.18"]
                 [cheshire "5.10.2"]
                 [tick "0.5.0-RC5"]
                 ;[hickory "0.7.1"]
                 [com.oscaro/clj-gcloud-storage "0.172-1.0"]
                 [org.clojure/data.csv "1.0.1"]]
  :repl-options {:init-ns reboot-times.core})
