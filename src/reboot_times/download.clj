(ns reboot-times.download
  (:require
   clj-gcloud.coerce
   [clj-gcloud.storage :as st]
   [clojure.java.io :as io]))

(def bucket "gs://origin-ci-test/")
(def bucket-root-dir "logs/")

(def anonymous-client (st/init {}))

(defn get-job-runs
  "Get filepaths to all runs for a job"
  [job-name]
  (->>
   (st/ls anonymous-client
          (str bucket bucket-root-dir job-name "/")
          {:current-directory true})
   (map #(let [{:keys [blob-id]} (clj-gcloud.coerce/->clj %)] (:name blob-id)))))

(defn get-filtered-filepaths-from-jobs
  "Returns a list of files within directories of given job-runs (seq of GCS uris)"
  ([job-runs]
   (get-filtered-filepaths-from-jobs (fn [_] true) job-runs))
  ([pred job-runs]
   (->> job-runs
        (map (fn [job-run]
               (->> (st/ls anonymous-client (str bucket job-run))
                    (map #(let [{:keys [blob-id]} (clj-gcloud.coerce/->clj %)] (:name blob-id))))))
        flatten
        (filter pred))))

(defn download-files
  "Downloads provided file-list to dest-dir into separate dir named after job id"
  [dest-dir file-list]
  (doall
   (map (fn [file]
          (let [job-id (re-find #"[0-9]{19}" file)
                local-dir (str dest-dir job-id)
                filename (.getName (io/file file))
                local-dest (str local-dir "/" filename)]
            (.mkdirs (io/file local-dir))
            (st/download-file-from-storage anonymous-client (str bucket file) local-dest)
            (println job-id "" filename)))
        file-list)))

(comment
  (def job-name "periodic-ci-openshift-release-master-ci-4.11-e2e-aws-upgrade-single-node")
  (def last-N-jobs 5)
  (def download-dir "/home/pm/work/full-events-test/")

  (->> (get-job-runs job-name)
       (take-last last-N-jobs)
       (get-filtered-filepaths-from-jobs
        #(or (re-matches #".*gather-extra/artifacts/events.json" %)
             (re-matches #".*\/e2e-events_.*.json" %)))
       (download-files download-dir))
  
  )
