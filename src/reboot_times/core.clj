(ns reboot-times.core
  (:require [cheshire.core :as json]
            clj-gcloud.coerce
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [tick.alpha.interval :as t.i]
            [tick.core :as t]))



(defn get-events-by-msg-substring
  [events-json substr]
  (filter #(string/includes? (:message %) substr) events-json))

(defn coallesce-times
  [events]
  (reduce (fn [joined e]
            (if (empty? joined)
              [e]
              (let [prev (last joined)]
                (case (t.i/relation prev e)
                  (:meets :overlaps) (conj (vec (butlast joined)) (t.i/new-interval (:tick/beginning prev) (:tick/end e)))
                  (:met-by :overlapped-by)  (conj (vec (butlast joined)) (t.i/new-interval (:tick/beginning e) (:tick/end prev)))
                  (conj joined e)))))
          []
          events))

(defn get-times-for-event-joined
  [events-json substr]
  (->> (get-events-by-msg-substring events-json substr)
       ;; remove events lasting 0 seconds (from and to are the same)
       (filter #(not (= (:from %) (:to %))))
       ;; transform json events into :from :to hashmaps
       (map #(t.i/new-interval (:from %) (:to %)))
       coallesce-times))

(defn durations
  [intervals]
  (reduce t/+ (map #(t/between (:tick/beginning %) (:tick/end %)) intervals)))

(defn get-job-id-from-filepath
  [filepath]
  (re-find #"[0-9]{19}" filepath))

(defn get-date-from-filename
  [filename]
  (let [date (re-find #"_([0-9]{8})-" filename)]
    (if (seq date)
      (t/date (str (subs (last date) 0 4) "-" (subs (last date) 4 6) "-" (subs (last date) 6 8)))
      nil)))

(defn create-jobid-date-map
  [files]
  (reduce
   (fn [dict file]
     (let
      [date (re-find #"_([0-9]{8})-" (.getName file))
       job-id (re-find #"[0-9]{19}" (.getPath file))]
       (if (seq date)
         (assoc dict (keyword job-id) (str (subs (last date) 0 4) "-" (subs (last date) 4 6) "-" (subs (last date) 6 8)))
         dict)))
   {}
   files))

(comment
  (def msg-reboot "rebooted and kubelet started")
  (def msg-img-reg-new "image-registry connection/new stopped")
  (def msg-img-reg-reused "image-registry connection/reused stopped")
  (def download-dir "/home/pm/work/full-events-test/")
  
  (def files (file-seq (io/file download-dir)))

  (def jobid-date-map (create-jobid-date-map files))

  (def k8s-events
    (->> files
         (filter #(string/ends-with? (.getName %) "events.json"))

         ;; load each file and create dict: {:job-id :date :json-data}
         (map (fn [file]
                (let
                 [filepath (.getPath file)
                  jobid (re-find #"[0-9]{19}" filepath)
                  date ((keyword jobid) jobid-date-map)]
                  {:job-id jobid
                   :date date
                   :json-data (:items (json/parse-string (slurp file) true))})))

         (map
          (fn [file]
            {:job-id (:job-id file)
             :date (:date file)
             :relevant-events (filter
                               (fn [event]
                                 (or (and (= "Reboot" (:reason event)) (= "machineconfigdaemon" (:component (:source event))))
                                     (and (= "Starting" (:reason event)) (= "kubelet" (:component (:source event))))))
                               (:json-data file))}))

         (map
          (fn [file]
            {:job-id (:job-id file)
             :date (:date file)
             :paired-events (reduce
                             (fn [events e]
                               (if (empty? events)
                                 [e]
                                 (if (and (= "Reboot" (:reason (last events)))
                                          (= "Starting" (:reason e)))
                                   (conj (drop-last events) {:msg "From reboot to kubelet starting"
                                                             :duration (t/seconds (t/between  (t/instant (:firstTimestamp (last events))) (t/instant (:firstTimestamp e))))})
                                   (conj events e))))
                             []
                             (:relevant-events file))}))

         ;; just take job-id and reboot-to-kubelet-starting duration
         (map
          (fn [file]
            {:job-id (:job-id file)
             :reboot-to-kubelet-starting-duration (:duration (first (:paired-events file)))}))))


  (def e2e-events
    (->> files
         (filter #(string/includes? (.getName %) "e2e-events"))

         ;; load each file and create dict: {:job-id :date :json-data}
         (map (fn [file]
                (let
                 [filepath (.getPath file)
                  jobid (re-find #"[0-9]{19}" filepath)
                  date ((keyword jobid) jobid-date-map)]
                  {:job-id jobid
                   :date date
                   :json-data (:items (json/parse-string (slurp file) true))})))

         ;; use json-data to create yet another dict - seqs with intervals of reboot and image-registry disruptions
         (map (fn [e]
                {:job-id (:job-id e)
                 :date (:date e)
                 :reboots (get-times-for-event-joined  (:json-data e) msg-reboot)
                 :img-reg-new (get-times-for-event-joined (:json-data e) msg-img-reg-new)
                 :img-reg-reused (get-times-for-event-joined (:json-data e) msg-img-reg-reused)}))

         ;; remove entries where there're no image-registry disruptions (there are actually two e2e-events files in each run, second one is filtered)
         (filter #(seq (:img-reg-new %)))

         ;; transform reboot and img-reg intervals into simple durations
         (map (fn [e]
                {:job-id (:job-id e)
                 :date (:date e)
                 :reboots-durations (t/seconds (durations (:reboots e)))
                 :img-reg-new-durations (t/seconds (durations (:img-reg-new e)))
                 :img-reg-reused-durations (t/seconds (durations (:img-reg-reused e)))}))))

  ;; create a seq of tick/date between first and last job
  (def dates (take-while
              #(t/< % (t/inc (t/date (:date (last e2e-events)))))
              (iterate t/inc (t/date (:date (first e2e-events))))))

  (->> dates

       ;; "zip" dates with jobs, if there's no job data for particular day it's still created to have better overview
       (map
        (fn [date]
          (let [matching-job (filter #(= (t/date (:date %)) date) e2e-events)]
            (if (seq matching-job)
              (first matching-job)
              {:job-id "0"
               :date date
               :reboots-durations nil
               :img-reg-new-durations nil
               :img-reg-reused-durations nil}))))

       ;; "zip" with k8s-events
       (map
        (fn [event]
          (let [matching-job (filter #(= (:job-id %) (:job-id event)) k8s-events)]
            (prn "---- matching job" matching-job)
            (if (seq matching-job)
              (assoc event :reboot-to-kubelet-starting-duration (:reboot-to-kubelet-starting-duration (first matching-job)))
              {:job-id "0"
               :date (:date event)
               :reboots-durations nil
               :img-reg-new-durations nil
               :img-reg-reused-durations nil
               :reboot-to-kubelet-starting-duration nil}))))

       ;; transform each dict into a vector
       (map (fn [e]
              [(:job-id e) (:date e) (:reboots-durations e) (:img-reg-new-durations e) (:img-reg-reused-durations e) (:reboot-to-kubelet-starting-duration e)]))

       ;; save [[vec-dict]] as a csv
       ;; double parens to call the anonymous function
       ((fn [entries]
          (with-open [writer (io/writer "out-file.csv")]
            (csv/write-csv writer [["jobId" "date" "rebootsDuration" "imgRegNewDurations" "imgRegReusedDurations" "rebootToKubeletStarting"]] :separator \; :quote? string?)
            (csv/write-csv writer entries :separator \; :quote? string?)))))
  )


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
