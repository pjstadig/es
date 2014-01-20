;;;; Copyright © 2013 Paul Stadig. All rights reserved.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this file,
;;;; You can obtain one at http://mozilla.org/MPL/2.0/.
;;;;
;;;; This Source Code Form is "Incompatible With Secondary Licenses", as defined
;;;; by the Mozilla Public License, v. 2.0.
(ns pjstadig.util
  (:require [clojure.core :as clj]
            [clojure.java.io :as io])
  (:import (java.util UUID)
           (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (org.apache.commons.codec.binary Base64)))

(defn uuid ^UUID []
  (UUID/randomUUID))

(def byte-array-class (Class/forName "[B"))

(defmulti ^bytes to-bytes type)
(defmethod to-bytes Byte [^Byte byte]
  (byte-array [byte]))
(defmethod to-bytes Short [^Short short]
  (-> (ByteBuffer/allocate 2)
      (.putShort short)
      .array))
(defmethod to-bytes Integer [^Integer int]
  (-> (ByteBuffer/allocate 4)
      (.putInt int)
      .array))
(defmethod to-bytes Long [^Long long]
  (-> (ByteBuffer/allocate 8)
      (.putLong long)
      .array))
(defmethod to-bytes UUID [^UUID uuid]
  (-> (ByteBuffer/allocate 16)
      (.putLong (.getMostSignificantBits uuid))
      (.putLong (.getLeastSignificantBits uuid))
      .array))
(defmethod to-bytes byte-array-class [bytes]
  bytes)

(defmulti ^String hex-str type)
(defmethod hex-str Byte [^Byte byte]
  (String/format "%02x" (object-array [byte])))
(defmethod hex-str byte-array-class [^bytes bytes]
  (apply str (map hex-str bytes)))
(defmethod hex-str UUID [^UUID uuid]
  (hex-str (to-bytes uuid)))
(defmethod hex-str :default [obj]
  (hex-str (to-bytes obj)))

(defmulti ^String base64-str type)
(defmethod base64-str UUID [^UUID uuid]
  (subs (base64-str (to-bytes uuid)) 0 22))
(defmethod base64-str :default [obj]
  (Base64/encodeBase64String (to-bytes obj)))

(defn base64url-str ^String [obj]
  (-> (base64-str obj)
      (.replaceAll "\\+" "-")
      (.replaceAll "/" "_")))

(defn uuid-url-str []
  (base64url-str (uuid)))

(defn uuid-hex-str []
  (hex-str (uuid)))
