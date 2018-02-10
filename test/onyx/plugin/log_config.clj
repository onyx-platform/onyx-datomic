(ns onyx.plugin.log-config
  (:require [taoensso.timbre :as log]))

(log/set-config! {:ns-blacklist ["org.apache.http.*" "org.eclipse.jetty.*"]})
