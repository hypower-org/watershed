(defproject watershed "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [lamina "0.5.5-SNAPSHOT"]     
                 [incanter "1.5.5"]
                 [net.mikera/core.matrix "0.29.1"]
                 [manifold "0.1.0-beta3"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [criterium "0.4.3"]
                 [jkobuki "1.1.0"]
                 [seesaw "1.4.4"]
                 [org.scream3r/jssc "2.8.0"]
                 [com.taoensso/nippy "2.7.0"]
                 [org.clojure/core.match "0.2.1"]
                 [byte-streams "0.2.0-alpha3"]
                 [aleph "0.4.0-alpha9"]]
  :plugins [[lein-nodisassemble "0.1.3"]]
  :main networking.kobuki-client
  :aot [networking.kobuki-client])
