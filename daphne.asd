(asdf:defsystem "daphne"
  :description "Bounded Debug Adapter Protocol sessions"
  :author "Lambda Symbolics"
  :license "ISC"
  :version "0.1.0"
  :depends-on ("argo" "babel" "bordeaux-threads" "uiop")
  :serial t
  :components ((:file "package") (:file "protocol")
               (:file "session") (:file "process"))
  :in-order-to ((asdf:test-op (asdf:test-op "daphne/tests"))))

(asdf:defsystem "daphne/tests"
  :depends-on ("daphne")
  :components ((:file "tests"))
  :perform (asdf:test-op (operation system)
             (declare (ignore operation system))
             (uiop:symbol-call :daphne :run-tests)))
