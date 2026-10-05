(in-package #:daphne)

(defstruct pending command response id)

(defclass session ()
  ((transport :initarg :transport :reader session-transport)
   (lock :initform (bt:make-lock "DAP session") :reader session-lock)
   (write-lock :initform (bt:make-lock "DAP writer") :reader session-write-lock)
   (close-lock :initform (bt:make-lock "DAP cleanup") :reader session-close-lock)
   (state :initform :new :accessor session-state)
   (failure :initform nil :accessor session-failure)
   (capabilities :initform nil :accessor session-capabilities)
   (sequence :initform 0 :accessor session-sequence)
   (pending :initform (make-hash-table) :reader session-pending)
   (events :initform nil :accessor session-event-queue)
   (event-count :initform 0 :accessor session-event-count)
   (max-events :initarg :max-events :reader session-max-events)
   (max-pending :initarg :max-pending :reader session-max-pending)
   (reader :initform nil :accessor session-reader))
  (:documentation "One adapter connection with correlated requests and bounded event storage."))

(defun session-close (session &optional failure)
  "Idempotently close SESSION and its transport, failing all outstanding requests.
Return only after transport cleanup completes, including concurrent close calls.
Adapter termination events set :TERMINATED but permit disconnect/cleanup."
  (bt:with-lock-held ((session-close-lock session))
    (let ((close-p nil))
      (bt:with-lock-held ((session-lock session))
        (unless (member (session-state session) '(:closed :failed))
          (setf (session-state session) (if failure :failed :closed)
                (session-failure session) failure
                close-p t)))
      (when close-p (transport-close (session-transport session)))))
  nil)

(defun session--send (session message &optional entry)
  "Serialize writes and assign sequences in wire order. ENTRY reserves a request."
  (handler-case
      (bt:with-lock-held ((session-write-lock session))
        (bt:with-lock-held ((session-lock session))
          (when (member (session-state session) '(:closed :failed))
            (error 'dap-state-error :message "DAP session is closed."))
          (let ((id (incf (session-sequence session))))
            (setf (gethash "seq" message) id)
            (when entry
              (remhash entry (session-pending session))
              (setf (pending-id entry) id
                    (gethash id (session-pending session)) entry))))
        (transport-write (session-transport session) message))
    (error (cause)
      (let ((failure (if (typep cause 'dap-error) cause
                         (make-condition 'dap-transport-error
                                         :message "DAP write failed." :cause cause))))
        (session-close session failure)
        (error failure)))))

(defun session--receive (session message)
  "Validate correlation and update events and lifecycle state."
  (let ((type (json-get message "type")) (seq (json-get message "seq")))
    (unless (and (integerp seq) (plusp seq) (stringp type))
      (protocol-error "DAP message requires a positive seq and string type."))
    (cond
      ((string= type "response")
       (bt:with-lock-held ((session-lock session))
         (when (member (session-state session) '(:closed :failed))
           (return-from session--receive nil))
         (let* ((id (json-get message "request_seq"))
                (entry (and (integerp id) (gethash id (session-pending session)))))
           (unless (and entry (not (pending-response entry))
                        (equal (pending-command entry) (json-get message "command"))
                        (or (eq t (gethash "success" message))
                            (argo:json-false-p (gethash "success" message))))
             (protocol-error "Uncorrelated, duplicate or malformed DAP response."))
           (setf (pending-response entry) message))))
      ((string= type "event")
       (unless (stringp (json-get message "event"))
         (protocol-error "DAP event requires an event name."))
       (bt:with-lock-held ((session-lock session))
         (when (member (session-state session) '(:closed :failed))
           (return-from session--receive nil))
         (when (>= (session-event-count session) (session-max-events session))
           (error 'dap-limit-error :message "DAP event queue exceeds its bound."))
         (setf (session-event-queue session)
               (nconc (session-event-queue session) (list message)))
         (incf (session-event-count session))
         (let ((event (json-get message "event")))
           (cond ((string= event "stopped") (setf (session-state session) :stopped))
                 ((string= event "continued") (setf (session-state session) :running))
                 ((string= event "terminated") (setf (session-state session) :terminated))))))
      ((string= type "request")
       ;; Reverse requests need application-specific authority. Reject explicitly.
       (unless (stringp (json-get message "command"))
         (protocol-error "Adapter request requires a command."))
       (session--send session
                      (json-object "type" "response" "request_seq" seq
                                   "command" (json-get message "command")
                                   "success" (json-boolean nil)
                                   "message" "Client does not support reverse requests.")))
      (t (protocol-error "Unknown DAP message type ~S." type)))))

(defun make-session (transport &key (max-events 1024) (max-pending 64))
  "Start a reader for owned TRANSPORT. Bounds must be positive integers."
  (unless (and (typep max-events '(integer 1)) (typep max-pending '(integer 1)))
    (error 'dap-limit-error :message "Session limits must be positive integers."))
  (let ((session (make-instance 'session :transport transport
                               :max-events max-events :max-pending max-pending)))
    (setf (session-reader session)
          (bt:make-thread
           (lambda ()
             (handler-case
                 (loop for message = (transport-read transport)
                       do (unless message
                            (error 'dap-transport-error :message "Adapter closed its output."))
                          (session--receive session message))
               (error (cause)
                 (session-close session
                                (if (typep cause 'dap-error) cause
                                    (make-condition 'dap-transport-error
                                                    :message "DAP reader failed." :cause cause))))))
           :name "DAP reader"))
    session))

(defun session-events (session &key name)
  "Atomically drain queued events in arrival order, optionally only those named NAME."
  (bt:with-lock-held ((session-lock session))
    (if name
        (let ((events (remove-if-not (lambda (event) (equal name (json-get event "event")))
                                     (session-event-queue session))))
          (setf (session-event-queue session)
                (remove-if (lambda (event) (equal name (json-get event "event")))
                           (session-event-queue session)))
          (decf (session-event-count session) (length events))
          events)
        (prog1 (session-event-queue session)
          (setf (session-event-queue session) nil (session-event-count session) 0)))))

(defun deadline (timeout)
  "Return a monotonic deadline for a positive finite timeout in seconds."
  (unless (and (realp timeout) (plusp timeout) (<= timeout 86400))
    (error 'dap-limit-error :message "Timeout must be positive and at most one day."))
  (+ (get-internal-real-time) (* timeout internal-time-units-per-second)))

(defun session--check (session end cancel-p &key pending)
  "Check deadlines and connection failure; retain a completed response across later EOF."
  (let ((failure (cond ((and cancel-p (funcall cancel-p))
                       (make-condition 'dap-cancelled :message "DAP operation cancelled."))
                      ((>= (get-internal-real-time) end)
                       (make-condition 'dap-timeout :message "DAP operation timed out.")))))
    (when failure (session-close session failure) (error failure)))
  (let ((failure nil) (closed-p nil) (completed-p nil))
    (bt:with-lock-held ((session-lock session))
      (when (member (session-state session) '(:closed :failed))
        (setf closed-p t
              completed-p (and pending (pending-response pending))
              failure (or (session-failure session)
                          (make-condition 'dap-state-error :message "DAP session is closed.")))))
    (when closed-p
      (session-close session)
      (unless completed-p (error failure)))))

(defun session-request (session command arguments &key (timeout 10) cancel-p on-event event-name)
  "Send COMMAND and JSON object ARGUMENTS; return body and full response.
Concurrent requests correlate by request_seq. ON-EVENT consumes queued events
in the caller thread and may make nested requests; EVENT-NAME restricts this
to one event name. Timeout/cancellation closes the connection, including when
the adapter stops reading. Caller callbacks must be bounded; keep arguments
immutable after submission."
  (unless (and (stringp command) (argo:json-object-p arguments))
    (error 'dap-protocol-error :message "Request needs a command string and JSON object arguments."))
  (let* ((end (deadline timeout)) (entry (make-pending :command command)))
    (session--check session end cancel-p)
    (bt:with-lock-held ((session-lock session))
      (when (>= (hash-table-count (session-pending session)) (session-max-pending session))
        (error 'dap-limit-error :message "Too many pending DAP requests."))
      (setf (gethash entry (session-pending session)) entry))
    (unwind-protect
         (handler-case
             (progn
               (bt:make-thread
                (lambda ()
                  (handler-case
                      (session--send session (json-object "type" "request" "command" command
                                                          "arguments" arguments) entry)
                    (error () nil)))
                :name "DAP request writer")
               (loop
                 (session--check session end cancel-p :pending entry)
                 (when on-event
                   (dolist (event (session-events session :name event-name)) (funcall on-event event)))
                 (let ((response (bt:with-lock-held ((session-lock session))
                                   (pending-response entry))))
                   (when response
                     (unless (json-true-p (gethash "success" response))
                       (error 'dap-request-error :response response
                              :message (or (json-get response "message") "Adapter rejected request.")))
                     (return (values (json-get response "body") response))))
                 (sleep 0.005)))
           (dap-request-error (failure) (error failure))
           (error (failure) (session-close session failure) (error failure)))
      (bt:with-lock-held ((session-lock session))
        (remhash entry (session-pending session))
        (when (pending-id entry) (remhash (pending-id entry) (session-pending session)))))))

(defun session-wait-event (session name &key (timeout 10) cancel-p)
  "Remove and return the first event named NAME; preserve other queued events."
  (let ((end (deadline timeout)))
    (loop
      (session--check session end cancel-p)
      (let ((event
              (bt:with-lock-held ((session-lock session))
                (let ((event (find name (session-event-queue session)
                                   :key (lambda (value) (json-get value "event")) :test #'equal)))
                  (when event
                    (setf (session-event-queue session) (delete event (session-event-queue session) :count 1))
                    (decf (session-event-count session)))
                  event))))
        (when event (return event)))
      (sleep 0.005))))

(defun session-initialize (session &key (arguments (json-object "adapterID" "daphne"
                                                              "linesStartAt1" t "columnsStartAt1" t
                                                              "pathFormat" "path"))
                                   (timeout 10) cancel-p)
  "Initialize once and return adapter capabilities. No reverse requests are advertised."
  (bt:with-lock-held ((session-lock session))
    (unless (eq (session-state session) :new)
      (error 'dap-state-error :message "DAP initialize requires a new session."))
    (setf (session-state session) :initializing))
  (handler-case
      (let ((capabilities (session-request session "initialize" arguments :timeout timeout :cancel-p cancel-p)))
        (bt:with-lock-held ((session-lock session))
          (setf (session-capabilities session) capabilities (session-state session) :initialized))
        capabilities)
    (error (failure) (session-close session failure) (error failure))))

(defun session-configuration-done (session &key (timeout 10) cancel-p)
  "Complete adapter configuration when its capabilities support configurationDone."
  (when (and (session-capabilities session)
             (json-get (session-capabilities session) "supportsConfigurationDoneRequest"))
    (session-request session "configurationDone" (json-object) :timeout timeout :cancel-p cancel-p)))

(defun session-start (session mode arguments &key configure (timeout 10) cancel-p)
  "Launch or attach, handling initialized/configurationDone before a delayed response.
MODE is :LAUNCH or :ATTACH. CONFIGURE is called with SESSION in the waiting
caller thread after initialized, to set breakpoints and other configuration."
  (unless (member mode '(:launch :attach))
    (error 'dap-state-error :message "Start mode must be :launch or :attach."))
  (bt:with-lock-held ((session-lock session))
    (unless (eq (session-state session) :initialized)
      (error 'dap-state-error :message "Start requires initialized session."))
    (setf (session-state session) :configuring))
  (let ((configured nil) (end (deadline timeout)))
    (labels ((remaining () (max 0.001 (/ (- end (get-internal-real-time)) internal-time-units-per-second)))
             (configure-event (event)
               (when (and (not configured) (equal (json-get event "event") "initialized"))
                 (when configure (funcall configure session))
                 (session-configuration-done session :timeout (remaining) :cancel-p cancel-p)
                 (setf configured t))))
      (handler-case
          (let ((body (session-request session (ecase mode (:launch "launch") (:attach "attach"))
                                       arguments :timeout (remaining) :cancel-p cancel-p
                                       :on-event #'configure-event :event-name "initialized")))
            (unless configured
              (configure-event (session-wait-event session "initialized" :timeout (remaining) :cancel-p cancel-p)))
            (bt:with-lock-held ((session-lock session))
              (when (eq (session-state session) :configuring) (setf (session-state session) :running)))
            body)
        (error (failure) (session-close session failure) (error failure))))))

(defun session--require-state (session states)
  "Reject semantic operations outside STATES, preserving existing connection errors."
  (session--check session (deadline 1) nil)
  (bt:with-lock-held ((session-lock session))
    (unless (member (session-state session) states)
      (error 'dap-state-error :message (format nil "Operation is invalid in DAP state ~S." (session-state session))))))

(defun session--identifier (value)
  "Validate a DAP integer identifier."
  (unless (typep value '(integer 0))
    (error 'dap-protocol-error :message "DAP identifier must be a nonnegative integer.")))

(defun session--page (start count)
  "Validate finite semantic page bounds."
  (unless (and (typep start '(integer 0)) (typep count '(integer 1 10000)))
    (error 'dap-limit-error :message "Page start must be nonnegative and count must be 1..10000.")))

(defun session-set-breakpoints (session source breakpoints &key (timeout 10) cancel-p)
  "Replace SOURCE breakpoints (a vector of DAP breakpoint objects)."
  (session--require-state session '(:initialized :configuring :running :stopped))
  (unless (and (argo:json-object-p source) (vectorp breakpoints) (not (stringp breakpoints)))
    (error 'dap-protocol-error :message "Breakpoints require a source object and breakpoint vector."))
  (session-request session "setBreakpoints" (json-object "source" source "breakpoints" breakpoints)
                   :timeout timeout :cancel-p cancel-p))

(defun session-continue (session thread-id &key (timeout 10) cancel-p)
  "Continue a stopped thread."
  (session--require-state session '(:stopped))
  (session--identifier thread-id)
  (session-request session "continue" (json-object "threadId" thread-id) :timeout timeout :cancel-p cancel-p))

(defun session-pause (session thread-id &key (timeout 10) cancel-p)
  "Pause a running thread."
  (session--require-state session '(:running))
  (session--identifier thread-id)
  (session-request session "pause" (json-object "threadId" thread-id) :timeout timeout :cancel-p cancel-p))

(defun session-step (session thread-id kind &key (timeout 10) cancel-p)
  "Step :IN, :OVER or :OUT in a stopped thread."
  (session--require-state session '(:stopped))
  (session--identifier thread-id)
  (unless (member kind '(:in :over :out))
    (error 'dap-protocol-error :message "Step kind must be :in, :over or :out."))
  (session-request session (ecase kind (:in "stepIn") (:over "next") (:out "stepOut"))
                   (json-object "threadId" thread-id) :timeout timeout :cancel-p cancel-p))

(defun session-threads (session &key (timeout 10) cancel-p)
  "Return the adapter's threads body."
  (session--require-state session '(:running :stopped))
  (session-request session "threads" (json-object) :timeout timeout :cancel-p cancel-p))

(defun session-stack-trace (session thread-id &key (start-frame 0) (levels 100) (timeout 10) cancel-p)
  "Return a bounded stack page from a stopped session."
  (session--require-state session '(:stopped))
  (session--identifier thread-id)
  (session--page start-frame levels)
  (session-request session "stackTrace" (json-object "threadId" thread-id "startFrame" start-frame "levels" levels)
                   :timeout timeout :cancel-p cancel-p))

(defun session-scopes (session frame-id &key (timeout 10) cancel-p)
  "Return scopes for a stopped frame."
  (session--require-state session '(:stopped))
  (session--identifier frame-id)
  (session-request session "scopes" (json-object "frameId" frame-id) :timeout timeout :cancel-p cancel-p))

(defun session-variables (session reference &key (start 0) (count 100) (timeout 10) cancel-p)
  "Return a bounded variables page from a stopped session."
  (session--require-state session '(:stopped))
  (session--identifier reference)
  (session--page start count)
  (session-request session "variables" (json-object "variablesReference" reference "start" start "count" count)
                   :timeout timeout :cancel-p cancel-p))

(defun session-evaluate (session expression &key frame-id (context "repl") (timeout 10) cancel-p)
  "Evaluate an expression using adapter semantics; it may mutate the debuggee."
  (session--require-state session '(:running :stopped))
  (unless (and (stringp expression) (stringp context))
    (error 'dap-protocol-error :message "Evaluation requires expression and context strings."))
  (when frame-id (session--identifier frame-id))
  (let ((arguments (json-object "expression" expression "context" context)))
    (when frame-id (setf (gethash "frameId" arguments) frame-id))
    (session-request session "evaluate" arguments :timeout timeout :cancel-p cancel-p)))

(defun session-terminate (session &key (disconnect nil) (terminate-debuggee t) (timeout 10) cancel-p)
  "Request terminate (or disconnect), then always close/reap the adapter transport."
  (unwind-protect
       (session-request session (if disconnect "disconnect" "terminate")
                        (if disconnect (json-object "terminateDebuggee" (json-boolean terminate-debuggee))
                            (json-object)) :timeout timeout :cancel-p cancel-p)
    (session-close session)))
