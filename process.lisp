(in-package #:daphne)

(defclass process-transport (stream-transport)
  ((process :initarg :process :reader adapter-process)
   (error-stream :initarg :error-stream :reader adapter-error-stream)
   (error-thread :initform nil :accessor adapter-error-thread)
   (stderr :initform (make-array 0 :element-type '(unsigned-byte 8)
                                :adjustable t :fill-pointer 0) :reader adapter-error-buffer)
   (stderr-limit :initarg :stderr-limit :reader adapter-stderr-limit)
   (lock :initform (bt:make-lock "DAP process") :reader adapter-lock)
   (closed :initform nil :accessor adapter-closed-p))
  (:documentation "An owned SBCL adapter subprocess with bounded stderr capture."))

(defun adapter-stderr (transport)
  "Return a detached vector containing the first stderr octets retained by TRANSPORT."
  (bt:with-lock-held ((adapter-lock transport))
    (copy-seq (adapter-error-buffer transport))))

(defmethod transport-close ((transport process-transport))
  (let ((close-p (bt:with-lock-held ((adapter-lock transport))
                   (unless (adapter-closed-p transport)
                     (setf (adapter-closed-p transport) t)))))
    (when close-p
      ;; Kill before closing a pipe with a blocked reader. No process is shared.
      (let ((process (adapter-process transport)))
        (when (sb-ext:process-alive-p process)
          (ignore-errors (sb-ext:process-kill process 15))
          (loop repeat 20 while (sb-ext:process-alive-p process) do (sleep 0.01))
          (when (sb-ext:process-alive-p process)
            (ignore-errors (sb-ext:process-kill process 9))))
        (sb-ext:process-wait process)
        (call-next-method)
        (ignore-errors (close (adapter-error-stream transport) :abort t))
        (sb-ext:process-close process))))
  nil)

(defun start-adapter (program arguments &key directory environment
                                        (max-header 8192) (max-body (* 16 1024 1024))
                                        (stderr-limit 65536) (max-events 1024) (max-pending 64))
  "Start PROGRAM with literal ARGUMENTS and return SESSION and PROCESS-TRANSPORT.
The owned adapter is killed/reaped on session close. ENVIRONMENT, when non-NIL,
is a list of NAME=VALUE strings replacing the inherited environment. No shell
is involved. SBCL's pipe streams support octet I/O. Stderr is drained separately."
  (unless (and (typep stderr-limit '(integer 0))
               (typep max-header '(integer 1)) (typep max-body '(integer 1))
               (typep max-events '(integer 1)) (typep max-pending '(integer 1)))
    (error 'dap-limit-error :message "Adapter limits are invalid."))
  (let* ((process
           (handler-case
               (apply #'sb-ext:run-program program arguments
                      :search t :wait nil :input :stream :output :stream :error :stream
                      :directory directory (when environment (list :environment environment)))
             (error (cause)
               (error 'dap-transport-error :message "Could not start debug adapter." :cause cause))))
         (transport nil))
    (handler-case
        (progn
          (setf transport
                (make-instance 'process-transport :process process
                               :input (sb-ext:process-output process)
                               :output (sb-ext:process-input process)
                               :error-stream (sb-ext:process-error process)
                               :stderr-limit stderr-limit :max-header max-header :max-body max-body))
          (setf (adapter-error-thread transport)
                (bt:make-thread
                 (lambda ()
                   (handler-case
                       (loop for byte = (read-byte (adapter-error-stream transport) nil nil)
                             while byte
                             do (bt:with-lock-held ((adapter-lock transport))
                                  (when (< (length (adapter-error-buffer transport)) stderr-limit)
                                    (vector-push-extend byte (adapter-error-buffer transport)))))
                     (error () nil)))
                 :name "DAP stderr"))
          (values (make-session transport :max-events max-events :max-pending max-pending) transport))
      (error (cause)
        (if transport (transport-close transport)
            (progn (ignore-errors (sb-ext:process-kill process 9))
                   (sb-ext:process-wait process) (sb-ext:process-close process)))
        (error cause)))))
