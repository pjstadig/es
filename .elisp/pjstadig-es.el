;;; Hooks

(defun format-buffer ()
  "Should be used in a git-hook because calling this function
actually kills emacs. Exits with code 0 if the file is correctly
formatted, 1 if it isn't."
  (indent-region (point-min) (point-max))
  (untabify (point-min) (point-max))
  (delete-trailing-whitespace)
  (if (buffer-modified-p)
      (kill-emacs 1)
    (kill-emacs 0)))

(when (> emacs-major-version 23)
  ;; Emacs 24 needs new whitespace-style settings
  (setq whitespace-style '(face trailing lines tabs)
        whitespace-line-column 80))

(defun my-clojure-test-for (namespace)
  (let* ((namespace (clojure-underscores-for-hyphens namespace))
         (segments (split-string namespace "\\."))
         (before (subseq segments 0 2))
         (after (subseq segments 2))
         (test-segments (append before (list "test") after)))
    (format "%stest/%s.clj"
            (locate-dominating-file buffer-file-name "src/")
            (mapconcat 'identity test-segments "/"))))

(defun my-clojure-test-implementation-for (namespace)
  (let* ((namespace (clojure-underscores-for-hyphens namespace))
         (segments (split-string namespace "\\."))
         (before (subseq segments 0 2))
         (after (subseq segments 3))
         (impl-segments (append before after)))
    (format "%s/src/%s.clj"
            (locate-dominating-file buffer-file-name "src/")
            (mapconcat 'identity impl-segments "/"))))

(eval-after-load 'clojure-mode
  '(add-hook 'clojure-mode-hook
             (lambda ()
               (progn
                 (font-lock-add-keywords
                  'clojure-mode
                  '(("(\\(with-[^[:space:]]*\\)" (1 font-lock-keyword-face))
                    ("(\\(when-[^[:space:]]*\\)" (1 font-lock-keyword-face))
                    ("(\\(if-[^[:space:]]*\\)" (1 font-lock-keyword-face))
                    ("(\\(def[^[:space:]]*\\)" (1 font-lock-keyword-face))
                    ("(\\(try\\+\\)" (1 font-lock-keyword-face))
                    ("(\\(throw\\+\\)" (1 font-lock-keyword-face))))
                 (setq clojure-test-for-fn 'my-clojure-test-for)
                 (setq clojure-test-implementation-for-fn 'my-clojure-test-implementation-for)))))

(provide 'pjstadig-es)
