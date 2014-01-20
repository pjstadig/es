((nil
  (fill-column . 80)
  (whitespace-line-column . nil))
 (clojure-mode
  (eval ignore-errors
        (add-to-list 'load-path (concat
                                 (locate-dominating-file default-directory "project.clj")
                                 ".elisp"))
        (require 'pjstadig-es)
        (require 'whitespace)
        (whitespace-mode 0)
        (whitespace-mode 1))))
