;; Never finishes: the sandbox stops it when the job's fuel runs out.
(module
  (memory (export "memory") 1)
  (func (export "run") (loop $forever (br $forever))))
