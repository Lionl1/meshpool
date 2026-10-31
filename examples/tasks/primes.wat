;; Counts primes below n by trial division. Input: n as 4 bytes little endian.
;; Output: the count, 4 bytes little endian. CPU-bound benchmark task.
(module
  (import "env" "read_input" (func $read_input (param i32)))
  (import "env" "write_output" (func $write_output (param i32 i32)))
  (memory (export "memory") 1)
  (func (export "run")
    (local $n i32) (local $i i32) (local $d i32) (local $count i32) (local $prime i32)
    (call $read_input (i32.const 0))
    (local.set $n (i32.load (i32.const 0)))
    (local.set $i (i32.const 2))
    (block $end
      (loop $outer
        (br_if $end (i32.ge_u (local.get $i) (local.get $n)))
        (local.set $prime (i32.const 1))
        (local.set $d (i32.const 2))
        (block $checked
          (loop $inner
            (br_if $checked (i32.gt_u (i32.mul (local.get $d) (local.get $d)) (local.get $i)))
            (if (i32.eqz (i32.rem_u (local.get $i) (local.get $d)))
              (then (local.set $prime (i32.const 0)) (br $checked)))
            (local.set $d (i32.add (local.get $d) (i32.const 1)))
            (br $inner)))
        (local.set $count (i32.add (local.get $count) (local.get $prime)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $outer)))
    (i32.store (i32.const 8) (local.get $count))
    (call $write_output (i32.const 8) (i32.const 4))))
