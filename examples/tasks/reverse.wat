;; Reverses its input. Input goes to address 0, the output right after it.
(module
  (import "env" "input_len" (func $input_len (result i32)))
  (import "env" "read_input" (func $read_input (param i32)))
  (import "env" "write_output" (func $write_output (param i32 i32)))
  (memory (export "memory") 1)
  (func (export "run")
    (local $n i32) (local $i i32)
    (local.set $n (call $input_len))
    ;; Grow memory to fit 2n bytes (fails with -1 beyond the job's memory limit).
    (drop (memory.grow (i32.div_u (i32.mul (local.get $n) (i32.const 2)) (i32.const 65536))))
    (call $read_input (i32.const 0))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
        (i32.store8
          (i32.add (local.get $n) (local.get $i))
          (i32.load8_u (i32.sub (i32.sub (local.get $n) (local.get $i)) (i32.const 1))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next)))
    (call $write_output (local.get $n) (local.get $n))))
