open Core.Std
open OUnit2

let () =
  let tests = test_list [
      Qupt_test.all;
      Log_test.all;
    ]
  in
  run_test_tt_main tests
