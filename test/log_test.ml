open Core.Std
open OUnit2

let log_tests (type a) log_module setup suite_name =
  let module Test_log = (val log_module : Log_intf.Log with type command = int and type t = a) in

  let open Test_log in

  let assert_entry expected actual =
    let print_entry entry = [%sexp_of: Test_log.entry option] entry |> Sexp.to_string_hum in
    assert_equal ~printer:print_entry expected actual
  in

  let assert_entry_list expected actual =
    let print_entries entries = [%sexp_of: Test_log.entry list] entries |> Sexp.to_string_hum in
    assert_equal ~printer:print_entries expected actual
  in

  let empty_log =
    let test f ctx = f (setup ctx) in
    "empty log" >::: [
      "tells that log is empty" >:: test (fun log ->
          assert_equal true (is_empty log)
        );

      "returns 'false' for exists with any parameters" >:: test (fun log ->
          assert_equal None (find ~index:0 log)
        );

      "returns empty list for from" >:: test (fun log ->
          assert_equal [] (from ~index:0 log)
        );

      "appends element" >:: test (fun log ->
          let entry = { Test_log.index = 1; term = 0; command = 1 } in
          let log = append log entry in
          assert_entry (Some entry) (find ~index:1 log)
        );

      "returns empty range" >:: test (fun log ->
          assert_equal [] (range ~first:0 ~last:100 log)
        );

      "returns no last element" >:: test (fun log ->
          assert_equal None (last log)
        );

      "returns no last element before anything" >:: test (fun log ->
          assert_equal None (last_before ~index:100 log)
        );
    ]
  in

  let populated_log =
    let test f ctx =
      let log = setup ctx in
      let log = Test_log.append_after ~index:0 log ~entries:[
          { Test_log.index = 5; term = 2; command = 40 };
          { Test_log.index = 4; term = 2; command = 30 };
          { Test_log.index = 2; term = 2; command = 20 };
          { Test_log.index = 1; term = 2; command = 10 };
        ]
      in
      f log
    in
    "populated log" >::: [
      "finds an element in the log" >:: test (fun log ->
          let expected = Some { Test_log.index = 4; term = 2; command = 30 } in
          assert_entry expected (find ~index:4 log)
        );

      "returns last log element" >:: test (fun log ->
          let expected = Some { Test_log.index = 5; term = 2; command = 40 } in
          assert_entry expected (last log)
        );

      "returns last log element before given index" >:: test (fun log ->
          let expected = Some { Test_log.index = 2; term = 2; command = 20 } in
          assert_entry expected (last_before ~index:4 log)
        );

      "returns empty element for index before first" >:: test (fun log ->
          assert_entry None (last_before ~index:0 log)
        );

      "returns entries from given index" >:: test (fun log ->
          let expected = [
            { Test_log.index = 5; term = 2; command = 40 };
            { Test_log.index = 4; term = 2; command = 30 };
            { Test_log.index = 2; term = 2; command = 20 };
          ]
          in
          assert_entry_list expected (from ~index:2 log)
        );

      "returns entries after non-existent index" >:: test (fun log ->
          let expected = [
            { Test_log.index = 5; term = 2; command = 40 };
            { Test_log.index = 4; term = 2; command = 30 };
          ]
          in
          assert_entry_list expected (from ~index:3 log)
        );

      "returns a half-open range of entries" >:: test (fun log ->
          let expected = [
            { Test_log.index = 5; term = 2; command = 40 };
            { Test_log.index = 4; term = 2; command = 30 };
          ]
          in
          assert_entry_list expected (range ~first:3 ~last:5 log)
        );

      "returns entries from beginning when range starts from 0" >:: test (fun log ->
          let expected = [{ Test_log.index = 1; term = 2; command = 10 }] in
          assert_entry_list expected (range ~first:0 ~last:1 log)
        );

      "returns empty list when range endpoints are equal" >:: test (fun log ->
          assert_entry_list [] (range ~first:1 ~last:1 log)
        );

      "adds appended element to the end" >:: test (fun log ->
          let new_entry = { Test_log.index = 6; term = 2; command = 50 } in
          let log = append log new_entry in
          let expected = [
            new_entry;
            { Test_log.index = 5; term = 2; command = 40 };
          ]
          in
          assert_entry_list expected (range ~first:4 ~last:6 log)
        );

      "adds appended elements to the end" >:: test (fun log ->
          let new_entries = [
            { Test_log.index = 7; term = 2; command = 60 };
            { Test_log.index = 6; term = 2; command = 50 }
          ]
          in
          let log = append_after ~index:5 ~entries:new_entries log in
          let expected = new_entries @ [
              { Test_log.index = 5; term = 2; command = 40 };
            ]
          in
          assert_entry_list expected (range ~first:4 ~last:7 log)
        );

      "drops elements after append index" >:: test (fun log ->
          let new_entries = [
            { Test_log.index = 7; term = 2; command = 60 };
            { Test_log.index = 6; term = 2; command = 50 }
          ]
          in
          let log = append_after ~index:1 ~entries:new_entries log in
          let expected = new_entries @ [
              { Test_log.index = 1; term = 2; command = 10 };
            ]
          in
          assert_entry_list expected (range ~first:0 ~last:7 log)
        );
    ]
  in
  suite_name >::: [
    empty_log;
    populated_log;
  ]

module Int_command = struct
  type command = int [@@deriving sexp]
end

module Sqlite_log = Log_sqlite.Make(Int_command)
let sqlite_log = (module Sqlite_log : Log_intf.Log with type command = int and type t = 'a)
let setup_sqlite ctx =
  let filename, _ = bracket_tmpfile ~prefix:"log_test" ctx in
  bracket (fun _ctx -> Sqlite_log.create filename) (fun log _ctx -> Sqlite_log.close log) ctx

module Memory_log = Log_memory.Make(Int_command)
let in_memory_log = (module Memory_log : Log_intf.Log with type command = int and type t = 'a)
let setup_memory _ctx = Memory_log.empty

let all = "Log" >::: [
    log_tests sqlite_log setup_sqlite "Sqlite3 log";
    log_tests in_memory_log setup_memory "In-memory log";
  ]
