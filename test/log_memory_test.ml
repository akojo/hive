open Core.Std
open OUnit2

module Test_log = Log_memory.Make(struct
    type command = int [@@deriving sexp]
  end)

let assert_entry expected actual =
  let print_entry entry = [%sexp_of: Test_log.entry option] entry |> Sexp.to_string_hum in
  assert_equal ~printer:print_entry expected actual

let assert_entry_list expected actual =
  let print_entries entries = [%sexp_of: Test_log.entry list] entries |> Sexp.to_string_hum in
  assert_equal ~printer:print_entries expected actual

let empty_log =
  let open Test_log in
  let log = Test_log.empty in
  "empty log" >::: [
    "returns 'false' for exists with any parameters" >:: (fun _ ->
        assert_equal None (find ~index:0 log)
      );

    "returns empty list for from" >:: (fun _ ->
        assert_equal [] (from ~index:0 log)
      );

    "appends element" >:: (fun _ ->
        let entry = { Test_log.index = 1; term = 0; command = 1 } in
        let log = append log entry in
        assert_entry (Some entry) (find ~index:1 log)
      );

    "returns empty range" >:: (fun _ ->
        assert_equal [] (range ~first:0 ~last:100 log)
      );

    "returns no last element" >:: (fun _ ->
        assert_equal None (last log)
      );

    "returns no last element before anything" >:: (fun _ ->
        assert_equal None (last_before ~index:100 log)
      );
  ]

let populated_log =
  let open Test_log in
  let log = Test_log.empty
            |> append_after ~index:0 ~entries:[
              { Test_log.index = 5; term = 2; command = 40 };
              { Test_log.index = 4; term = 2; command = 30 };
              { Test_log.index = 2; term = 2; command = 20 };
              { Test_log.index = 1; term = 2; command = 10 };
            ]
  in
  "populated log" >::: [
    "finds an element in the log" >:: (fun _ ->
        let expected = Some { Test_log.index = 4; term = 2; command = 30 } in
        assert_entry expected (find ~index:4 log)
      );

    "returns last log element" >:: (fun _ ->
        let expected = Some { Test_log.index = 5; term = 2; command = 40 } in
        assert_entry expected (last log)
      );

    "returns last log element before given index" >:: (fun _ ->
        let expected = Some { Test_log.index = 2; term = 2; command = 20 } in
        assert_entry expected (last_before ~index:4 log)
      );

    "returns empty element for index before first" >:: (fun _ ->
        assert_entry None (last_before ~index:0 log)
      );

    "returns entries from given index" >:: (fun _ ->
        let expected = [
          { Test_log.index = 5; term = 2; command = 40 };
          { Test_log.index = 4; term = 2; command = 30 };
          { Test_log.index = 2; term = 2; command = 20 };
        ]
        in
        assert_entry_list expected (from ~index:2 log)
      );

    "returns entries after non-existent index" >:: (fun _ ->
        let expected = [
          { Test_log.index = 5; term = 2; command = 40 };
          { Test_log.index = 4; term = 2; command = 30 };
        ]
        in
        assert_entry_list expected (from ~index:3 log)
      );

    "returns a half-open range of entries" >:: (fun _ ->
        let expected = [
          { Test_log.index = 5; term = 2; command = 40 };
          { Test_log.index = 4; term = 2; command = 30 };
        ]
        in
        assert_entry_list expected (range ~first:3 ~last:5 log)
      );

    "returns entries from beginning when range starts from 0" >:: (fun _ ->
        let expected = [{ Test_log.index = 1; term = 2; command = 10 }] in
        assert_entry_list expected (range ~first:0 ~last:1 log)
      );

    "returns empty list when range endpoints are equal" >:: (fun _ ->
        assert_entry_list [] (range ~first:1 ~last:1 log)
      );

    "adds appended element to the end" >:: (fun _ ->
        let new_entry = { Test_log.index = 6; term = 2; command = 50 } in
        let log = append log new_entry in
        let expected = [
          new_entry;
          { Test_log.index = 5; term = 2; command = 40 };
        ]
        in
        assert_entry_list expected (range ~first:4 ~last:6 log)
      );

    "adds appended elements to the end" >:: (fun _ ->
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

    "drops elements after append index" >:: (fun _ ->
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

let all =
  "in-memory log" >::: [
    empty_log;
    populated_log;
  ]
