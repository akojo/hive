open Core.Std
open OUnit2

let test_state = ref []

let reset new_state =
  test_state := new_state;
  test_state

module Test_machine =
struct
  type t = int list ref
  type command = int [@@deriving sexp]
  type response = int [@@deriving sexp]

  let apply state command =
    state := command :: !state;
    command, state
end

module Test_raft = Raft.Make(Test_machine)(Int)

let print_io io = [%sexp_of: Test_raft.io list] io |> Sexp.to_string_hum

let print_state state = [%sexp_of: int list] state |> Sexp.to_string_hum

let append ?(sender = 0) ?(term = 0) ?(prev_idx = 0) ?(prev_term = 0) ?(commit = 0) log =
  Test_raft.{ sender; term; message = Append { prev_idx; prev_term; log; commit }}

let response ?(sender = 1) ?(term = 0) message =
  Test_raft.{ sender; term; message}

let test f _ =
  f (reset [])

let leader =
  let open Test_raft in
  "leader" >::: [
    "timeout sends heartbeat to followers" >:: test (fun _ ->
        let raft = init 0 Leader [0;1] test_state in
        let (io, _) = handle_timeout raft in
        let expected = append [] in
        assert_equal ~printer:print_io [Rpc (1, expected)] io
      );

    "client command sends append rpc to followers" >:: test (fun _ ->
        let raft = init 0 Leader [0;1] test_state in
        let (req_id, io, _) = handle_command raft 17 in
        let expected = append [{ index = 1; term = 0; command = 17 }] in
        assert_equal 1 req_id;
        assert_equal ~printer:print_io [Rpc (1, expected)] io
      );

    "succesful append response commits log entry" >:: test (fun state ->
        let raft = init 0 Leader [0;1] test_state in
        let (_, _, raft) = handle_command raft 18 in
        let _ = handle_rpc raft (response (AppendSuccess 1)) in
        assert_equal ~printer:print_state [18] !state
      );

    "commit index is incremented after commit" >:: test (fun _ ->
        let raft = init 0 Leader [0;1] test_state in
        let (_, _, raft) = handle_command raft 18 in
        let (_, raft) = handle_rpc raft (response (AppendSuccess 1)) in
        let (io, _) = handle_timeout raft in
        assert_equal ~printer:print_io [Rpc (1, append ~prev_idx:1 ~commit:1 [])] io
      );

    "client response is sent after commit" >:: test (fun _ ->
        let raft = init 0 Leader [0;1] test_state in
        let (index, _, raft) = handle_command raft 18 in
        let (io, _) = handle_rpc raft (response (AppendSuccess 1)) in
        assert_equal ~printer:print_io [Response (index, 18)] io
      );
  ]

let follower =
  let open Test_raft in
  "follower" >::: [
    "append into empty state returns success to leader" >:: test (fun _ ->
        let raft = init 1 Follower [0;1] test_state in
        let rpc = append [{ index = 1; term = 0; command = 17 }] in
        let (io, _) = handle_rpc raft rpc in
        assert_equal ~printer:print_io [Rpc (0, response (AppendSuccess 1))] io
      );

    "leader commit commits log" >:: test (fun state ->
        let raft = init 1 Follower [0;1] test_state in
        let rpc = append ~commit:2 [
            { index = 2; term = 0; command = 19 };
            { index = 1; term = 0; command = 18 };
          ] in
        let _ = handle_rpc raft rpc in
        assert_equal ~printer:print_state [19; 18] !state
      )
  ]

let () =
  let tests = test_list [leader; follower] in
  run_test_tt_main tests
