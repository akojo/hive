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

module Test_qupt = Qupt.Make(Test_machine)(Int)

let append ?(sender = 0) ?(term = 0) ?(prev_idx = 0) ?(prev_term = 0) ?(commit = 0) log =
  Test_qupt.{ sender; term; message = Append (prev_idx, prev_term, commit, log)}

let response ?(sender = 1) ?(term = 0) message =
  Test_qupt.{ sender; term; message}

let vote ?(sender = 1) ?(term = 0) last_idx last_term =
  Test_qupt.{ sender; term; message = Vote { last_idx; last_term }}

let assert_io expected actual =
  let print_io io = [%sexp_of: Test_qupt.io list] io |> Sexp.to_string_hum in
  assert_equal ~printer:print_io expected actual

let assert_state expected actual =
  let print_state state = [%sexp_of: int list] state |> Sexp.to_string_hum in
  assert_equal ~printer:print_state expected !actual

let test f _ =
  f (reset [])

let leader_2 =
  let open Test_qupt in
  let qupt = init 0 Leader [0;1] test_state in
  "leader (2 nodes)" >::: [
    "timeout sends heartbeat to followers" >:: test (fun _ ->
        let (io, _) = handle_timeout qupt in
        assert_io [Rpc (1, append [])] io
      );

    "client command sends append rpc to followers" >:: test (fun _ ->
        let (req_id, io, _) = handle_command qupt 17 in
        let expected = append [(1, 0, 17 )] in
        assert_equal 1 req_id;
        assert_io [Rpc (1, expected)] io
      );

    "succesful append response commits log entry" >:: test (fun state ->
        let (_, _, qupt) = handle_command qupt 18 in
        let _ = handle_rpc qupt (response (AppendResponse (true, 1))) in
        assert_state [18] state
      );

    "commit index is incremented after commit" >:: test (fun _ ->
        let (_, _, qupt) = handle_command qupt 18 in
        let (_, qupt) = handle_rpc qupt (response (AppendResponse (true, 1))) in
        let (io, _) = handle_timeout qupt in
        assert_io [Rpc (1, append ~prev_idx:1 ~commit:1 [])] io
      );

    "client response is sent after commit" >:: test (fun _ ->
        let (index, _, qupt) = handle_command qupt 18 in
        let (io, _) = handle_rpc qupt (response (AppendResponse (true, 1))) in
        assert_io [Response (index, 18)] io
      );

    "append failure sends logs from last client commmit" >:: test (fun _ ->
        let _, _, qupt = handle_command qupt 18 in
        let _, _, qupt = handle_command qupt 19 in
        let io, _ = handle_rpc qupt (response (AppendResponse (false, 1))) in
        let expected = [(2, 0, 19)] in
        assert_io [Rpc (1, append ~prev_idx:1 expected)] io
      );
  ]

let leader_4 =
  let open Test_qupt in
  let qupt = init 0 Leader [0;1;2;3] test_state in
  "leader (4 nodes)" >::: [
    "client command sends append to all nodes" >:: test (fun _ ->
        let (_, io, _) = handle_command qupt 110 in
        let expected = append [(1, 0, 110)] in
        assert_io [Rpc (1, expected); Rpc (2, expected); Rpc (3, expected)] io
      );

    "minority does not commit log" >:: test (fun state ->
        let (_, _, qupt) = handle_command qupt 110 in
        let (io, _) = handle_rpc qupt (response ~sender:1 (AppendResponse (true, 1))) in
        assert_io [] io;
        assert_state [] state
      );

    "majority commits log" >:: test (fun state ->
        let (index, _, qupt) = handle_command qupt 110 in
        let (_, qupt) = handle_rpc qupt (response ~sender:1 (AppendResponse (true, 1))) in
        let (io, _) = handle_rpc qupt (response ~sender:2 (AppendResponse (true, 1))) in
        assert_io [Response (index, 110)] io;
        assert_state [110] state
      );

    "client response is sent only after first majority" >:: test (fun _ ->
        let (_, _, qupt) = handle_command qupt 110 in
        let (_, qupt) = handle_rpc qupt (response ~sender:1 (AppendResponse (true, 1))) in
        let (_, qupt) = handle_rpc qupt (response ~sender:2 (AppendResponse (true, 1))) in
        let (io, _) = handle_rpc qupt (response ~sender:3 (AppendResponse (true, 1))) in
        assert_io [] io
      );

    "new state is committed only once" >:: test (fun state ->
        let (_, _, qupt) = handle_command qupt 110 in
        let (_, qupt) = handle_rpc qupt (response ~sender:1 (AppendResponse (true, 1))) in
        let (_, qupt) = handle_rpc qupt (response ~sender:2 (AppendResponse (true, 1))) in
        let _ = handle_rpc qupt (response ~sender:3 (AppendResponse (true, 1))) in
        assert_state [110] state
      )
  ]

let follower_2 =
  let open Test_qupt in
  let qupt = init 1 Follower [0;1] test_state in
  "follower (2 nodes)" >::: [
    "append into empty state returns success to leader" >:: test (fun _ ->
        let rpc = append [(1, 0, 17)] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, response (AppendResponse (true, 1)))] io
      );

    "larger term increases term number" >:: test (fun _ ->
        let rpc = append ~term:1 [(1, 1, 17)] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, response ~term:1 (AppendResponse (true, 1)))] io
      );

    "append with old term returns failure to leader" >:: test (fun _ ->
        let rpc = append ~term:1 [(1, 0, 17)] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append [(1, 0, 18)] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, response ~term:1 (AppendResponse (false, 0)))] io
      );

    "append returns failure when prev index not found" >:: test (fun _ ->
        let rpc = append [(1, 0, 17)] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append ~prev_idx:2 [(3, 0, 18)] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, response (AppendResponse (false, 0)))] io
      );

    "append failure returns last committed index" >:: test (fun _ ->
        let rpc = append ~commit:1 [(4, 0, 20); (3, 0, 19); (1, 0, 18)] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append ~prev_idx:2 [(4, 0, 20)] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, response (AppendResponse (false, 1)))] io
      );

    "leader commit commits log" >:: test (fun state ->
        let rpc = append ~commit:2 [(2, 0, 19); (1, 0, 18)] in
        let _ = handle_rpc qupt rpc in
        assert_state [19; 18] state
      );

    "append removes entries not in leader log" >:: test (fun state ->
        let rpc = append [(3, 0, 20); (2, 0, 19); (1, 0, 18)] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append ~commit:2 ~prev_idx:1 [(2, 0, 30)] in
        let _ = handle_rpc qupt rpc in
        assert_state [30; 18] state
      );

    "append commits min(commit, last index)" >:: test (fun state ->
        let rpc = append ~commit:3 [(1, 0, 100)] in
        let (_, qupt) = handle_rpc qupt rpc in
        let () = assert_state [100] state in
        let rpc = append ~commit:3 [(3, 0, 102); (2, 0, 101)] in
        let _ = handle_rpc qupt rpc in
        assert_state [102; 101; 100] state
      );

    "convert to candidate if election timeout elapses" >:: (fun _ ->
        let (io, _) = handle_timeout qupt in
        assert_io [Rpc (0, vote ~term:1 0 0)] io
      );
  ]

let () =
  let tests = test_list [
      leader_2;
      leader_4;
      follower_2
    ]
  in
  run_test_tt_main tests
