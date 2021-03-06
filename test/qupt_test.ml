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

module Test_log = Log_memory.Make(Test_machine)

module Test_qupt = Qupt.Make(Test_machine)(Int)(Test_log)

let response ?(sender = 1) ?(term = 0) message =
  Test_qupt.{ sender; term; message}

let append ?(sender = 0) ?(term = 0) ?(prev_idx = 0) ?(prev_term = 0) ?(commit = 0) entries =
  response ~sender ~term Test_qupt.(Append { prev_idx; prev_term; commit; entries })

let append_response ?(sender = 1) ?(term = 0) ?(index = 1) success =
  response ~sender ~term Test_qupt.(AppendResponse { success; index })

let vote ?(sender = 1) ?(term = 0) last_idx last_term =
  response ~sender ~term Test_qupt.(Vote { last_idx; last_term })

let vote_granted ?(sender = 1) ?(term = 0) () =
  response ~sender ~term Test_qupt.VoteGranted

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
  let qupt = init true 0 [0;1] test_state (Test_log.empty) 1.0 in
  "leader (2 nodes)" >::: [
    "timeout sends heartbeat to followers" >:: test (fun _ ->
        let (io, _) = handle_timeout qupt in
        assert_io [Rpc (1, append [])] io
      );

    "client command sends append rpc to followers" >:: test (fun _ ->
        let (req_id, io, _) = handle_command qupt 17 in
        let expected = append [{ Test_log.index = 1; term = 0; command = 17 }] in
        assert_equal 1 req_id;
        assert_io [Rpc (1, expected)] io
      );

    "succesful append response commits log entry" >:: test (fun state ->
        let (_, _, qupt) = handle_command qupt 18 in
        let _ = handle_rpc qupt (append_response true) in
        assert_state [18] state
      );

    "commit index is incremented after commit" >:: test (fun _ ->
        let (_, _, qupt) = handle_command qupt 18 in
        let (_, qupt) = handle_rpc qupt (append_response true) in
        let (io, _) = handle_timeout qupt in
        assert_io [Rpc (1, append ~prev_idx:1 ~commit:1 [])] io
      );

    "client response is sent after commit" >:: test (fun _ ->
        let (index, _, qupt) = handle_command qupt 18 in
        let (io, _) = handle_rpc qupt (append_response true) in
        assert_io [Response (index, 18)] io
      );

    "append failure sends logs from last client commmit" >:: test (fun _ ->
        let _, _, qupt = handle_command qupt 18 in
        let _, _, qupt = handle_command qupt 19 in
        let io, _ = handle_rpc qupt (append_response false) in
        let expected = [{ Test_log.index = 2; term = 0; command = 19 }] in
        assert_io [Rpc (1, append ~prev_idx:1 expected)] io
      );
  ]

let leader_4 =
  let open Test_qupt in
  let qupt = init true 0 [0;1;2;3] test_state (Test_log.empty) 1.0 in
  "leader (4 nodes)" >::: [
    "client command sends append to all nodes" >:: test (fun _ ->
        let (_, io, _) = handle_command qupt 110 in
        let expected = append [{ Test_log.index = 1; term = 0; command = 110 }] in
        assert_io [Rpc (1, expected); Rpc (2, expected); Rpc (3, expected)] io
      );

    "minority does not commit log" >:: test (fun state ->
        let (_, _, qupt) = handle_command qupt 110 in
        let (io, _) = handle_rpc qupt (append_response ~sender:1 true) in
        assert_io [] io;
        assert_state [] state
      );

    "majority commits log" >:: test (fun state ->
        let (index, _, qupt) = handle_command qupt 110 in
        let (_, qupt) = handle_rpc qupt (append_response ~sender:1 true) in
        let (io, _) = handle_rpc qupt (append_response ~sender:2 true) in
        assert_io [Response (index, 110)] io;
        assert_state [110] state
      );

    "client response is sent only after first majority" >:: test (fun _ ->
        let (_, _, qupt) = handle_command qupt 110 in
        let (_, qupt) = handle_rpc qupt (append_response ~sender:1 true) in
        let (_, qupt) = handle_rpc qupt (append_response ~sender:2 true) in
        let (io, _) = handle_rpc qupt (append_response ~sender:3 true) in
assert_io [] io
);

"new state is committed only once" >:: test (fun state ->
    let (_, _, qupt) = handle_command qupt 110 in
    let (_, qupt) = handle_rpc qupt (append_response ~sender:1 true) in
    let (_, qupt) = handle_rpc qupt (append_response ~sender:2 true) in
    let _ = handle_rpc qupt (append_response ~sender:3 true) in
    assert_state [110] state
  )
]

let follower_2 =
  let open Test_qupt in
  let qupt = init false 1 [0;1] test_state (Test_log.empty) 1.0 in
  "follower (2 nodes)" >::: [
    "append into empty state returns success to leader" >:: test (fun _ ->
        let rpc = append [{ Test_log.index = 1; term = 0; command = 17 }] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, append_response true)] io
      );

    "larger term increases term number" >:: test (fun _ ->
        let rpc = append ~term:1 [{ Test_log.index = 1; term = 1; command = 17 }] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, append_response ~term:1 true)] io
      );

    "append with old term returns failure to leader" >:: test (fun _ ->
        let rpc = append ~term:1 [{ Test_log.index = 1; term = 0; command = 17 }] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append [{ Test_log.index = 1; term = 0; command = 18 }] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, append_response ~term:1 ~index:0 false)] io
      );

    "append returns failure when prev index not found" >:: test (fun _ ->
        let rpc = append [{ Test_log.index = 1; term = 0; command = 17 }] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append ~prev_idx:2 [{ Test_log.index = 3; term = 0; command = 18 }] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, append_response ~index:0 false)] io
      );

    "append failure returns last committed index" >:: test (fun _ ->
        let rpc = append ~commit:1 [
            { Test_log.index = 4; term = 0; command = 20 };
            { Test_log.index = 3; term = 0; command = 19 };
            { Test_log.index = 1; term = 0; command = 18 };
          ] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append ~prev_idx:2 [{ Test_log.index = 4; term = 0; command = 20 }] in
        let (io, _) = handle_rpc qupt rpc in
        assert_io [Rpc (0, append_response false)] io
      );

    "leader commit commits log" >:: test (fun state ->
        let rpc = append ~commit:2 [
            { Test_log.index = 2; term = 0; command = 19 };
            { Test_log.index = 1; term = 0; command = 18 }
          ] in
        let _ = handle_rpc qupt rpc in
        assert_state [19; 18] state
      );

    "append removes entries not in leader log" >:: test (fun state ->
        let rpc = append [
            { Test_log.index = 3; term = 0; command = 20 };
            { Test_log.index = 2; term = 0; command = 19 };
            { Test_log.index = 1; term = 0; command = 18 }
          ] in
        let (_, qupt) = handle_rpc qupt rpc in
        let rpc = append ~commit:2 ~prev_idx:1 [{ Test_log.index = 2; term = 0; command = 30 }] in
        let _ = handle_rpc qupt rpc in
        assert_state [30; 18] state
      );

    "append commits min(commit, last index)" >:: test (fun state ->
        let rpc = append ~commit:3 [{ Test_log.index = 1; term = 0; command = 100 }] in
        let _, qupt = handle_rpc qupt rpc in
        let () = assert_state [100] state in
        let rpc = append ~commit:3 ~prev_idx:1 [
            { Test_log.index = 3; term = 0; command = 102 };
            { Test_log.index = 2; term = 0; command = 101 }
          ] in
        let _ = handle_rpc qupt rpc in
        assert_state [102; 101; 100] state
      );

    "convert to candidate if election timeout elapses" >:: (fun _ ->
        let (io, _) = handle_timeout qupt in
        assert_io [Rpc (0, vote ~term:1 0 0)] io
      );

    "decline vote if already voted for self" >:: (fun _ ->
        let _, qupt = handle_timeout qupt in
        let io, _ = handle_rpc qupt (vote ~sender:0 ~term:1 0 0) in
        assert_io [] io
      );

    "grant vote if candidate log up-to-date" >:: (fun _ ->
        let rpc = append ~commit:3 [
            { Test_log.index = 3; term = 0; command = 20 };
            { Test_log.index = 2; term = 0; command = 19 };
            { Test_log.index = 1; term = 0; command = 18 }
          ] in
        let _, qupt = handle_rpc qupt rpc in
        let io, _ = handle_rpc qupt (vote ~sender:0 ~term:1 3 0) in
        assert_io [Rpc (0, response ~term:1 VoteGranted)] io
      );

    "grant vote if already voted for candidate" >:: (fun _ ->
        let _, qupt = handle_rpc qupt (vote ~sender:0 ~term:1 3 0) in
        let io, _ = handle_rpc qupt (vote ~sender:0 ~term:1 3 0) in
        assert_io [Rpc (0, response ~term:1 VoteGranted)] io
      );

    "decline vote if already voted for someone else" >:: (fun _ ->
        let _, qupt = handle_rpc qupt (vote ~sender:0 ~term:1 3 0) in
        let io, _ = handle_rpc qupt (vote ~sender:1 ~term:1 3 0) in
        assert_io [] io
      );

    "decline vote if term < current term" >:: (fun _ ->
        let _, qupt = handle_rpc qupt (append ~term:2 []) in
        let io, _ = handle_rpc qupt (vote ~sender:0 ~term:1 3 0) in
        assert_io [] io
      );
  ]

let candidate_4 =
  let open Test_qupt in
  let _, qupt = init false 0 [0;1;2;3] test_state (Test_log.empty) 1.0 |> handle_timeout in
  "candidate (4 nodes)" >::: [
    "minority does not convert to leader" >:: test (fun _ ->
        let (io, _) = handle_rpc qupt (vote_granted ()) in
        assert_io [] io
      );

    "majority converts to leader" >:: test (fun _ ->
        let (_, qupt) = handle_rpc qupt (vote_granted ~sender:1 ()) in
        let (io, _) = handle_rpc qupt (vote_granted ~sender:2 ()) in
        let expected = [
          Rpc (1, append ~term:1 []);
          Rpc (2, append ~term:1 []);
          Rpc (3, append ~term:1 []);
        ]
        in
        assert_io expected io
      );

    "ignores duplicate votes" >:: test (fun _ ->
        let (_, qupt) = handle_rpc qupt (vote_granted ~sender:1 ()) in
        let (io, _) = handle_rpc qupt (vote_granted ~sender:1 ()) in
        assert_io [] io
      );

    "vote from later term converts to follower" >:: test (fun _ ->
        let io, _ = handle_rpc qupt (vote ~sender:1 ~term:2 0 0) in
        assert_io [Rpc (1, response ~sender:0 ~term:2 VoteGranted)] io
      );

    "append from new leader converts to follower" >:: test (fun _ ->
        let io, _ = handle_rpc qupt (append ~term:1 ~sender:1 []) in
        let response = append_response ~sender:0 ~term:1 ~index:0 true in
        assert_io [Rpc (1, response)] io
      );

    "append from new leader doesn't invalidate existing vote" >:: test (fun _ ->
        let _, qupt = handle_rpc qupt (append ~term:1 ~sender:1 []) in
        let io, _ = handle_rpc qupt (vote ~sender:1 ~term:1 0 0) in
        assert_io [] io
      );

    "timeout starts new election" >:: test (fun _ ->
        let (io, _) = handle_timeout qupt in
        let expected = [
          Rpc (1, vote ~sender:0 ~term:2 0 0);
          Rpc (2, vote ~sender:0 ~term:2 0 0);
          Rpc (3, vote ~sender:0 ~term:2 0 0);
        ]
        in
        assert_io expected io
      );

    "leader heartbeats start from last log index + 1" >:: test (fun _ ->
        let log = [
          { Test_log.index = 3; term = 1; command = 102 };
          { Test_log.index = 2; term = 1; command = 101 };
          { Test_log.index = 1; term = 1; command = 100 };
        ] in

        let _, qupt = handle_rpc qupt (append ~term:1 ~sender:1 log) in
        let _, qupt = handle_timeout qupt in
        let _, qupt = handle_rpc qupt (vote_granted ~sender:1 ()) in
        let io, _ = handle_rpc qupt (vote_granted ~sender:2 ()) in
        let expected = [
          Rpc (1, append ~term:2 ~prev_idx:3 ~prev_term:1 []);
          Rpc (2, append ~term:2 ~prev_idx:3 ~prev_term:1 []);
          Rpc (3, append ~term:2 ~prev_idx:3 ~prev_term:1 []);
        ]
        in
        assert_io expected io
      );
  ]

let all = "qupt" >::: [
    leader_2;
    leader_4;
    follower_2;
    candidate_4;
  ]
