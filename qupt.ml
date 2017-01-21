open Core.Std

include Qupt_intf

module Make (State : State_machine) (Id: Id)
  : S with type state := State.t
       and type command := State.command
       and type response := State.response
       and type id := Id.t =
struct
  type log_entry = int * int * State.command [@@deriving sexp]

  type qupt = {
    self: Id.t;
    configuration: Id.t list;
    state: State.t;
    (* Persistent Qupt state *)
    term: int;
    voted: Id.t option;
    log: log_entry list;
    (* Volatile Qupt state *)
    commit: int;
    last_applied: int;
  }

  type leader_state = {
    qupt: qupt;
    next_index: (Id.t * int) list;
    match_index: (Id.t * int) list;
  }

  type candidate_state = {
    qupt: qupt;
    votes: Id.t list;
  }

  type t =
    | Leader of leader_state
    | Follower of qupt
    | Candidate of candidate_state

  type vote = {
    last_idx: int;
    last_term: int;
  } [@@deriving sexp]

  type message =
    | Append of int * int * int * log_entry list
    | AppendResponse of bool * int
    | Vote of vote
    | VoteGranted
  [@@deriving sexp]

  type rpc = {
    sender: Id.t;
    term: int;
    message: message
  } [@@deriving sexp]

  type io =
    | Rpc of Id.t * rpc
    | Response of int * State.response
  [@@deriving sexp]

  module Common = struct
    let init self configuration state =
      {
        self;
        configuration;
        state;
        term = 0;
        voted = None;
        log = [];
        commit = 0;
        last_applied = 0;
      }

    let last_log_entry = function
      | (index, term, _) :: _ -> index, term
      | [] -> 0, 0

    let find_index (log:log_entry list) index term =
      List.drop_while log ~f:(fun (i, t, _) -> i <> index || t  <> term)

    let make_rpc qupt message = {
      sender = qupt.self;
      term = qupt.term;
      message
    }

    let apply_committed qupt =
      let io, state =
        List.drop_while qupt.log ~f:(fun (index, _, _) -> index > qupt.commit)
        |> List.take_while ~f:(fun (index, _, _) -> index > qupt.last_applied)
        |> List.fold_right
          ~init:([], qupt.state)
          ~f:(fun (index, _, command) (io, state) ->
              let response, state = State.apply state command in
              (Response (index, response)) :: io, state
            )
      in
      io, { qupt with state; last_applied = qupt.commit }

    let handle_append (qupt:qupt) rpc prev_idx prev_term commit log =
      if rpc.term < qupt.term then
        [Rpc (rpc.sender, make_rpc qupt (AppendResponse (false, 0)))], qupt
      else
        let current_log = find_index qupt.log prev_idx prev_term in
        let message, qupt = if prev_idx <> 0 && List.is_empty current_log then
            AppendResponse (false, qupt.commit), qupt
          else
            let log = log @ current_log in
            let last, _ = last_log_entry log in
            let qupt = { qupt with log; commit = min last commit } in
            AppendResponse (true, last), qupt
        in
        let response = make_rpc qupt message in
        let io, qupt = apply_committed qupt in
        Rpc (rpc.sender, response) :: io, qupt

    let start_election (qupt:qupt) =
      let qupt = { qupt with term = qupt.term + 1; voted = Some qupt.self } in
      let others = List.filter qupt.configuration ~f:((<>) qupt.self) in
      let io = List.map others ~f:(fun id ->
          let last_idx, last_term = last_log_entry qupt.log in
          Rpc (id, make_rpc qupt (Vote { last_idx; last_term }))
        )
      in
      io, (Candidate { qupt; votes = [] })

    let handle_vote qupt rpc vote =
      let last_idx, last_term = last_log_entry qupt.log in
      let valid_vote =
        rpc.term >= qupt.term
        && Option.value_map qupt.voted ~f:((=) rpc.sender) ~default:true
        && vote.last_idx >= last_idx
        && vote.last_term >= last_term
      in
      if valid_vote then [Rpc (rpc.sender, make_rpc qupt VoteGranted)] else []
  end

  module Leader = struct
    let init qupt =
      let followers = List.filter qupt.configuration ~f:((<>) qupt.self) in
      let index, _ = Common.last_log_entry qupt.log in
      {
        qupt;
        next_index = List.map followers ~f:(fun id -> (id, index + 1));
        match_index = List.map followers ~f:(fun id -> (id, 0));
      }

    let send_append qupt index =
      let log, prev = List.split_while qupt.log ~f:(fun (i, _, _) -> i >= index) in
      let prev_idx, prev_term = Common.last_log_entry prev in
      Common.make_rpc qupt (Append (prev_idx, prev_term, qupt.commit, log))

    let commit_if_majority leader index =
      let matches = List.count leader.match_index ~f:(fun (_, i) -> i >= index) in
      let qupt = leader.qupt in
      let in_log = List.exists qupt.log ~f:(fun (i, t, _) -> i = index && t = qupt.term) in
      let nodes = List.length qupt.configuration in
      if in_log && matches >= nodes / 2 then
        { leader with qupt = { qupt with commit = index } }
      else
        leader

    let handle_timeout leader =
      let io = List.map leader.next_index ~f:(fun (id, index) ->
          Rpc (id, send_append leader.qupt index)
        )
      in
      io, Leader leader

    let handle_rpc leader rpc =
      let io, leader = match rpc.message with
        | AppendResponse (success, index) ->
          let next_index = List.Assoc.add leader.next_index rpc.sender (index + 1) in
          let match_index = List.Assoc.add leader.match_index rpc.sender index in
          let leader = { leader with match_index; next_index } in
          if success then
            let leader = commit_if_majority leader index in
            let io, qupt = Common.apply_committed leader.qupt in
            io, { leader with qupt }
          else
            [Rpc (rpc.sender, send_append leader.qupt (index + 1))], leader
        | Vote vote ->
          Common.handle_vote leader.qupt rpc vote, leader
        | Append _
        | VoteGranted ->
          [], leader
      in
      io, Leader leader

    let handle_command (leader:leader_state) command =
      let index = fst (Common.last_log_entry leader.qupt.log) + 1 in
      let log = (index, leader.qupt.term, command) :: leader.qupt.log in
      let qupt = { leader.qupt with log } in
      let io, role = handle_timeout { leader with qupt } in
      index, io, role
  end

  module Follower = struct
    let init qupt = qupt

    let handle_rpc (qupt:qupt) rpc =
      let io, qupt = match rpc.message with
        | Append (prev_idx, prev_term, commit, log) ->
          Common.handle_append qupt rpc prev_idx prev_term commit log
        | Vote vote ->
          Common.handle_vote qupt rpc vote, qupt
        | AppendResponse _
        | VoteGranted ->
          [], qupt
      in
      io, Follower qupt
  end

  module Candidate = struct
    let handle_rpc candidate rpc =
      match rpc.message with
      | Append (prev_idx, prev_term, commit, log) ->
        let io, qupt = Common.handle_append candidate.qupt rpc prev_idx prev_term commit log in
        io, Follower qupt
      | VoteGranted ->
        let candidate = if List.mem candidate.votes rpc.sender then
            candidate
          else
            { candidate with votes = rpc.sender :: candidate.votes }
        in
        let votes = List.length candidate.votes in
        let nodes = List.length candidate.qupt.configuration in
        if votes >= nodes / 2 then
          Leader.handle_timeout (Leader.init candidate.qupt)
        else
          [], Candidate candidate
      | Vote _
      | AppendResponse _ ->
        [], Candidate candidate
  end

  let init leader self configuration state =
    let qupt = Common.init self configuration state in
    if leader then Leader (Leader.init qupt)
    else Follower (Follower.init qupt)

  let timeout role =
    let base = 1.0 in
    let timeout = match role with
      | Leader _ -> base
      | Follower _
      | Candidate _ -> 10.0 *. base
    in
    timeout +. Random.float timeout

  let handle_timeout role =
    match role with
    | Leader leader  -> Leader.handle_timeout leader
    | Follower qupt -> Common.start_election qupt
    | Candidate candidate -> Common.start_election candidate.qupt

  let handle_rpc role message =
    let qupt_of = function
      | Leader leader -> leader.qupt
      | Follower qupt -> qupt
      | Candidate candidate -> candidate.qupt
    in
    let maybe_convert (qupt:qupt) =
      if message.term > qupt.term then
        Follower { qupt with term = message.term }
      else
        role
    in
    match maybe_convert (qupt_of role) with
    | Leader leader -> Leader.handle_rpc leader message
    | Follower qupt -> Follower.handle_rpc qupt message
    | Candidate candidate -> Candidate.handle_rpc candidate message

  let handle_command role command =
    match role with
    | Leader leader -> Leader.handle_command leader command
    | Follower _
    | Candidate _ -> failwith "command sent to non-leader"
end
