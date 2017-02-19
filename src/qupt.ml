open Core.Std

module type State_machine =
sig
  type t
  type command [@@deriving sexp]
  type response [@@deriving sexp]

  val apply: t -> command -> response * t
end

module type Id =
sig
  type t [@@deriving sexp]
end

module type Log =
sig
  type command
  type entry = {
    index: int;
    term: int;
    command: command;
  } [@@deriving sexp]
  type t

  val is_empty: t -> bool
  val find: t -> index:int -> entry option
  val last: t -> entry option
  val last_before: t -> index:int -> entry option
  val from: t -> index:int -> entry list
  val range: t -> first:int -> last:int -> entry list
  val append: t -> entry -> t
  val append_after: t -> index:int -> entries:entry list -> t
end

module Make
    (State : State_machine)
    (Id: Id)
    (Log: sig
       include Log with type command := State.command
     end) =
struct
  type qupt = {
    self: Id.t;
    configuration: Id.t list;
    state: State.t;
    heartbeat: float;
    (* Persistent Qupt state *)
    term: int;
    voted: Id.t option;
    log: Log.t;
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

  type append = {
    prev_idx: int;
    prev_term: int;
    commit: int;
    entries: Log.entry list
  } [@@deriving sexp]

  type append_response = {
    success: bool;
    index: int;
  } [@@deriving sexp]

  type message =
    (* previous index, previous term, commit, log *)
    | Append of append
    | AppendResponse of append_response
    | Vote of vote
    | VoteGranted
  [@@deriving sexp]

  type rpc = {
    sender: Id.t;
    term: int;
    message: message
  }
  [@@deriving sexp]

  type io =
    | Rpc of Id.t * rpc
    | Response of int * State.response
  [@@deriving sexp]

  module Common = struct
    let init self configuration state log heartbeat =
      {
        self;
        configuration;
        state;
        heartbeat;
        term = 0;
        voted = None;
        log = log;
        commit = 0;
        last_applied = 0;
      }

    let last_log_entry log =
      match Log.last log with
      | Some entry -> entry.Log.index, entry.Log.term
      | None -> 0, 0

    let make_rpc qupt message = {
      sender = qupt.self;
      term = qupt.term;
      message
    }

    let apply_committed qupt =
      let io, state =
        Log.range qupt.log ~first:qupt.last_applied ~last:qupt.commit
        |> List.fold_right
          ~init:([], qupt.state)
          ~f:(fun entry (io, state) ->
              let response, state = State.apply state entry.Log.command in
              (Response (entry.Log.index, response)) :: io, state
            )
      in
      io, { qupt with state; last_applied = qupt.commit }

    let handle_append (qupt:qupt) rpc append =
      let append_entries qupt entries =
        let log = Log.append_after ~index:append.prev_idx ~entries qupt.log in
        let last_index, _ = last_log_entry log in
        let qupt = { qupt with log; commit = min last_index append.commit } in
        AppendResponse { success = true; index = last_index}, qupt
      in
      if rpc.term < qupt.term then
        [Rpc (rpc.sender, make_rpc qupt (AppendResponse { success = false; index = 0 }))], qupt
      else
        let message, qupt =
          match Log.find ~index:append.prev_idx qupt.log with
          | Some entry when entry.Log.term = append.prev_term ->
            append_entries qupt append.entries
          | None when Log.is_empty qupt.log ->
            append_entries qupt append.entries
          | _ -> AppendResponse { success = false; index = qupt.commit }, qupt
        in
        let io, qupt = apply_committed qupt in
        Rpc (rpc.sender, make_rpc qupt message) :: io, qupt

    let start_election (qupt:qupt) =
      let qupt = { qupt with term = qupt.term + 1; voted = Some qupt.self } in
      let others = List.filter qupt.configuration ~f:((<>) qupt.self) in
      let io = List.map others ~f:(fun id ->
          let last_idx, last_term = last_log_entry qupt.log in
          Rpc (id, make_rpc qupt (Vote { last_idx; last_term }))
        )
      in
      io, (Candidate { qupt; votes = [] })
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
      let last_log = Log.from ~index qupt.log in
      let prev_idx, prev_term = match Log.last_before ~index qupt.log with
        | Some entry -> entry.Log.index, entry.Log.term
        | None -> 0, 0
      in
      Common.make_rpc qupt (Append {
          prev_idx;
          prev_term;
          commit = qupt.commit;
          entries = last_log
        })

    let commit_if_majority leader index =
      let matches = List.count leader.match_index ~f:(fun (_, i) -> i >= index) in
      let qupt = leader.qupt in
      let nodes = List.length qupt.configuration in
      match Log.find ~index qupt.log with
      | Some entry when entry.term = qupt.term && (matches >= nodes / 2) ->
        { leader with qupt = { qupt with commit = index } }
      | _ ->
        leader

    let handle_timeout leader =
      let io = List.map leader.next_index ~f:(fun (id, index) ->
          Rpc (id, send_append leader.qupt index)
        )
      in
      io, Leader leader

    let handle_append leader rpc append =
      let next_index = List.Assoc.add leader.next_index rpc.sender (append.index + 1) in
      let match_index = List.Assoc.add leader.match_index rpc.sender append.index in
      let leader = { leader with match_index; next_index } in
      if append.success then
        let leader = commit_if_majority leader append.index in
        let io, qupt = Common.apply_committed leader.qupt in
        io, { leader with qupt }
      else
        [Rpc (rpc.sender, send_append leader.qupt (append.index + 1))], leader

    let handle_rpc leader rpc =
      let io, leader = match rpc.message with
        | AppendResponse append -> handle_append leader rpc append
        | Vote _
        | Append _
        | VoteGranted -> [], leader
      in
      io, Leader leader

    let handle_command (leader:leader_state) command =
      let last_index, _ = Common.last_log_entry leader.qupt.log in
      let new_index = last_index + 1 in
      let entry = { Log.index = new_index; term = leader.qupt.term; command = command } in
      let qupt = { leader.qupt with log = Log.append leader.qupt.log entry } in
      let io, role = handle_timeout { leader with qupt } in
      new_index, io, role
  end

  module Follower = struct
    let init qupt = qupt

    let handle_vote qupt rpc vote =
      let last_idx, last_term = Common.last_log_entry qupt.log in
      let valid_vote =
        rpc.term >= qupt.term
        && Option.value_map qupt.voted ~f:((=) rpc.sender) ~default:true
        && vote.last_idx >= last_idx
        && vote.last_term >= last_term
      in
      if valid_vote then
        let qupt = { qupt with voted = Some rpc.sender } in
        [Rpc (rpc.sender, Common.make_rpc qupt VoteGranted)], qupt
      else
        [], qupt

    let handle_rpc (qupt:qupt) rpc =
      let io, qupt = match rpc.message with
        | Append append -> Common.handle_append qupt rpc append
        | Vote vote -> handle_vote qupt rpc vote
        | AppendResponse _
        | VoteGranted -> [], qupt
      in
      io, Follower qupt
  end

  module Candidate = struct
    let handle_vote_granted candidate rpc =
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

    let handle_rpc candidate rpc =
      match rpc.message with
      | Append append ->
        let io, qupt = Common.handle_append candidate.qupt rpc append in
        io, Follower qupt
      | VoteGranted -> handle_vote_granted candidate rpc
      | Vote _
      | AppendResponse _ -> [], Candidate candidate
  end

  let init leader self configuration state log heartbeat =
    let qupt = Common.init self configuration state log heartbeat in
    if leader then Leader (Leader.init qupt)
    else Follower (Follower.init qupt)

  let timeout role =
    let timeout = match role with
      | Leader leader -> leader.qupt.heartbeat
      | Follower qupt -> 10.0 *. qupt.heartbeat
      | Candidate candidate -> 10.0 *. candidate.qupt.heartbeat
    in
    timeout +. Random.float timeout

  let handle_timeout role =
    match role with
    | Leader leader  -> Leader.handle_timeout leader
    | Follower qupt -> Common.start_election qupt
    | Candidate candidate -> Common.start_election candidate.qupt

  let handle_rpc role message =
    let maybe_convert_to_follower role =
      let qupt = match role with
        | Leader leader -> leader.qupt
        | Follower qupt -> qupt
        | Candidate candidate -> candidate.qupt
      in
      if message.term > qupt.term then
        Follower { qupt with term = message.term; voted = None }
      else
        role
    in
    match maybe_convert_to_follower role with
    | Leader leader -> Leader.handle_rpc leader message
    | Follower qupt -> Follower.handle_rpc qupt message
    | Candidate candidate -> Candidate.handle_rpc candidate message

  let handle_command role command =
    match role with
    | Leader leader -> Leader.handle_command leader command
    | Follower _
    | Candidate _ -> failwith "command sent to non-leader"
end
