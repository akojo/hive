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
    votes: int
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
    | VoteDeclined
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

  let common_init self configuration state =
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

  let leader_init qupt =
    let followers = List.filter qupt.configuration ~f:((<>) qupt.self) in
    Leader {
      qupt;
      next_index = List.map followers ~f:(fun id -> (id, 1));
      match_index = List.map followers ~f:(fun id -> (id, 0));
    }

  let follower_init qupt =
    Follower qupt

  let init leader self configuration state =
    let qupt = common_init self configuration state in
    if leader then leader_init qupt
    else follower_init qupt

  let last_log_entry = function
    | (index, term, _) :: _ -> index, term
    | [] -> 0, 0

  let last_log_index = function
    | (index, _, _) :: _ -> index
    | [] -> 0

  let find_index (log:log_entry list) index term =
    List.drop_while log ~f:(fun (i, t, _) -> i <> index || t  <> term)

  let make_rpc qupt message = { sender = qupt.self; term = qupt.term; message }

  let send_append qupt index =
    let log, prev = List.split_while qupt.log ~f:(fun (i, _, _) -> i >= index) in
    let prev_idx, prev_term = last_log_entry prev in
    make_rpc qupt (Append (prev_idx, prev_term, qupt.commit, log))

  let handle_timeout role =
    match role with
    | Leader leader  ->
      let io = List.map leader.next_index ~f:(fun (id, index) ->
          Rpc (id, send_append leader.qupt index)
        )
      in
      io, role
    | Follower qupt ->
      let qupt = { qupt with term = qupt.term + 1; voted = Some qupt.self } in
      let others = List.filter qupt.configuration ~f:((<>) qupt.self) in
      let io = List.map others ~f:(fun id ->
          let last_idx, last_term = last_log_entry qupt.log in
          Rpc (id, make_rpc qupt (Vote { last_idx; last_term }))
        )
      in
      io, (Candidate { qupt; votes = 0 })
    | Candidate _ ->
      [], role

  let handle_append qupt prev_idx prev_term commit log =
    let current_log = find_index qupt.log prev_idx prev_term in
    let message, qupt = if prev_idx <> 0 && List.is_empty current_log then
        AppendResponse (false, qupt.commit), qupt
      else
        let log = log @ current_log in
        let last = last_log_index log in
        let qupt = { qupt with log; commit = min last commit } in
        AppendResponse (true, last), qupt
    in
    make_rpc qupt message, qupt

  let commit_if_majority leader index =
    let matches = List.count leader.match_index ~f:(fun (_, i) -> i >= index) in
    let qupt = leader.qupt in
    let in_log = List.exists qupt.log ~f:(fun (i, t, _) -> i = index && t = qupt.term) in
    let nodes = List.length qupt.configuration in
    if in_log && matches >= nodes / 2 then
      { leader with qupt = { qupt with commit = index } }
    else
      leader

  let handle_vote qupt sender term vote =
    let last_idx, last_term = last_log_entry qupt.log in
    let valid_vote =
      term >= qupt.term
      && Option.value_map qupt.voted ~f:((=) sender) ~default:true
      && vote.last_idx >= last_idx
      && vote.last_term >= last_term
    in
    let message = if valid_vote then VoteGranted else VoteDeclined in
    [Rpc (sender, make_rpc qupt message)]

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

  let maybe_convert_to_follower role term =
    let convert (qupt:qupt) =
      if term > qupt.term then Follower { qupt with term } else role
    in
    match role with
    | Leader leader -> convert leader.qupt
    | Follower qupt -> convert qupt
    | Candidate candidate -> convert candidate.qupt

  let handle_rpc role { sender; term; message } =
    let role = maybe_convert_to_follower role term in
    match role with
    | Leader leader ->
      let rpc, leader = match message with
        | AppendResponse (success, index) ->
          let next_index = List.Assoc.add leader.next_index sender (index + 1) in
          let match_index = List.Assoc.add leader.match_index sender index in
          let leader = { leader with match_index; next_index } in
          if success then
            let leader = commit_if_majority leader index in
            [], leader
          else
            [Rpc (sender, send_append leader.qupt (index + 1))], leader
        | Vote vote ->
          let io = handle_vote leader.qupt sender term vote in
          io, leader
        | Append _
        | VoteGranted
        | VoteDeclined ->
          [], leader
      in
      let io, qupt = apply_committed leader.qupt in
      io @ rpc, Leader { leader with qupt }
    | Follower qupt ->
      let rpc, qupt = match message with
        | Append (prev_idx, prev_term, commit, log) ->
          if term < qupt.term then
            [Rpc (sender, make_rpc qupt (AppendResponse (false, 0)))], qupt
          else
            let resp, qupt = handle_append qupt prev_idx prev_term commit log in
            [Rpc (sender, resp)], qupt
        | Vote vote ->
          let io = handle_vote qupt sender term vote in
          io, qupt
        | AppendResponse _
        | VoteGranted
        | VoteDeclined ->
          [], qupt
      in
      let io, qupt = apply_committed qupt in
      io @ rpc, Follower qupt
    | Candidate candidate ->
      let rpc, qupt = match message with
        | Append (prev_idx, prev_term, commit, log) ->
          let qupt = candidate.qupt in
          if term < qupt.term then
            [Rpc (sender, make_rpc qupt (AppendResponse (false, 0)))], qupt
          else
            let resp, qupt = handle_append qupt prev_idx prev_term commit log in
            [Rpc (sender, resp)], qupt
        | Vote vote ->
          let io = handle_vote candidate.qupt sender term vote in
          io, candidate.qupt
        | AppendResponse _
        | VoteGranted
        | VoteDeclined ->
          [], candidate.qupt
      in
      rpc, Candidate { candidate with qupt }

  let handle_command role command =
    match role with
    | Leader leader ->
      let index = (last_log_index leader.qupt.log) + 1 in
      let log = (index, leader.qupt.term, command) :: leader.qupt.log in
      let qupt = { leader.qupt with log } in
      let io, qupt = handle_timeout ( Leader { leader with qupt }) in
      index, io, qupt
    | Follower _
    | Candidate _ ->
      failwith "command sent to non-leader"
end
