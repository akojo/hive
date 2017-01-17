open Core.Std

include Qupt_intf

module Make (State : State_machine) (Id: Id)
  : S with type state := State.t
       and type command := State.command
       and type response := State.response
       and type id := Id.t =
struct
  type role =
    | Leader
    | Follower
    | Candidate

  type log_entry = int * int * State.command [@@deriving sexp]

  type t = {
    self: Id.t;
    role: role;
    configuration: Id.t list;
    state: State.t;
    (* Persistent Qupt state *)
    term: int;
    voted: string option;
    log: log_entry list;
    (* Volatile Qupt state *)
    commit: int;
    last_applied: int;
    (* Volatile Qupt state on leaders *)
    next_index: (Id.t * int) list;
    match_index: (Id.t * int) list;
  }

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

  let init self role configuration state =
    let followers = List.filter configuration ~f:((<>) self) in
    {
      self;
      role;
      configuration;
      state;
      term = 0;
      voted = None;
      log = [];
      commit = 0;
      last_applied = 0;
      next_index = List.map followers ~f:(fun id -> (id, 1));
      match_index = List.map followers ~f:(fun id -> (id, 0));
    }

  let last_log_index log =
    match log with
    | (index, _, _) :: _ -> index
    | [] -> 0

  let find_index (log:log_entry list) index term =
    List.drop_while log ~f:(fun (i, t, _) -> i <> index || t  <> term)

  let handle_timeout qupt =
    if qupt.role = Leader then
      let io = List.map qupt.next_index ~f:(fun (id, idx) ->
          let log, prev = List.split_while qupt.log ~f:(fun (i, _, _) -> i >= idx) in
          let prev_idx, prev_term = match List.hd prev with
            | Some (index, term, _) ->index, term
            | None -> 0,0
          in
          Rpc (id, {
              sender = qupt.self;
              term = qupt.term;
              message = Append (prev_idx, prev_term, qupt.commit, log)
            })
        )
      in
      io, qupt
    else
      [], qupt

  let handle_append (qupt:t) prev_idx prev_term commit log =
    let current_log = find_index qupt.log prev_idx prev_term in
    let message, qupt = if prev_idx <> 0 && List.is_empty current_log then
        AppendResponse (false, qupt.commit), qupt
      else
        let log = log @ current_log in
        let last = last_log_index log in
        let qupt = { qupt with log; commit = min last commit } in
        AppendResponse (true, last), qupt
    in
    { sender = qupt.self; term = qupt.term; message }, qupt

  let commit_if_majority (qupt:t) index =
    let in_log = List.exists qupt.log ~f:(fun (i, t, _) -> i = index && t = qupt.term) in
    let matches = List.count qupt.match_index ~f:(fun (_, i) -> i >= index) in
    let nodes = List.length qupt.configuration in
    if in_log && matches >= nodes / 2 then
      { qupt with commit = index }
    else
      qupt

  let handle_rpc (qupt:t) { sender; term; message } =
    let qupt = if term > qupt.term then { qupt with term} else qupt in
    let rpc, qupt = match message with
      | Append (prev_idx, prev_term, commit, log) ->
        if term < qupt.term then
          let resp = { sender = qupt.self; term = qupt.term; message = (AppendResponse (false, 0)) } in
          [Rpc (sender, resp)], qupt
        else
          let resp, qupt = handle_append qupt prev_idx prev_term commit log in
          [Rpc (sender, resp)], qupt
      | AppendResponse (success, index) ->
        let next_index = List.Assoc.add qupt.next_index sender (index + 1) in
        let match_index = List.Assoc.add qupt.match_index sender index in
        if success then
          [], commit_if_majority { qupt with match_index; next_index } index
        else
          handle_timeout { qupt with match_index; next_index }
      | Vote _
      | VoteGranted
      | VoteDeclined ->
        [], qupt
    in
    let uncommitted = List.filter qupt.log ~f:(fun (index, _, _) ->
        (index > qupt.last_applied) && (index <= qupt.commit)
      )
    in
    let io, state = List.fold_right uncommitted ~init:(rpc, qupt.state) ~f:(fun (index, _, command) (io, state)  ->
        let response, state = State.apply state command in
        (Response (index, response)) :: io, state
      ) in
    io, { qupt with state; last_applied = qupt.commit }

  let handle_command (qupt:t) command =
    let index = (last_log_index qupt.log) + 1 in
    let log = (index, qupt.term, command) :: qupt.log in
    let io, qupt = handle_timeout { qupt with log } in
    index, io, qupt
end
