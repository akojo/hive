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

  type log_entry = {
    index: int;
    term: int;
    command: State.command
  } [@@deriving sexp]

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

  type append = {
    prev_idx: int;
    prev_term: int;
    log: log_entry list;
    commit: int
  } [@@deriving sexp]

  type vote = {
    last_idx: int;
    last_term: int;
  } [@@deriving sexp]

  type message =
    | Append of append
    | AppendSuccess of int
    | AppendFailed of int
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
    List.hd log |> Option.value_map ~f:(fun e -> e.index) ~default:0

  let find_index (log:log_entry list) term idx =
    List.drop_while log ~f:(fun el -> el.term <> term || el.index <> idx)

  let handle_timeout qupt =
    if qupt.role = Leader then
      let io = List.map qupt.next_index ~f:(fun (id, idx) ->
          let log, prev = List.split_while qupt.log ~f:(fun e -> e.index >= idx) in
          let prev_idx, prev_term = match List.hd prev with
            | Some e -> e.index, e.term
            | None -> 0,0
          in
          Rpc (id, {
              sender = qupt.self;
              term = qupt.term;
              message = Append { prev_idx; prev_term; log; commit = qupt.commit }
            })
        )
      in
      io, qupt
    else
      [], qupt

  let handle_append (qupt:t) append =
    let log = find_index qupt.log append.prev_term append.prev_idx in
    let message, qupt = if append.prev_idx <> 0 && List.is_empty log then
        (AppendFailed qupt.commit), qupt
      else
        let log = append.log @ log in
        let last = last_log_index log in
        let qupt = { qupt with log; commit = min last append.commit } in
        AppendSuccess (last_log_index qupt.log), qupt
    in
    { sender = qupt.self; term = qupt.term; message }, qupt

  let commit_if_majority (qupt:t) index =
    let in_log = List.exists qupt.log ~f:(fun e -> e.index = index && e.term = qupt.term) in
    let matches = List.count qupt.match_index ~f:(fun (_, i) -> i >= index) in
    let nodes = List.length qupt.configuration in
    if in_log && matches >= nodes / 2 then
      { qupt with commit = index }
    else
      qupt

  let handle_rpc (qupt:t) { sender; term; message } =
    let qupt = if term > qupt.term then { qupt with term} else qupt in
    let rpc, qupt = match message with
      | Append append ->
        if term < qupt.term then
          let resp = { sender = qupt.self; term = qupt.term; message = (AppendFailed 0) } in
          [Rpc (sender, resp)], qupt
        else
          let resp, qupt = handle_append qupt append in
          [Rpc (sender, resp)], qupt
      | AppendSuccess index ->
        let next_index = List.Assoc.add qupt.next_index sender (index + 1) in
        let match_index = List.Assoc.add qupt.match_index sender index in
        [], commit_if_majority { qupt with match_index; next_index } index
      | AppendFailed index ->
        let next_index = List.Assoc.add qupt.next_index sender (index + 1) in
        let match_index = List.Assoc.add qupt.match_index sender index in
        handle_timeout { qupt with match_index; next_index }
      | Vote _
      | VoteGranted
      | VoteDeclined ->
        [], qupt
    in
    let uncommitted = List.filter qupt.log ~f:(fun entry ->
        (entry.index > qupt.last_applied) && (entry.index <= qupt.commit)
      )
    in
    let io, state = List.fold_right uncommitted ~init:(rpc, qupt.state) ~f:(fun entry (io, state)  ->
        let response, state = State.apply state entry.command in
        (Response (entry.index, response)) :: io, state
      ) in
    io, { qupt with state; last_applied = qupt.commit }

  let handle_command (qupt:t) command =
    let index = (last_log_index qupt.log) + 1 in
    let log = { index; term = qupt.term; command } :: qupt.log in
    let io, qupt = handle_timeout { qupt with log } in
    index, io, qupt
end
