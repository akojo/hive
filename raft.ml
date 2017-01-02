open Core.Std

include Raft_intf

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
    (* Persistent Raft state *)
    term: int;
    voted: string option;
    log: log_entry list;
    (* Volatile Raft state *)
    commit: int;
    last_applied: int;
    (* Volatile Raft state on leaders *)
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
    | Vote of vote
    | AppendSuccess of int
    | AppendFailed
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

  let last_log_index (raft:t) =
    List.hd raft.log |> Option.value_map ~f:(fun e -> e.index) ~default:0

  let handle_timeout raft =
    if raft.role = Leader then
      let io = List.map raft.next_index ~f:(fun (id, idx) ->
          let log = List.take_while raft.log ~f:(fun entry ->
              entry.index >= idx
            )
          in
          let prev_idx, prev_term = if idx > 1 then
              let e = List.find_exn raft.log ~f:(fun e -> e.index = idx - 1) in
              e.index, e.term
            else
              (0,0)
          in
          Rpc (id, {
              sender = raft.self;
              term = raft.term;
              message = Append {
                  prev_idx; prev_term; log; commit = raft.commit
                }
            })
        )
      in
      io, raft
    else
      [], raft

  let handle_rpc (raft:t) { sender; term = _; message } =
    let rpc, raft = match message with
      | Append append ->
        let raft = { raft with log = append.log @ raft.log; commit = append.commit } in
        let resp = {
          sender = raft.self;
          term = raft.term;
          message = AppendSuccess (last_log_index raft)
        } in
        [Rpc (sender, resp)], raft
      | AppendSuccess index ->
        let next_index = List.Assoc.add raft.next_index sender (index + 1) in
        let match_index = List.Assoc.add raft.match_index sender index in
        [], { raft with match_index; next_index; commit = index }
      | AppendFailed
      | Vote _
      | VoteGranted
      | VoteDeclined ->
        [], raft
    in
    let uncommitted = List.filter raft.log ~f:(fun entry ->
        (entry.index > raft.last_applied) && (entry.index <= raft.commit)
      )
    in
    let (io, state) = List.fold_right uncommitted ~init:(rpc, raft.state) ~f:(fun entry (io, state)  ->
        let (response, state) = State.apply state entry.command in
        (Response (entry.index, response)) :: io, state
      ) in
    let last_applied = last_log_index raft in
    io, { raft with state; last_applied }

  let handle_command raft command =
    let index = (last_log_index raft) + 1 in
    let log = { index; term = raft.term; command } :: raft.log in
    let io, raft = handle_timeout { raft with log } in
    index, io, raft
end
