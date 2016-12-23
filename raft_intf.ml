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

module type S =
sig
  type t
  type id
  type command
  type response
  type state

  type role =
    | Leader
    | Follower
    | Candidate

  type log_entry = {
    index: int;
    term: int;
    command: command
  } [@@deriving sexp]

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
    | AppendSuccess
    | AppendFailed
    | VoteGranted
    | VoteDeclined
  [@@deriving sexp]

  type rpc = {
    sender: id;
    term: int;
    message: message
  } [@@deriving sexp]

  type io =
    | Rpc of id * rpc
    | Response of int * response
  [@@deriving sexp]

  val init: id -> role -> id list -> state -> t
  val handle_timeout: t -> io list * t
  val handle_rpc: t -> rpc -> io list * t
  val handle_command: t -> command -> int * io list * t
end