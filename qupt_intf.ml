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

  type log_entry = int * int * command [@@deriving sexp]

  type vote = {
    last_idx: int;
    last_term: int;
  } [@@deriving sexp]

  type message =
    (* previous index, previous term, commit, log *)
    | Append of int * int * int * log_entry list
    | AppendResponse of bool * int
    | Vote of vote
    | VoteGranted
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

  val init: bool -> id -> id list -> state -> t
  val handle_timeout: t -> io list * t
  val handle_rpc: t -> rpc -> io list * t
  val handle_command: t -> command -> int * io list * t
end
