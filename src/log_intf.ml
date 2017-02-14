module type Command =
sig
  type command [@@deriving sexp]
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

  val create: unit -> t
  val close: t -> unit
  val is_empty: t -> bool
  val find: t -> index:int -> entry option
  val last: t -> entry option
  val last_before: t -> index:int -> entry option
  val from: t -> index:int -> entry list
  val range: t -> first:int -> last:int -> entry list
  val append: t -> entry -> t
  val append_after: t -> index:int -> entries:entry list -> t
end
