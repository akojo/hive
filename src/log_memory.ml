open Core.Std

include Log_intf

module Make (Command: Command) :
  Log with type command = Command.command =
struct
  type command = Command.command
  type entry = {
    index: int;
    term: int;
    command: Command.command;
  } [@@deriving sexp]
  type t = entry list

  let create () = []

  let close _ = ()

  let is_empty = function
    | [] -> true
    | _ -> false

  let find log ~index =
    List.find log ~f:(fun entry -> entry.index = index)

  let from log ~index =
    List.take_while log ~f:(fun entry -> entry.index >= index)

  let append log entry = entry :: log

  let append_after log ~index ~entries =
    let current_log = List.drop_while log ~f:(fun entry -> entry.index > index) in
    entries @ current_log

  let range log ~first ~last =
    List.drop_while log ~f:(fun entry -> entry.index > last)
    |> List.take_while ~f:(fun entry -> entry.index > first)

  let last = function
    | e :: _ -> Some e
    | [] -> None

  let last_before log ~index =
    let _, old = List.split_while log ~f:(fun entry -> entry.index >= index) in
    last old
end
