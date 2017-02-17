open Core.Std

type t = string String.Map.t

type command =
  | Set of string * string
  | Get of string
[@@deriving sexp]

type response = {
  key: string;
  value: string option
} [@@deriving sexp]

let empty = String.Map.empty

let apply store command =
  match command with
  | Set (key, value) ->
    { key; value = Some value }, Map.add ~key ~data:value store
  | (Get key) ->
    { key; value = Map.find store key }, store
