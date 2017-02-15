open Core.Std

include Log_intf

module Make (Command: Command) =
struct
  type command = Command.command
  type entry = {
    index: int;
    term: int;
    command: Command.command;
  } [@@deriving sexp]

  type t = Sqlite3.db

  let check db expected rc =
    if rc <> expected then
      let code = Sqlite3.Rc.to_string rc in
      let message = Sqlite3.errmsg db in
      failwith (code ^ ": " ^ message)

  let create () =
    let open Sqlite3 in
    let db = db_open ":memory:" in
    let () = exec db
        "CREATE TABLE IF NOT EXISTS log ( \
         idx INTEGER PRIMARY KEY, \
         term INTEGER NOT NULL, \
         command TEXT NOT NULL)" |> check db Sqlite3.Rc.OK in
    db

  let close db =
    Sqlite3.db_close db |> ignore

  let to_int = function
    | Sqlite3.Data.INT value -> Int64.to_int_exn value
    | col -> failwith ("to_int: " ^ Sqlite3.Data.to_string_debug col)

  let read_entry stmt =
    let to_command = function
      | Sqlite3.Data.TEXT value ->
        value |> Sexp.of_string |> Command.command_of_sexp
      | col -> failwith ("to_command: " ^ Sqlite3.Data.to_string_debug col)
    in
    {
      index = Sqlite3.column stmt 0 |> to_int;
      term = Sqlite3.column stmt 1 |> to_int;
      command = Sqlite3.column stmt 2 |> to_command
    }

  let bind_int index value stmt =
    let _ = Sqlite3.(bind stmt index (Data.INT (Int64.of_int value))) in
    stmt

  let bind_command index command stmt =
    let command_string = command |> Command.sexp_of_command |> Sexp.to_string in
    let _ = Sqlite3.(bind stmt index (Data.TEXT command_string)) in
    stmt

  let bind_entry entry stmt =
    bind_int 1 entry.index stmt
    |> bind_int 2 entry.term
    |> bind_command 3 entry.command

  let is_empty db =
    let query = "SELECT COUNT(*) FROM log" in
    let stmt = Sqlite3.prepare db query in
    let () = Sqlite3.step stmt |> check db Sqlite3.Rc.ROW in
    (Sqlite3.column stmt 0 |> to_int) = 0

  let find_one stmt =
    let open Sqlite3 in
    let _ = step stmt in
    if data_count stmt = 3 then Some (read_entry stmt) else None

  let find db ~index =
    let query = "SELECT idx, term, command FROM log WHERE idx = ?" in
    Sqlite3.prepare db query |> bind_int 1 index |> find_one

  let rec read_entries stmt result =
    match Sqlite3.step stmt with
    | Sqlite3.Rc.DONE -> result
    | Sqlite3.Rc.ROW -> read_entries stmt (read_entry stmt :: result)
    | rc -> failwith ("read_entries: " ^ Sqlite3.Rc.to_string rc)

  let from db ~index =
    let query =
      "SELECT idx, term, command \
       FROM log \
       WHERE idx >= ? \
       ORDER BY idx ASC" in
    let stmt = Sqlite3.prepare db query |> bind_int 1 index in
    read_entries stmt []

  let range db ~first ~last =
    let query =
      "SELECT idx, term, command \
       FROM log \
       WHERE idx > ? AND idx <= ? \
       ORDER BY idx ASC" in
    let stmt = Sqlite3.prepare db query
               |> bind_int 1 first |> bind_int 2 last in
    read_entries stmt []

  let append db entry =
    let () = Sqlite3.prepare db "INSERT INTO log VALUES (?, ?, ?)"
             |> bind_entry entry
             |> Sqlite3.step
             |> check db Sqlite3.Rc.DONE
    in
    db

  let append_after db ~index ~entries =
    let () = Sqlite3.prepare db "DELETE FROM log WHERE idx > ?"
             |> bind_int 1 index
             |> Sqlite3.step
             |> check db Sqlite3.Rc.DONE
    in
    List.fold_left entries ~init:db ~f:(fun db entry -> append db entry)

  let last db =
    let query =
      "SELECT idx, term, command \
       FROM log \
       ORDER BY idx DESC \
       LIMIT 1" in
    Sqlite3.prepare db query |> find_one

  let last_before db ~index =
    let query =
      "SELECT idx, term, command \
       FROM log \
       WHERE idx < ? \
       ORDER BY idx DESC \
       LIMIT 1" in
    Sqlite3.prepare db query |> bind_int 1 index |> find_one
end
