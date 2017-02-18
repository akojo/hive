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

  let failure db rc = failwith Sqlite3.((Rc.to_string rc) ^ ": " ^ (errmsg db))

  let check db expected rc = if rc <> expected then failure db rc

  let to_int = function
    | Sqlite3.Data.INT value -> Int64.to_int_exn value
    | col -> failwith ("to_int: " ^ Sqlite3.Data.to_string_debug col)

  let to_string = function
    | Sqlite3.Data.TEXT value -> value
    | col -> failwith ("to_string: " ^ Sqlite3.Data.to_string_debug col)

  let get_int index stmt = Sqlite3.column stmt index |> to_int

  let get_string index stmt = Sqlite3.column stmt index |> to_string

  let of_int i = (Sqlite3.Data.INT (Int64.of_int i))

  let of_string str = Sqlite3.Data.TEXT str

  let query ~mapper ?(params = []) ~sql db =
    let rec bind_params stmt index = function
      | param :: rest ->
        let () = Sqlite3.bind stmt index param |> check db Sqlite3.Rc.OK in
        bind_params stmt (index + 1) rest
      | [] -> stmt
    in
    let read_rows mapper stmt =
      let rec loop res =
        match Sqlite3.step stmt with
        | Sqlite3.Rc.ROW -> loop (mapper stmt :: res)
        | Sqlite3.Rc.DONE -> res
        | rc -> failure db rc
      in
      loop []
    in
    let stmt = Sqlite3.prepare db sql in
    let result = bind_params stmt 1 params |> read_rows mapper in
    let () = Sqlite3.finalize stmt |> check db Sqlite3.Rc.OK in
    result

  let query_one ~mapper ?(params = []) ~sql db=
    match query ~mapper ~params ~sql db with
    | result :: [] -> result
    | _ -> failwith "query_one"

  let update ?(params = []) ~sql db =
    match query ~mapper:ident ~params ~sql db with
    | [] -> db
    | _ -> failwith "update"

  let of_entry entry =
    [
      of_int entry.index;
      of_int entry.term;
      Command.sexp_of_command entry.command |> Sexp.to_string |> of_string
    ]

  let read_entry stmt =
    {
      index = get_int 0 stmt;
      term = get_int 1 stmt;
      command = get_string 2 stmt |> Sexp.of_string |> Command.command_of_sexp
    }

  let create filename =
    Sqlite3.db_open filename |> update
      ~sql:"CREATE TABLE IF NOT EXISTS log ( \
            idx INTEGER PRIMARY KEY, \
            term INTEGER NOT NULL, \
            command TEXT NOT NULL)"

  let close db = assert (Sqlite3.db_close db)

  let is_empty db =
    (query_one db ~sql:"SELECT COUNT(*) FROM log" ~mapper:(get_int 0)) = 0

  let find db ~index =
    query db ~sql:"SELECT idx, term, command FROM log WHERE idx = ?"
      ~mapper:read_entry ~params:[of_int index] |> List.hd

  let from db ~index =
    query db ~mapper:read_entry ~params:[of_int index]
      ~sql:"SELECT idx, term, command \
            FROM log \
            WHERE idx >= ? \
            ORDER BY idx ASC"

  let range db ~first ~last =
    query db ~mapper:read_entry ~params:[of_int first; of_int last]
      ~sql:"SELECT idx, term, command \
            FROM log \
            WHERE idx > ? AND idx <= ? \
            ORDER BY idx ASC"

  let append db entry =
    update db ~params:(of_entry entry) ~sql:"INSERT INTO log VALUES (?, ?, ?)"

  let append_after db ~index ~entries =
    let db = update db ~params:[of_int index] ~sql:"DELETE FROM log WHERE idx > ?" in
    List.fold_left entries ~init:db ~f:(fun db entry -> append db entry)

  let last db =
    let sql =
      "SELECT idx, term, command \
       FROM log \
       ORDER BY idx DESC \
       LIMIT 1" in
    query db ~sql ~mapper:read_entry |> List.hd

  let last_before db ~index =
    let sql =
      "SELECT idx, term, command \
       FROM log \
       WHERE idx < ? \
       ORDER BY idx DESC \
       LIMIT 1" in
    query db ~sql ~mapper:read_entry ~params:[of_int index] |> List.hd
end
