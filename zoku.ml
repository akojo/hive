open Core.Std

module Key_value_store = struct
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
end

module Zoku = Qupt.Make(Key_value_store)(String)

type ext_config = {
  hosts: (string * Unix.Inet_addr.Blocking_sexp.t * int) list
} [@@deriving sexp]

type cluster = Unix.sockaddr_blocking_sexp String.Map.t [@@deriving sexp]

type server_state = {
  zoku_fd: Unix.File_descr.t;
  client_fd: Unix.File_descr.t;
  qupt: Zoku.t;
  cluster: cluster;
  pending: (int * Unix.sockaddr) list;
}

let bind_socket addr port =
  let sock = Unix.(socket ~domain:PF_INET ~kind:SOCK_DGRAM ~protocol:0) in
  let () = Unix.(bind sock ~addr:(ADDR_INET (addr, port))) in
  sock

let parse_sexp buf convf fd addr =
  try
    match Sexp.parse buf with
    | Sexp.Done (sexp, _) -> Some (convf sexp)
    | _ -> failwith ("Invalid input: " ^ buf)
  with ex ->
    let err = Exn.sexp_of_t ex |> Sexp.to_string_hum in
    let () = print_endline err in
    let _ = Unix.(sendto fd ~buf:err ~pos:0 ~len:(String.length err) ~mode:[] ~addr) in
    None

let read_configuration localhost ext_config =
  let cluster = match ext_config with
    | Some ext_config ->
      let conf = Sexp.of_string ext_config |> ext_config_of_sexp in
      List.fold_left (localhost :: conf.hosts)
        ~init:String.Map.empty
        ~f:(fun hosts (name, addr, port) ->
            let sockaddr = Unix.ADDR_INET (addr, port) in
            Map.add hosts ~key:name ~data:sockaddr
          )
    | None -> String.Map.empty
  in
  let () = sexp_of_cluster cluster |> Sexp.to_string_hum |> print_endline in
  cluster

let do_io state messages =
  let cluster = state.cluster in
  List.iter messages ~f:(fun message ->
      match message with
      | Zoku.Rpc (id, rpc) ->
        let addr = Map.find_exn cluster id in
        let buf = Zoku.sexp_of_rpc rpc |> Sexp.to_string_hum in
        ignore Unix.(sendto state.zoku_fd ~buf ~pos:0 ~len:(String.length buf) ~mode:[] ~addr)
      | Zoku.Response (id, response) ->
        (match List.Assoc.find state.pending id with
         | Some addr ->
           let buf = Key_value_store.sexp_of_response response |> Sexp.to_string_hum in
           ignore Unix.(sendto state.client_fd ~buf ~pos:0 ~len:(String.length buf) ~mode:[] ~addr)
         | None -> ()
        )
    )

let handle_timeout state =
  let messages, qupt = Zoku.handle_timeout state.qupt in
  let () = do_io state messages in
  { state with qupt }

let run zoku_port client_port leader bind_to name configuration () =
  let bind_addr = match bind_to with
    | Some addr -> Unix.Inet_addr.of_string addr
    | None -> Unix.Inet_addr.bind_any
  in
  let myname = match name with
    | Some name -> name
    | None -> Unix.gethostname ()
  in
  let buf = String.create 65536 in
  let handle_read state fd =
    let (len, addr) = Unix.recvfrom fd ~buf:buf ~pos:0 ~len:65536 ~mode:[] in
    let input = String.slice buf 0 len |> String.strip in
    let () = print_endline input in
    if fd = state.zoku_fd then
      parse_sexp input Zoku.rpc_of_sexp fd addr
      |> Option.value_map ~default:state ~f:(fun rpc ->
          let io, qupt = Zoku.handle_rpc state.qupt rpc in
          let () = do_io state io in
          { state with qupt }
        )
    else
      parse_sexp input Key_value_store.command_of_sexp fd addr
      |> Option.value_map ~default:state ~f:(fun command ->
          let index, io, qupt = Zoku.handle_command state.qupt command in
          let pending = List.Assoc.add state.pending index addr in
          let () = do_io state io in
          { state with qupt; pending }
        )
  in
  let rec loop state =
    let read_fds = [state.zoku_fd; state.client_fd] in
    let timeout = `After (Time_ns.Span.of_sec 1.0) in
    let fds = Unix.select ~read:read_fds ~write:[] ~except:[] ~timeout () in
    let open Unix.Select_fds in
    if List.length fds.read = 0 then
      handle_timeout state |> loop
    else
      List.fold_left ~init:state fds.read ~f:handle_read |> loop
  in
  let zoku_fd = bind_socket bind_addr zoku_port in
  let client_fd = bind_socket bind_addr client_port in
  let cluster = read_configuration (myname, bind_addr, zoku_port) configuration in
  let ids = Map.keys cluster in
  let qupt = Zoku.init leader myname ids Key_value_store.empty in
  loop { zoku_fd; client_fd; qupt; cluster; pending = [] }

let () = Command.run
    (Command.basic
       ~summary:"Start a zoku node"
       Command.Spec.(
         empty
         +> flag "-P" (optional_with_default 7383 int) ~doc:"port zoku port (default 7383)"
         +> flag "-p" (optional_with_default 2360 int) ~doc:"port client port (default 2360)"
         +> flag "-L" no_arg ~doc:"start as a leader"
         +> flag "-b" (optional string) ~doc:"address ip addres to bind to (default 0.0.0.0)"
         +> flag "-n" (optional string) ~doc:"node name (default hostname)"
         +> flag "-c" (optional string) ~doc:"configuration"
       )
       run
    )
