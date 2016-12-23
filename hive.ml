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

module Hive = Raft.Make(Key_value_store)(Int64)

type ext_config = {
  hosts: (Unix.Inet_addr.Blocking_sexp.t * int) list
} [@@deriving sexp]

type cluster = Unix.sockaddr_blocking_sexp Int64.Map.t [@@deriving sexp]

type server_state = {
  raft: Hive.t;
  cluster: cluster;
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
  let make_id addr port =
    Int64.(shift_left (of_int32 (Unix.Inet_addr.inet4_addr_to_int32_exn addr)) 32 |> bit_or (of_int port))
  in
  let cluster = match ext_config with
    | Some ext_config ->
      let conf = Sexp.of_string ext_config |> ext_config_of_sexp in
      List.fold_left (localhost :: conf.hosts) ~init:Int64.Map.empty ~f:(fun hosts (addr, port) ->
          let sockaddr = Unix.ADDR_INET (addr, port) in
          let index = make_id addr port in
          Map.add hosts ~key:index ~data:sockaddr
        )
    | None -> Int64.Map.empty
  in
  let () = sexp_of_cluster cluster |> Sexp.to_string_hum |> print_endline in
  let (localaddr, localport) = localhost in
  make_id localaddr localport, cluster

let do_io fd cluster messages =
  List.iter messages ~f:(fun message ->
      match message with
      | Hive.Rpc (id, rpc) ->
        let addr = Map.find_exn cluster id in
        let buf = Hive.sexp_of_rpc rpc |> Sexp.to_string_hum in
        ignore Unix.(sendto fd ~buf ~pos:0 ~len:(String.length buf) ~mode:[] ~addr)
      | _ -> failwith "Not implemented yet"
    )

let handle_timeout fd state =
  let messages, raft = Hive.handle_timeout state.raft in
  let () = do_io fd state.cluster messages in
  { state with raft }

let run hive_port client_port leader bind_to configuration () =
  let bind_addr = match bind_to with
    | Some addr -> Unix.Inet_addr.of_string addr
    | None -> Unix.Inet_addr.bind_any
  in
  let hivefd = bind_socket bind_addr hive_port in
  let clifd = bind_socket bind_addr client_port in
  let buf = String.create 65536 in
  let read_fds = [hivefd; clifd] in
  let handle_read state fd =
    let (len, addr) = Unix.recvfrom fd ~buf:buf ~pos:0 ~len:65536 ~mode:[] in
    let input = String.slice buf 0 len |> String.strip in
    let () = print_endline input in
    if fd = hivefd then
      parse_sexp input Hive.rpc_of_sexp fd addr
      |> Option.value_map ~default:state ~f:(fun rpc ->
          let messages, raft = Hive.handle_rpc state.raft rpc in
          let () = do_io fd state.cluster messages in
          { state with raft }
        )
    else
      parse_sexp input Key_value_store.command_of_sexp fd addr
      |> Option.value_map ~default:state ~f:(fun command ->
          let _, _, raft = Hive.handle_command state.raft command in
          { state with raft }
        )
  in
  let rec loop state =
    let timeout = `After (Time_ns.Span.of_sec 1.0) in
    let fds = Unix.select ~read:read_fds ~write:[] ~except:[] ~timeout () in
    let open Unix.Select_fds in
    if List.length fds.read = 0 then
      handle_timeout hivefd state |> loop
    else
      List.fold_left ~init:state fds.read ~f:handle_read |> loop
  in
  let self, cluster = read_configuration (bind_addr, hive_port) configuration in
  let role = if leader then Hive.Leader else Hive.Follower in
  let ids = Map.keys cluster in
  let raft = Hive.init self role ids Key_value_store.empty in
  loop { raft; cluster }

let () = Command.run
    (Command.basic
       ~summary:"Start a hive node"
       Command.Spec.(
         empty
         +> flag "-P" (optional_with_default 7383 int) ~doc:"port hive port (default 7383)"
         +> flag "-p" (optional_with_default 2360 int) ~doc:"port client port (default 2360)"
         +> flag "-L" no_arg ~doc:"start as a leader"
         +> flag "-b" (optional string) ~doc:"address ip addres to bind to (default 0.0.0.0)"
         +> flag "-c" (optional string) ~doc:"configuration"
       )
       run
    )