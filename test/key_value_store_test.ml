open Core.Std
open OUnit2

let assert_response expected actual =
  let print_response response =
    Key_value_store.sexp_of_response response |> Sexp.to_string_hum in
  assert_equal ~printer:print_response expected actual

let all =
  let test f ctx =
    let store = bracket
        (fun _ctx -> Key_value_store.empty)
        (fun _log _ctx -> ()) ctx
    in
    f store
  in
  let open Key_value_store in
  "Key-value store" >::: [
    "returns None for non-existent value" >:: test (fun store ->
        let response, _ = apply store (Get "key1") in
        assert_response { key = "key1"; value = None } response
      );

    "returns set value" >:: test (fun store ->
        let response, _ = apply store (Set ("key1", "value1")) in
        assert_response { key = "key1"; value = Some "value1"} response
      );

    "returns previously set value" >:: test (fun store ->
        let _, store = apply store (Set ("key2", "value2")) in
        let response, _ = apply store (Get "key2") in
        assert_response { key = "key2"; value = Some "value2" } response
      );

    "returns last value set for key" >:: test (fun store ->
        let _, store = apply store (Set ("key3", "value3")) in
        let _, store = apply store (Set ("key3", "value4")) in
        let response, _ = apply store (Get "key3") in
        assert_response { key = "key3"; value = Some "value4" } response
      );

    "stores several keys" >:: test (fun store ->
        let _, store = apply store (Set ("key4", "value4")) in
        let _, store = apply store (Set ("key5", "value5")) in
        let response4, store = apply store (Get "key4") in
        let response5, _ = apply store (Get "key5") in
        assert_response { key = "key4"; value = Some "value4" } response4;
        assert_response { key = "key5"; value = Some "value5" } response5
      );
  ]
