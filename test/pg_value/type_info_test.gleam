import gleam/option.{None, Some}
import pg_value/type_info

pub fn new_test() {
  let info = type_info.new(23)

  assert 23 == info.oid
  assert "" == info.name
  assert "" == info.typesend
  assert "" == info.typereceive
  assert 0 == info.typelen
  assert "" == info.output
  assert "" == info.input
  assert 0 == info.elem_oid
  assert None == info.elem_type
  assert 0 == info.base_oid
  assert [] == info.comp_oids
  assert None == info.comp_types
}

pub fn type_info_test() {
  let int4_info =
    type_info.new(23)
    |> type_info.name("int4")
    |> type_info.typesend("int4send")
    |> type_info.typereceive("int4recv")
    |> type_info.typelen(4)
    |> type_info.output("int4out")
    |> type_info.input("int4in")

  assert 23 == int4_info.oid
  assert "int4" == int4_info.name
  assert "int4send" == int4_info.typesend
  assert "int4recv" == int4_info.typereceive
  assert 4 == int4_info.typelen
  assert "int4out" == int4_info.output
  assert "int4in" == int4_info.input
  assert 0 == int4_info.elem_oid
  assert None == int4_info.elem_type
  assert 0 == int4_info.base_oid
  assert [] == int4_info.comp_oids
  assert None == int4_info.comp_types

  let array_info =
    type_info.new(1007)
    |> type_info.name("_int4")
    |> type_info.typesend("array_send")
    |> type_info.typereceive("array_recv")
    |> type_info.typelen(-1)
    |> type_info.output("array_out")
    |> type_info.input("array_in")
    |> type_info.elem_oid(23)
    |> type_info.elem_type(Some(int4_info))

  assert 1007 == array_info.oid
  assert "_int4" == array_info.name
  assert "array_send" == array_info.typesend
  assert "array_recv" == array_info.typereceive
  assert -1 == array_info.typelen
  assert "array_out" == array_info.output
  assert "array_in" == array_info.input
  assert 23 == array_info.elem_oid
  let assert Some(_int4) = array_info.elem_type
  assert 0 == array_info.base_oid
  assert [] == array_info.comp_oids
  assert None == array_info.comp_types
}
