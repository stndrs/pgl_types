import gleam/bit_array
import gleam/dynamic.{type Dynamic}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import pgl/types/internal
import pgl/value.{type Value}

pub type TypeInfo {
  TypeInfo(
    oid: Int,
    name: String,
    typesend: String,
    typereceive: String,
    typelen: Int,
    output: String,
    input: String,
    elem_oid: Int,
    elem_type: Option(TypeInfo),
    base_oid: Int,
    comp_oids: List(Int),
    comp_types: Option(List(TypeInfo)),
  )
}

pub fn info(oid: Int) -> TypeInfo {
  TypeInfo(
    oid:,
    name: "",
    typesend: "",
    typereceive: "",
    typelen: 0,
    output: "",
    input: "",
    elem_oid: 0,
    elem_type: None,
    base_oid: 0,
    comp_oids: [],
    comp_types: None,
  )
}

pub fn name(ti: TypeInfo, name: String) -> TypeInfo {
  TypeInfo(..ti, name:)
}

pub fn typesend(ti: TypeInfo, typesend: String) -> TypeInfo {
  TypeInfo(..ti, typesend:)
}

pub fn typereceive(ti: TypeInfo, typereceive: String) -> TypeInfo {
  TypeInfo(..ti, typereceive:)
}

pub fn typelen(ti: TypeInfo, typelen: Int) -> TypeInfo {
  TypeInfo(..ti, typelen:)
}

pub fn output(ti: TypeInfo, output: String) -> TypeInfo {
  TypeInfo(..ti, output:)
}

pub fn input(ti: TypeInfo, input: String) -> TypeInfo {
  TypeInfo(..ti, input:)
}

pub fn elem_oid(ti: TypeInfo, elem_oid: Int) -> TypeInfo {
  TypeInfo(..ti, elem_oid:)
}

pub fn base_oid(ti: TypeInfo, base_oid: Int) -> TypeInfo {
  TypeInfo(..ti, base_oid:)
}

pub fn comp_oids(ti: TypeInfo, comp_oids: List(Int)) -> TypeInfo {
  TypeInfo(..ti, comp_oids:)
}

pub fn elem_type(ti: TypeInfo, elem_type: Option(TypeInfo)) -> TypeInfo {
  TypeInfo(..ti, elem_type:)
}

pub fn comp_types(ti: TypeInfo, comp_types: Option(List(TypeInfo))) -> TypeInfo {
  TypeInfo(..ti, comp_types:)
}

// ---------- Encoding ---------- //

pub fn encode(value: Value, ti: TypeInfo) -> Result(BitArray, String) {
  case value {
    value.Null -> encode_null(Nil, ti)
    value.Bool(val) -> encode_bool(val, ti)
    value.Int(val) -> encode_int(val, ti)
    value.Float(val) -> encode_float(val, ti)
    value.Text(val) -> encode_text(val, ti)
    value.Bytea(val) -> encode_bytea(val, ti)
    value.Time(val) -> encode_time(val, ti)
    value.Date(val) -> encode_date(val, ti)
    value.Timestamp(val) -> encode_timestamp(val, ti)
    value.Interval(val) -> encode_interval(val, ti)
    value.Array(val) -> encode_array(val, ti)
  }
}

fn encode_array(elems: List(Value), ti: TypeInfo) -> Result(BitArray, String) {
  let dimensions = arr_dims(elems)

  case ti.elem_type {
    Some(elem_ti) -> {
      elems
      |> list.try_map(encode(_, elem_ti))
      |> result.try(fn(encoded_elems) {
        let has_nulls = list.contains(encoded_elems, <<-1:big-int-size(32)>>)

        do_encode_array(dimensions, has_nulls, elem_ti, encoded_elems)
      })
    }
    None -> Error("Missing elem type info")
  }
}

fn arr_dims(elems: List(Value)) -> List(Int) {
  case elems {
    [] -> []
    [value.Array(inner), ..] -> {
      let inner_dims = arr_dims(inner)
      let dim = list.length(elems)
      [dim, ..inner_dims]
    }
    elems -> {
      let dim = list.length(elems)
      [dim]
    }
  }
}

fn do_encode_array(
  dimensions: List(Int),
  has_nulls: Bool,
  ti: TypeInfo,
  encoded: List(BitArray),
) -> Result(BitArray, String) {
  let header = array_header(dimensions, has_nulls, ti.oid)

  case encoded {
    [] -> encode_bytea(header, ti)
    _ -> bit_array.concat([header, ..encoded]) |> encode_bytea(ti)
  }
}

fn array_header(
  dimensions: List(Int),
  has_nulls: Bool,
  elem_type_oid: Int,
) -> BitArray {
  let num_dims = list.length(dimensions)

  let flags = case has_nulls {
    True -> 1
    False -> 0
  }

  let encoded_dimensions =
    dimensions
    |> list.map(fn(dim) { <<dim:big-int-size(32), 1:big-int-size(32)>> })
    |> bit_array.concat

  [
    <<num_dims:int-size(32), flags:int-size(32), elem_type_oid:int-size(32)>>,
    encoded_dimensions,
  ]
  |> bit_array.concat
}

fn encode_null(_: a, _ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<-1:big-int-size(32)>>)
}

fn encode_bool(bool: Bool, _ti: TypeInfo) -> Result(BitArray, String) {
  case bool {
    True -> Ok(<<1:big-int-size(32), 1:big-int-size(8)>>)
    False -> Ok(<<1:big-int-size(32), 0:big-int-size(8)>>)
  }
}

fn encode_oid(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case 0 <= num && num <= internal.oid_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for oid")
  }
}

fn encode_int(num: Int, ti: TypeInfo) -> Result(BitArray, String) {
  case ti.typesend {
    "oidsend" -> encode_oid(num, ti)
    "int2send" -> encode_int2(num, ti)
    "int4send" -> encode_int4(num, ti)
    "int8send" -> encode_int8(num, ti)
    _ -> {
      echo ti
      Error("Unsupported int type")
    }
  }
}

fn encode_int2(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case -internal.int2_min <= num && num <= internal.int2_max {
    True -> Ok(<<2:big-int-size(32), num:big-int-size(16)>>)
    False -> Error("Out of range for int2")
  }
}

fn encode_int4(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case -internal.int4_min <= num && num <= internal.int4_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for int4")
  }
}

fn encode_int8(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case -internal.int8_min <= num && num <= internal.int8_max {
    True -> Ok(<<8:big-int-size(32), num:big-int-size(64)>>)
    False -> Error("Out of range for int8")
  }
}

fn encode_float(num: Float, ti: TypeInfo) -> Result(BitArray, String) {
  case ti.typesend {
    "float4send" -> encode_float4(num, ti)
    "float8send" -> encode_float8(num, ti)
    _ -> Error("Unsupported int type")
  }
}

fn encode_float4(num: Float, _ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<4:big-int-size(32), num:big-float-size(32)>>)
}

fn encode_float8(num: Float, _ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<8:big-int-size(32), num:big-float-size(64)>>)
}

fn encode_text(text: String, ti: TypeInfo) -> Result(BitArray, String) {
  bit_array.from_string(text) |> encode_bytea(ti)
}

fn encode_bytea(bits: BitArray, _ti: TypeInfo) -> Result(BitArray, String) {
  let len = bit_array.byte_size(bits)

  Ok(<<len:big-int-size(32), bits:bits>>)
}

fn encode_date(date: calendar.Date, _ti: TypeInfo) -> Result(BitArray, String) {
  let gregorian_days =
    internal.date_to_gregorian_days(
      date.year,
      calendar.month_to_int(date.month),
      date.day,
    )
  let pg_days = gregorian_days - internal.postgres_gd_epoch

  Ok(<<4:big-int-size(32), pg_days:big-int-size(32)>>)
}

fn encode_time(
  tod: calendar.TimeOfDay,
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let usecs =
    duration.hours(tod.hours)
    |> duration.add(duration.minutes(tod.minutes))
    |> duration.add(duration.seconds(tod.seconds))
    |> duration.add(duration.nanoseconds(tod.nanoseconds))
    |> internal.to_microseconds(duration.to_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), usecs:big-int-size(64)>>)
}

fn encode_interval(
  dur: duration.Duration,
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let usecs = internal.to_microseconds(dur, duration.to_seconds_and_nanoseconds)

  let encoded = <<
    16:big-int-size(32),
    usecs:big-int-size(64),
    0:big-int-size(32),
    0:big-int-size(32),
  >>

  Ok(encoded)
}

fn encode_timestamp(
  ts: timestamp.Timestamp,
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let ts_int =
    internal.unix_seconds_before_postgres_epoch()
    |> timestamp.add(ts, _)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), ts_int:big-int-size(64)>>)
}

fn encode_timestamptz(
  tsz: #(timestamp.Timestamp, duration.Duration),
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let #(ts, offset) = tsz

  let ts_int =
    internal.unix_seconds_before_postgres_epoch()
    |> duration.add(offset)
    |> timestamp.add(ts, _)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), ts_int:big-int-size(64)>>)
}

// ---------- Decoding ---------- //

pub fn decode(val: BitArray, ti: TypeInfo) -> Result(Dynamic, String) {
  case ti.typereceive {
    "array_recv" ->
      decode_array(val, with: fn(elem) {
        case ti.elem_type {
          Some(elem_ti) -> decode(elem, elem_ti)
          None -> Error("elem type missing")
        }
      })
    "boolrecv" -> decode_bool(val)
    "oidrecv" -> decode_oid(val)
    "int2recv" -> decode_int2(val)
    "int4recv" -> decode_int4(val)
    "int8recv" -> decode_int8(val)
    "float4recv" -> decode_float4(val)
    "float8recv" -> decode_float8(val)
    "textrecv" -> decode_text(val)
    "varcharrecv" -> decode_varchar(val)
    "namerecv" -> decode_text(val)
    "charrecv" -> decode_text(val)
    "bytearecv" -> decode_bytea(val)
    "time_recv" -> decode_time(val)
    "date_recv" -> decode_date(val)
    "timestamp_recv" -> decode_timestamp(val)
    "timestamptz_recv" -> decode_timestamp(val)
    "interval_recv" -> decode_interval(val)
    _ -> Error("Unsupported type")
  }
}

fn decode_array(
  bits: BitArray,
  with decoder: fn(BitArray) -> Result(Dynamic, String),
) -> Result(Dynamic, String) {
  case bits {
    <<
      dimensions:big-signed-int-size(32),
      _flags:big-signed-int-size(32),
      _elem_oid:big-signed-int-size(32),
      rest:bits,
    >> -> {
      use data <- result.try(do_decode_array(dimensions, rest, []))

      decode_array_elems(data.0, decoder, [])
      |> result.map(dynamic.array)
    }
    _ -> Error("invalid array")
  }
}

fn do_decode_array(
  count: Int,
  bits: BitArray,
  acc: List(#(Int, Int)),
) -> Result(#(BitArray, List(#(Int, Int))), String) {
  case count {
    0 -> Ok(#(bits, acc))
    idx -> {
      case bits {
        <<
          nbr:big-signed-int-size(32),
          l_bound:big-signed-int-size(32),
          rest1:bits,
        >> -> {
          let current = #(nbr, l_bound)

          let data_info1 = list.prepend(acc, current)

          do_decode_array({ idx - 1 }, rest1, data_info1)
        }
        _ -> Error("invalid array")
      }
    }
  }
}

fn decode_array_elems(
  bits: BitArray,
  decoder: fn(BitArray) -> Result(Dynamic, String),
  acc: List(Dynamic),
) -> Result(List(Dynamic), String) {
  case bits {
    <<>> -> Ok(list.reverse(acc))
    <<-1:big-signed-int-size(32), rest:bits>> -> {
      list.prepend(acc, dynamic.nil())
      |> decode_array_elems(rest, decoder, _)
    }
    <<size:big-signed-int-size(32), rest:bits>> -> {
      let elem_len = size * 8

      case rest {
        <<val_bin:bits-size(elem_len), rest1:bits>> -> {
          use decoded <- result.try(decoder(val_bin))

          list.prepend(acc, decoded)
          |> decode_array_elems(rest1, decoder, _)
        }
        _ -> Error("invalid array")
      }
    }
    _ -> Error("invalid array")
  }
}

// Types decode functions

fn decode_bool(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<1:big-signed-int-size(8)>> -> Ok(dynamic.bool(True))
    <<0:big-signed-int-size(8)>> -> Ok(dynamic.bool(False))
    _ -> Error("invalid bool")
  }
}

fn decode_int2(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-signed-int-size(16)>> -> Ok(dynamic.int(num))
    _ -> Error("invalid int2")
  }
}

fn decode_oid(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-unsigned-int-size(32)>> -> Ok(dynamic.int(num))

    _ -> Error("invalid oid")
  }
}

fn decode_int4(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-signed-int-size(32)>> -> Ok(dynamic.int(num))
    _ -> Error("invalid int4")
  }
}

fn decode_int8(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-signed-int-size(64)>> -> Ok(dynamic.int(num))
    _ -> Error("invalid int8")
  }
}

fn decode_float4(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<value:big-float-size(32)>> -> {
      float.to_precision(value, 4)
      |> dynamic.float
      |> Ok
    }
    _ -> Error("invalid float4")
  }
}

fn decode_float8(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<value:big-float-size(64)>> -> {
      float.to_precision(value, 8)
      |> dynamic.float
      |> Ok
    }
    _ -> Error("invalid float8")
  }
}

fn decode_varchar(bits: BitArray) -> Result(Dynamic, String) {
  bit_array.to_string(bits)
  |> result.map(dynamic.string)
  |> result.replace_error("invalid varchar")
}

fn decode_text(bits: BitArray) -> Result(Dynamic, String) {
  bit_array.to_string(bits)
  |> result.map(dynamic.string)
  |> result.replace_error("invalid text")
}

fn decode_bytea(bits: BitArray) -> Result(Dynamic, String) {
  Ok(dynamic.bit_array(bits))
}

fn decode_time(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<microseconds:big-int-size(64)>> -> {
      let tod = from_microseconds(microseconds)

      dynamic.array([
        dynamic.int(tod.hours),
        dynamic.int(tod.minutes),
        dynamic.int(tod.seconds),
        dynamic.int(tod.nanoseconds / 1000),
      ])
      |> Ok
    }
    _ -> Error("invalid time")
  }
}

fn decode_timestamp(bits: BitArray) -> Result(Dynamic, String) {
  let pos_infinity = internal.int8_max
  let neg_infinity = -internal.int8_min

  case bits {
    <<num:signed-big-int-size(64)>> -> {
      case num {
        _pos_inf if num == pos_infinity -> Ok(dynamic.string("infinity"))
        _neg_inf if num == neg_infinity -> Ok(dynamic.string("-infinity"))
        _ -> Ok(handle_timestamp(num))
      }
    }
    _ -> Error("invalid timestamp")
  }
}

fn handle_timestamp(microseconds: Int) -> Dynamic {
  let seconds_since_unix_epoch =
    { microseconds / 1_000_000 }
    |> int.add(internal.postgres_gs_epoch)
    |> int.subtract(internal.gs_to_unix_epoch)

  let usecs_since_unix_epoch = seconds_since_unix_epoch * 1_000_000

  usecs_since_unix_epoch
  |> int.add({ microseconds % 1_000_000 })
  |> dynamic.int
}

fn decode_date(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<days:big-signed-int-size(32)>> -> {
      days_to_date(days)
      |> result.map(fn(date) {
        let month = calendar.month_to_int(date.month)

        dynamic.array([
          dynamic.int(date.year),
          dynamic.int(month),
          dynamic.int(date.day),
        ])
      })
      |> result.replace_error("Invalid month")
    }
    _ -> Error("invalid date")
  }
}

fn decode_interval(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<
      microseconds:big-signed-int-size(64),
      _days:big-signed-int-size(32),
      _months:big-signed-int-size(32),
    >> -> Ok(dynamic.int(microseconds))
    _ -> Error("invalid interval")
  }
}

fn from_microseconds(usecs: Int) -> calendar.TimeOfDay {
  let seconds = usecs / internal.usecs_per_sec
  let nanoseconds = { usecs % internal.usecs_per_sec } * 1000

  let #(hours, minutes, seconds) = internal.seconds_to_time(seconds)

  calendar.TimeOfDay(hours:, minutes:, seconds:, nanoseconds:)
}

fn days_to_date(days: Int) -> Result(calendar.Date, Nil) {
  let #(year, month, day) =
    internal.gregorian_days_to_date(days + internal.postgres_gd_epoch)

  calendar.month_from_int(month)
  |> result.map(fn(month) { calendar.Date(year:, month:, day:) })
}
