//// PostgreSQL values, along with their encoders and decoders. Can
//// be used by PostgreSQL client libraries written in gleam.

import gleam/bit_array
import gleam/bool
import gleam/dynamic.{type Dynamic}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp

/// `Offset` represents a UTC offset. `Timestamptz` is composed
/// of a `timestamp.Timestamp` and `Offset`. The offset will be
/// applied to the timestamp when being encoded.
///
/// A timestamp with a positive offset represents some time in
/// the future, relative to UTC.
/// A timestamp with a negative offset represents some time in
/// the past, relative to UTC.
///
/// Offsets will be subtracted from the `timestamp.Timestamp`
/// so the encoded value is a UTC timestamp.
pub type Offset {
  Offset(hours: Int, minutes: Int)
}

/// Returns an Offset with the provided hours and 0 minutes.
pub fn offset(hours: Int) -> Offset {
  Offset(hours:, minutes: 0)
}

/// Applies some number of minutes to the Offset
pub fn minutes(offset: Offset, minutes: Int) -> Offset {
  Offset(..offset, minutes:)
}

pub type Value {
  Null
  Bool(Bool)
  Int(Int)
  Float(Float)
  Text(String)
  Bytea(BitArray)
  Time(calendar.TimeOfDay)
  Date(calendar.Date)
  Timestamp(timestamp.Timestamp)
  Timestamptz(timestamp.Timestamp, offset: Offset)
  Interval(duration.Duration)
  Array(List(Value))
}

pub const null = Null

pub const true = Bool(True)

pub const false = Bool(False)

pub fn bool(val: Bool) -> Value {
  Bool(val)
}

pub fn int(val: Int) -> Value {
  Int(val)
}

pub fn float(val: Float) -> Value {
  Float(val)
}

pub fn text(val: String) -> Value {
  Text(val)
}

pub fn bytea(val: BitArray) -> Value {
  Bytea(val)
}

pub fn time(val: calendar.TimeOfDay) -> Value {
  Time(val)
}

pub fn date(val: calendar.Date) -> Value {
  Date(val)
}

pub fn timestamp(ts: timestamp.Timestamp) -> Value {
  Timestamp(ts)
}

pub fn timestamptz(ts: timestamp.Timestamp, offset: Offset) -> Value {
  Timestamptz(ts, offset)
}

pub fn interval(val: duration.Duration) -> Value {
  Interval(val)
}

pub fn array(vals: List(a), of kind: fn(a) -> Value) -> Value {
  vals
  |> list.map(kind)
  |> Array
}

/// Checks if the provided value is `option.Some` or `option.None`. If
/// `None`, then the value returned is `value.Null`. If `Some` value is
/// provided, then it is passed to the `inner_type` function.
///
/// Example:
///
/// ```gleam
///   let int = pg_value.nullable(pg_value.int, Some(10))
///
///   let null = pg_value.nullable(pg_value.int, None)
/// ```
pub fn nullable(inner_type: fn(a) -> Value, value: Option(a)) -> Value {
  case value {
    Some(term) -> inner_type(term)
    None -> Null
  }
}

/// Converts a `Value` to a string formatted properly for PostgreSQL
pub fn to_string(val: Value) -> String {
  case val {
    Null -> "NULL"
    Bool(val) -> bool_to_string(val)
    Int(val) -> int.to_string(val)
    Float(val) -> float.to_string(val)
    Text(val) -> text_to_string(val)
    Bytea(val) -> bytea_to_string(val)
    Time(val) -> time_to_string(val)
    Date(val) -> date_to_string(val)
    Timestamp(val) -> timestamp_to_string(val)
    Timestamptz(ts, offset) -> timestamptz_to_string(ts, offset)
    Interval(val) -> duration_to_string(val)
    Array(vals) -> array_to_string(vals)
  }
}

fn text_to_string(val: String) -> String {
  let val = string.replace(in: val, each: "'", with: "\\'")

  single_quote(val)
}

// https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
fn array_to_string(val: List(Value)) -> String {
  let elems = case val {
    [] -> ""
    [val] -> to_string(val)
    vals -> {
      vals
      |> list.map(to_string)
      |> string.join(", ")
    }
  }

  "ARRAY[" <> elems <> "]"
}

// https://www.postgresql.org/docs/current/datatype-boolean.html#DATATYPE-BOOLEAN
fn bool_to_string(val: Bool) -> String {
  case val {
    True -> "TRUE"
    False -> "FALSE"
  }
}

// https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
fn bytea_to_string(val: BitArray) -> String {
  let val = "\\x" <> bit_array.base16_encode(val)

  single_quote(val)
}

fn date_to_string(date: calendar.Date) -> String {
  let year = int.to_string(date.year)
  let month = calendar.month_to_int(date.month) |> pad_zero
  let day = pad_zero(date.day)

  let date = year <> "-" <> month <> "-" <> day

  single_quote(date)
}

fn time_to_string(tod: calendar.TimeOfDay) -> String {
  let hours = pad_zero(tod.hours)
  let minutes = pad_zero(tod.minutes)
  let seconds = pad_zero(tod.seconds)
  let milliseconds = tod.nanoseconds / 1_000_000

  let msecs = case milliseconds < 100 {
    True if milliseconds == 0 -> ""
    True if milliseconds < 10 -> ".00" <> int.to_string(milliseconds)
    True -> ".0" <> int.to_string(milliseconds)
    False -> "." <> int.to_string(milliseconds)
  }

  let time = hours <> ":" <> minutes <> ":" <> seconds <> msecs

  single_quote(time)
}

fn timestamp_to_string(ts: timestamp.Timestamp) -> String {
  timestamp.to_rfc3339(ts, calendar.utc_offset)
  |> single_quote
}

fn timestamptz_to_string(ts: timestamp.Timestamp, offset: Offset) -> String {
  offset_to_duration(offset)
  |> timestamp.add(ts, _)
  |> timestamp_to_string
}

fn offset_to_duration(offset: Offset) -> duration.Duration {
  let sign = case offset.hours < 0 {
    True -> 1
    False -> -1
  }

  int.absolute_value(offset.hours)
  |> int.multiply(60)
  |> int.add(offset.minutes)
  |> int.multiply(sign)
  |> duration.minutes
}

fn duration_to_string(dur: duration.Duration) -> String {
  duration.to_iso8601_string(dur)
  |> single_quote
}

fn single_quote(val: String) -> String {
  "'" <> val <> "'"
}

fn pad_zero(n: Int) -> String {
  case n < 10 {
    True -> "0" <> int.to_string(n)
    False -> int.to_string(n)
  }
}

// PG Types

/// Information about a PostgreSQL type. Needed for encoding and decoding Values.
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

/// Returns a TypeInfo record with the provided oid Int, with
/// all other values empty.
pub fn type_info(oid: Int) -> TypeInfo {
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

/// Sets the name of a TypeInfo record.
pub fn name(ti: TypeInfo, name: String) -> TypeInfo {
  TypeInfo(..ti, name:)
}

/// Sets the typesend value of a TypeInfo record.
pub fn typesend(ti: TypeInfo, typesend: String) -> TypeInfo {
  TypeInfo(..ti, typesend:)
}

/// Sets the typereceive value of a TypeInfo record.
pub fn typereceive(ti: TypeInfo, typereceive: String) -> TypeInfo {
  TypeInfo(..ti, typereceive:)
}

/// Sets the typelen value of a TypeInfo record.
pub fn typelen(ti: TypeInfo, typelen: Int) -> TypeInfo {
  TypeInfo(..ti, typelen:)
}

/// Sets the output value of a TypeInfo record.
pub fn output(ti: TypeInfo, output: String) -> TypeInfo {
  TypeInfo(..ti, output:)
}

/// Sets the input value of a TypeInfo record.
pub fn input(ti: TypeInfo, input: String) -> TypeInfo {
  TypeInfo(..ti, input:)
}

/// Sets the elem_oid value of a TypeInfo record.
pub fn elem_oid(ti: TypeInfo, elem_oid: Int) -> TypeInfo {
  TypeInfo(..ti, elem_oid:)
}

/// Sets the base_oid value of a TypeInfo record.
pub fn base_oid(ti: TypeInfo, base_oid: Int) -> TypeInfo {
  TypeInfo(..ti, base_oid:)
}

/// Sets the comp_oids value of a TypeInfo record.
pub fn comp_oids(ti: TypeInfo, comp_oids: List(Int)) -> TypeInfo {
  TypeInfo(..ti, comp_oids:)
}

/// Sets the elem_type value of a TypeInfo record.
pub fn elem_type(ti: TypeInfo, elem_type: Option(TypeInfo)) -> TypeInfo {
  TypeInfo(..ti, elem_type:)
}

/// Sets the comp_types value of a TypeInfo record.
pub fn comp_types(ti: TypeInfo, comp_types: Option(List(TypeInfo))) -> TypeInfo {
  TypeInfo(..ti, comp_types:)
}

// ---------- Encoding ---------- //

/// Encodes a Value as a PostgreSQL data type
pub fn encode(value: Value, ti: TypeInfo) -> Result(BitArray, String) {
  case value {
    Null -> encode_null(ti)
    Bool(val) -> encode_bool(val, ti)
    Int(val) -> encode_int(val, ti)
    Float(val) -> encode_float(val, ti)
    Text(val) -> encode_text(val, ti)
    Bytea(val) -> encode_bytea(val, ti)
    Time(val) -> encode_time(val, ti)
    Date(val) -> encode_date(val, ti)
    Timestamp(val) -> encode_timestamp(val, ti)
    Timestamptz(ts, offset) -> encode_timestamptz(ts, offset, ti)
    Interval(val) -> encode_interval(val, ti)
    Array(val) -> encode_array(val, ti)
  }
}

fn validate_typesend(
  expected: String,
  ti: TypeInfo,
  next: fn() -> Result(t, String),
) -> Result(t, String) {
  use <- bool.lazy_guard(when: expected == ti.typesend, return: next)

  Error("Attempted to encode " <> expected <> " as " <> ti.typesend)
}

fn encode_array(elems: List(Value), ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("array_send", ti)

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
    [Array(inner), ..] -> {
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

  let encoder = fn(bits) {
    let len = bit_array.byte_size(bits)

    Ok(<<len:big-int-size(32), bits:bits>>)
  }

  case encoded {
    [] -> encoder(header)
    _ -> bit_array.concat([header, ..encoded]) |> encoder
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

fn encode_null(_ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<-1:big-int-size(32)>>)
}

fn encode_bool(bool: Bool, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("boolsend", ti)

  case bool {
    True -> Ok(<<1:big-int-size(32), 1:big-int-size(8)>>)
    False -> Ok(<<1:big-int-size(32), 0:big-int-size(8)>>)
  }
}

fn encode_oid(num: Int, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("oidsend", ti)

  case 0 <= num && num <= oid_max {
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
      let message =
        "Attempted to encode " <> int.to_string(num) <> " as " <> ti.typesend

      Error(message)
    }
  }
}

fn encode_int2(num: Int, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("int2send", ti)

  case -int2_min <= num && num <= int2_max {
    True -> Ok(<<2:big-int-size(32), num:big-int-size(16)>>)
    False -> Error("Out of range for int2")
  }
}

fn encode_int4(num: Int, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("int4send", ti)

  case -int4_min <= num && num <= int4_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for int4")
  }
}

fn encode_int8(num: Int, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("int8send", ti)

  case -int8_min <= num && num <= int8_max {
    True -> Ok(<<8:big-int-size(32), num:big-int-size(64)>>)
    False -> Error("Out of range for int8")
  }
}

fn encode_float(num: Float, ti: TypeInfo) -> Result(BitArray, String) {
  case ti.typesend {
    "float4send" -> encode_float4(num, ti)
    "float8send" -> encode_float8(num, ti)
    _ -> Error("Unsupported float type")
  }
}

fn encode_float4(num: Float, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("float4send", ti)

  Ok(<<4:big-int-size(32), num:big-float-size(32)>>)
}

fn encode_float8(num: Float, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("float8send", ti)

  Ok(<<8:big-int-size(32), num:big-float-size(64)>>)
}

fn encode_text(text: String, ti: TypeInfo) -> Result(BitArray, String) {
  let encoder = fn(text) {
    let bits = bit_array.from_string(text)
    let len = bit_array.byte_size(bits)

    Ok(<<len:big-int-size(32), bits:bits>>)
  }

  case ti.typesend {
    "varcharsend" -> encoder(text)
    "textsend" -> encoder(text)
    "charsend" -> encoder(text)
    "namesend" -> encoder(text)
    _ -> Error("Attempted to encode '" <> text <> "' as " <> ti.typesend)
  }
}

fn encode_bytea(bits: BitArray, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("byteasend", ti)

  let len = bit_array.byte_size(bits)

  Ok(<<len:big-int-size(32), bits:bits>>)
}

fn encode_date(date: calendar.Date, ti: TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("date_send", ti)

  let gregorian_days =
    date_to_gregorian_days(
      date.year,
      calendar.month_to_int(date.month),
      date.day,
    )
  let pg_days = gregorian_days - postgres_gd_epoch

  Ok(<<4:big-int-size(32), pg_days:big-int-size(32)>>)
}

fn encode_time(
  tod: calendar.TimeOfDay,
  ti: TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("time_send", ti)

  let usecs =
    duration.hours(tod.hours)
    |> duration.add(duration.minutes(tod.minutes))
    |> duration.add(duration.seconds(tod.seconds))
    |> duration.add(duration.nanoseconds(tod.nanoseconds))
    |> to_microseconds(duration.to_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), usecs:big-int-size(64)>>)
}

fn encode_interval(
  dur: duration.Duration,
  ti: TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("interval_send", ti)

  let usecs = to_microseconds(dur, duration.to_seconds_and_nanoseconds)

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
  ti: TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("timestamp_send", ti)

  let ts_int =
    unix_seconds_before_postgres_epoch()
    |> timestamp.add(ts, _)
    |> to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), ts_int:big-int-size(64)>>)
}

fn encode_timestamptz(
  ts: timestamp.Timestamp,
  offset: Offset,
  ti: TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("timestamptz_send", ti)

  let offset_dur = offset_to_duration(offset)

  let ts_int =
    unix_seconds_before_postgres_epoch()
    |> duration.add(offset_dur)
    |> timestamp.add(ts, _)
    |> to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), ts_int:big-int-size(64)>>)
}

// ---------- Decoding ---------- //

/// Decodes binary PostgreSQL data into a Dynamic value.
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
  let pos_infinity = int8_max
  let neg_infinity = -int8_min

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
    |> int.add(postgres_gs_epoch)
    |> int.subtract(gs_to_unix_epoch)

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
  let seconds = usecs / usecs_per_sec
  let nanoseconds = { usecs % usecs_per_sec } * 1000

  let #(hours, minutes, seconds) = seconds_to_time(seconds)

  calendar.TimeOfDay(hours:, minutes:, seconds:, nanoseconds:)
}

fn days_to_date(days: Int) -> Result(calendar.Date, Nil) {
  let #(year, month, day) = gregorian_days_to_date(days + postgres_gd_epoch)

  calendar.month_from_int(month)
  |> result.map(fn(month) { calendar.Date(year:, month:, day:) })
}

fn to_microseconds(
  kind: a,
  to_seconds_and_nanoseconds: fn(a) -> #(Int, Int),
) -> Int {
  let #(seconds, nanoseconds) = to_seconds_and_nanoseconds(kind)

  { seconds * usecs_per_sec } + { nanoseconds / nsecs_per_usec }
}

fn unix_seconds_before_postgres_epoch() -> duration.Duration {
  gs_to_unix_epoch
  |> int.subtract(postgres_gs_epoch)
  |> duration.seconds
}

// constants

const oid_max = 0xFFFFFFFF

const int2_min = 0x8000

const int2_max = 0x7FFF

const int4_min = 0x80000000

const int4_max = 0x7FFFFFFF

const int8_max = 0x7FFFFFFFFFFFFFFF

const int8_min = 0x8000000000000000

// Seconds between Jan 1, 0001 and Dec 31, 1999
const postgres_gs_epoch = 63_113_904_000

// Seconds between Jan 1, 0001 and Jan 1, 1970
const gs_to_unix_epoch = 62_167_219_200

// Days between Jan 1, 0001 and Dec 31, 1999
const postgres_gd_epoch = 730_485

const usecs_per_sec = 1_000_000

const nsecs_per_usec = 1000

// FFI

@external(erlang, "calendar", "gregorian_days_to_date")
fn gregorian_days_to_date(days: Int) -> #(Int, Int, Int)

@external(erlang, "calendar", "seconds_to_time")
fn seconds_to_time(seconds: Int) -> #(Int, Int, Int)

@external(erlang, "calendar", "date_to_gregorian_days")
fn date_to_gregorian_days(year: Int, month: Int, day: Int) -> Int
