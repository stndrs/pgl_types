//// PostgreSQL values, along with their encoders and decoders. Can
//// be used by PostgreSQL client libraries written in gleam.
//// Currently used by [pgl][1].
////
//// [1]: https://github.com/stndrs/pgl

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
import pg_value/interval
import pg_value/type_info

/// `Offset` represents a UTC offset. `Timestamptz` is composed
/// of a [`gleam/time/timestamp.Timestamp`][1] and `Offset`. The offset will be
/// applied to the timestamp when being encoded.
///
/// A timestamp with a positive offset represents some time in
/// the future, relative to UTC.
/// A timestamp with a negative offset represents some time in
/// the past, relative to UTC.
///
/// `Offset`s will be subtracted from the `gleam/time/timestamp.Timestamp`
/// so the encoded value is a UTC timestamp.
///
/// [1]: https://hexdocs.pm/gleam_time/gleam/time/timestamp.html
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

/// The `Value` type represents PostgreSQL data types. Values can be encoded
/// to PostgreSQL's binary format. `Value`s can be used when interacting with
/// PostgreSQL databases through client libraries like [pgl][1].
///
/// [1]: https://github.com/stndrs/pgl
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
  Timestamptz(timestamp.Timestamp, Offset)
  Interval(interval.Interval)
  Array(List(Value))
  Uuid(BitArray)
}

pub const null = Null

pub const true = Bool(True)

pub const false = Bool(False)

pub fn bool(bool: Bool) -> Value {
  Bool(bool)
}

pub fn int(int: Int) -> Value {
  Int(int)
}

pub fn float(float: Float) -> Value {
  Float(float)
}

pub fn text(text: String) -> Value {
  Text(text)
}

pub fn bytea(bytea: BitArray) -> Value {
  Bytea(bytea)
}

/// Returns a UUID `Value`. Callers are responsible for ensuring the provided
/// value is a valid UUID. If the value is not a valid UUID, encoding will fail.
pub fn uuid(uuid: BitArray) -> Value {
  Uuid(uuid)
}

pub fn time(time_of_day: calendar.TimeOfDay) -> Value {
  Time(time_of_day)
}

pub fn date(date: calendar.Date) -> Value {
  Date(date)
}

pub fn timestamp(timestamp: timestamp.Timestamp) -> Value {
  Timestamp(timestamp)
}

pub fn timestamptz(timestamp: timestamp.Timestamp, offset: Offset) -> Value {
  Timestamptz(timestamp, offset)
}

pub fn interval(interval: interval.Interval) -> Value {
  Interval(interval)
}

pub fn array(elements: List(a), of kind: fn(a) -> Value) -> Value {
  elements
  |> list.map(kind)
  |> Array
}

/// Checks if the provided value is `option.Some` or `option.None`. If
/// `None` then the value returned is `value.Null`. If `Some` value is
/// provided then it is passed to the `inner_type` function.
///
/// Example:
///
/// ```gleam
///   let int = pg_value.nullable(pg_value.int, Some(10))
///
///   let null = pg_value.nullable(pg_value.int, None)
/// ```
pub fn nullable(inner_type: fn(a) -> Value, optional: Option(a)) -> Value {
  case optional {
    Some(term) -> inner_type(term)
    None -> Null
  }
}

/// Converts a `Value` to a string formatted properly for PostgreSQL
pub fn to_string(value: Value) -> String {
  case value {
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
    Interval(val) -> interval.to_iso8601_string(val) |> single_quote
    Array(vals) -> array_to_string(vals)
    Uuid(val) -> uuid_to_string(val)
  }
}

fn uuid_to_string(uuid: BitArray) -> String {
  do_uuid_to_string(uuid, 0, "", "-")
}

fn do_uuid_to_string(
  uuid: BitArray,
  position: Int,
  acc: String,
  separator: String,
) -> String {
  case position {
    8 | 13 | 18 | 23 ->
      do_uuid_to_string(uuid, position + 1, acc <> separator, separator)
    _ ->
      case uuid {
        <<i:size(4), rest:bits>> -> {
          let string = int.to_base16(i) |> string.lowercase
          do_uuid_to_string(rest, position + 1, acc <> string, separator)
        }
        _ -> acc
      }
  }
}

fn text_to_string(text: String) -> String {
  let val = string.replace(in: text, each: "'", with: "\\'")

  single_quote(val)
}

// https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
fn array_to_string(array: List(Value)) -> String {
  let elems = case array {
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
fn bool_to_string(bool: Bool) -> String {
  case bool {
    True -> "TRUE"
    False -> "FALSE"
  }
}

// https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
fn bytea_to_string(bytea: BitArray) -> String {
  let val = "\\x" <> bit_array.base16_encode(bytea)

  single_quote(val)
}

fn date_to_string(date: calendar.Date) -> String {
  let year = int.to_string(date.year)
  let month = calendar.month_to_int(date.month) |> pad_zero
  let day = pad_zero(date.day)

  let date = year <> "-" <> month <> "-" <> day

  single_quote(date)
}

fn time_to_string(time_of_day: calendar.TimeOfDay) -> String {
  let hours = pad_zero(time_of_day.hours)
  let minutes = pad_zero(time_of_day.minutes)
  let seconds = pad_zero(time_of_day.seconds)
  let milliseconds = time_of_day.nanoseconds / 1_000_000

  let msecs = case milliseconds < 100 {
    True if milliseconds == 0 -> ""
    True if milliseconds < 10 -> ".00" <> int.to_string(milliseconds)
    True -> ".0" <> int.to_string(milliseconds)
    False -> "." <> int.to_string(milliseconds)
  }

  let time = hours <> ":" <> minutes <> ":" <> seconds <> msecs

  single_quote(time)
}

fn timestamp_to_string(timestamp: timestamp.Timestamp) -> String {
  timestamp.to_rfc3339(timestamp, calendar.utc_offset)
  |> single_quote
}

fn timestamptz_to_string(
  timestamp: timestamp.Timestamp,
  offset: Offset,
) -> String {
  offset_to_duration(offset)
  |> timestamp.add(timestamp, _)
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

fn single_quote(value: String) -> String {
  "'" <> value <> "'"
}

fn pad_zero(n: Int) -> String {
  case n < 10 {
    True -> "0" <> int.to_string(n)
    False -> int.to_string(n)
  }
}

// ---------- Encoding ---------- //

/// Encodes a Value as a PostgreSQL data type
pub fn encode(
  value: Value,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  case value {
    Null -> encode_null()
    Bool(val) -> encode_bool(val, info)
    Int(val) -> encode_int(val, info)
    Float(val) -> encode_float(val, info)
    Text(val) -> encode_text(val, info)
    Bytea(val) -> encode_bytea(val, info)
    Time(val) -> encode_time(val, info)
    Date(val) -> encode_date(val, info)
    Timestamp(val) -> encode_timestamp(val, info)
    Timestamptz(ts, offset) -> encode_timestamptz(ts, offset, info)
    Interval(val) -> encode_interval(val, info)
    Array(val) -> encode_array(val, info)
    Uuid(val) -> encode_uuid(val, info)
  }
}

fn validate_typesend(
  expected: String,
  info: type_info.TypeInfo,
  next: fn() -> Result(t, String),
) -> Result(t, String) {
  use <- bool.lazy_guard(when: expected == info.typesend, return: next)

  Error("Attempted to encode " <> expected <> " as " <> info.typesend)
}

fn encode_uuid(
  uuid: BitArray,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("uuid_send", info)

  case uuid {
    <<uuid:big-int-size(128)>> ->
      Ok(<<16:big-int-size(32), uuid:big-int-size(128)>>)
    _ -> Error("Invalid UUID")
  }
}

fn encode_array(
  elems: List(Value),
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("array_send", info)

  let dimensions = arr_dims(elems)

  case info.elem_type {
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
  info: type_info.TypeInfo,
  encoded: List(BitArray),
) -> Result(BitArray, String) {
  let header = array_header(dimensions, has_nulls, info.oid)

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

fn encode_null() -> Result(BitArray, String) {
  Ok(<<-1:big-int-size(32)>>)
}

fn encode_bool(bool: Bool, info: type_info.TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("boolsend", info)

  case bool {
    True -> Ok(<<1:big-int-size(32), 1:big-int-size(8)>>)
    False -> Ok(<<1:big-int-size(32), 0:big-int-size(8)>>)
  }
}

fn encode_oid(num: Int, info: type_info.TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("oidsend", info)

  case 0 <= num && num <= oid_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for oid")
  }
}

fn encode_int(num: Int, info: type_info.TypeInfo) -> Result(BitArray, String) {
  case info.typesend {
    "oidsend" -> encode_oid(num, info)
    "int2send" -> encode_int2(num, info)
    "int4send" -> encode_int4(num, info)
    "int8send" -> encode_int8(num, info)
    _ -> {
      let message =
        "Attempted to encode " <> int.to_string(num) <> " as " <> info.typesend

      Error(message)
    }
  }
}

fn encode_int2(num: Int, info: type_info.TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("int2send", info)

  case -int2_min <= num && num <= int2_max {
    True -> Ok(<<2:big-int-size(32), num:big-int-size(16)>>)
    False -> Error("Out of range for int2")
  }
}

fn encode_int4(num: Int, info: type_info.TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("int4send", info)

  case -int4_min <= num && num <= int4_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for int4")
  }
}

fn encode_int8(num: Int, info: type_info.TypeInfo) -> Result(BitArray, String) {
  use <- validate_typesend("int8send", info)

  case -int8_min <= num && num <= int8_max {
    True -> Ok(<<8:big-int-size(32), num:big-int-size(64)>>)
    False -> Error("Out of range for int8")
  }
}

fn encode_float(
  num: Float,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  case info.typesend {
    "float4send" -> encode_float4(num, info)
    "float8send" -> encode_float8(num, info)
    _ -> Error("Unsupported float type")
  }
}

fn encode_float4(
  num: Float,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("float4send", info)

  Ok(<<4:big-int-size(32), num:big-float-size(32)>>)
}

fn encode_float8(
  num: Float,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("float8send", info)

  Ok(<<8:big-int-size(32), num:big-float-size(64)>>)
}

fn encode_text(
  text: String,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  let encoder = fn(text) {
    let bits = bit_array.from_string(text)
    let len = bit_array.byte_size(bits)

    Ok(<<len:big-int-size(32), bits:bits>>)
  }

  case info.typesend {
    "varcharsend" -> encoder(text)
    "textsend" -> encoder(text)
    "charsend" -> encoder(text)
    "namesend" -> encoder(text)
    _ -> Error("Attempted to encode '" <> text <> "' as " <> info.typesend)
  }
}

fn encode_bytea(
  bits: BitArray,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("byteasend", info)

  let len = bit_array.byte_size(bits)

  Ok(<<len:big-int-size(32), bits:bits>>)
}

fn encode_date(
  date: calendar.Date,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("date_send", info)

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
  time_of_day: calendar.TimeOfDay,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("time_send", info)

  let usecs =
    duration.hours(time_of_day.hours)
    |> duration.add(duration.minutes(time_of_day.minutes))
    |> duration.add(duration.seconds(time_of_day.seconds))
    |> duration.add(duration.nanoseconds(time_of_day.nanoseconds))
    |> to_microseconds(duration.to_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), usecs:big-int-size(64)>>)
}

fn encode_interval(
  interval: interval.Interval,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("interval_send", info)

  let interval.Interval(months:, days:, seconds:, microseconds:) = interval

  let usecs = { seconds * usecs_per_sec } + microseconds

  let encoded = <<
    16:big-int-size(32),
    usecs:big-int-size(64),
    days:big-int-size(32),
    months:big-int-size(32),
  >>

  Ok(encoded)
}

fn encode_timestamp(
  timestamp: timestamp.Timestamp,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("timestamp_send", info)

  let timestamp_int =
    unix_seconds_before_postgres_epoch()
    |> timestamp.add(timestamp, _)
    |> to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), timestamp_int:big-int-size(64)>>)
}

fn encode_timestamptz(
  timestamp: timestamp.Timestamp,
  offset: Offset,
  info: type_info.TypeInfo,
) -> Result(BitArray, String) {
  use <- validate_typesend("timestamptz_send", info)

  let offset_dur = offset_to_duration(offset)

  let timestamp_int =
    unix_seconds_before_postgres_epoch()
    |> duration.add(offset_dur)
    |> timestamp.add(timestamp, _)
    |> to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), timestamp_int:big-int-size(64)>>)
}

// ---------- Decoding ---------- //

/// Decodes binary PostgreSQL data into a Dynamic value. Dynamic values
/// can then be decoded using [gleam/dynamic/decode][1].
///
/// [1]: https://hexdocs.pm/gleam_stdlib/gleam/dynamic/decode.html
pub fn decode(
  bits: BitArray,
  info: type_info.TypeInfo,
) -> Result(Dynamic, String) {
  case info.typereceive {
    "array_recv" ->
      decode_array(bits, with: fn(elem) {
        case info.elem_type {
          Some(elem_ti) -> decode(elem, elem_ti)
          None -> Error("elem type missing")
        }
      })
    "boolrecv" -> decode_bool(bits)
    "oidrecv" -> decode_oid(bits)
    "int2recv" -> decode_int2(bits)
    "int4recv" -> decode_int4(bits)
    "int8recv" -> decode_int8(bits)
    "float4recv" -> decode_float4(bits)
    "float8recv" -> decode_float8(bits)
    "textrecv" -> decode_text(bits)
    "varcharrecv" -> decode_varchar(bits)
    "namerecv" -> decode_text(bits)
    "charrecv" -> decode_text(bits)
    "bytearecv" -> decode_bytea(bits)
    "uuid_recv" -> decode_uuid(bits)
    "time_recv" -> decode_time(bits)
    "date_recv" -> decode_date(bits)
    "timestamp_recv" -> decode_timestamp(bits)
    "timestamptz_recv" -> decode_timestamp(bits)
    "interval_recv" -> decode_interval(bits)
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

fn decode_uuid(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<_uuid:big-int-size(128)>> -> Ok(dynamic.bit_array(bits))
    _ -> Error("invalid uuid")
  }
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
      days:big-signed-int-size(32),
      months:big-signed-int-size(32),
    >> -> {
      dynamic.array([
        dynamic.int(months),
        dynamic.int(days),
        dynamic.int(microseconds),
      ])
      |> Ok
    }
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

@external(erlang, "calendar", "gregorian_days_to_date")
fn gregorian_days_to_date(days: Int) -> #(Int, Int, Int)

@external(erlang, "calendar", "seconds_to_time")
fn seconds_to_time(seconds: Int) -> #(Int, Int, Int)

@external(erlang, "calendar", "date_to_gregorian_days")
fn date_to_gregorian_days(year: Int, month: Int, day: Int) -> Int
