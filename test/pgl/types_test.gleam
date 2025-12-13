import gleam/dynamic
import gleam/function
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import pgl/types
import pgl/types/internal
import pgl/value

pub fn decode_timestamp_test() {
  let ts_value =
    { 1 - internal.postgres_gs_epoch + internal.gs_to_unix_epoch } * 1_000_000

  let in = <<ts_value:big-int-size(64)>>
  let out = dynamic.int(1_000_000)

  let assert Ok(ts) = types.decode(in, timestamp())
  let assert True = out == ts
}

pub fn decode_timestamp_pos_infinity_test() {
  let in = <<internal.int8_max:big-int-size(64)>>
  let out = dynamic.string("infinity")

  let assert Ok(ts) = types.decode(in, timestamp())

  let assert True = out == ts
}

pub fn decode_timestamp_neg_infinity_test() {
  let in = <<-internal.int8_min:big-int-size(64)>>
  let out = dynamic.string("-infinity")

  let assert Ok(ts) = types.decode(in, timestamp())

  let assert True = out == ts
}

pub fn decode_oid_test() {
  use valid <- list.map([23, 1042, 0])

  let in = <<valid:big-int-size(32)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, oid())

  let assert True = out == result
}

pub fn decode_bool_test() {
  use #(byte, expected) <- list.map([#(1, True), #(0, False)])

  let in = <<byte:big-int-size(8)>>
  let out = dynamic.bool(expected)

  let assert Ok(result) = types.decode(in, bool())

  let assert True = out == result
}

pub fn decode_int2_test() {
  use valid <- list.map([32_767, 0, -32_768])

  let in = <<valid:big-int-size(16)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, int2())

  let assert True = out == result
}

pub fn decode_int2_error_test() {
  let in = <<1:big-int-size(8)>>
  let out = "invalid int2"

  let assert Error(msg) = types.decode(in, int2())

  let assert True = out == msg
}

pub fn decode_int4_test() {
  use valid <- list.map([2_147_483_647, 0, -2_147_483_648])

  let in = <<valid:big-int-size(32)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, int4())

  let assert True = out == result
}

pub fn decode_int4_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid int4"

  let assert Error(msg) = types.decode(in, int4())

  let assert True = out == msg
}

pub fn decode_int8_test() {
  use valid <- list.map([
    9_223_372_036_854_775_807,
    0,
    -9_223_372_036_854_775_808,
  ])

  let in = <<valid:big-int-size(64)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, int8())

  let assert True = out == result
}

pub fn decode_int8_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid int8"

  let assert Error(msg) = types.decode(in, int8())

  let assert True = out == msg
}

pub fn decode_float4_test() {
  use valid <- list.map([0.0, 3.14, -42.5])

  let in = <<valid:big-float-size(32)>>
  let out = dynamic.float(valid)

  let assert Ok(result) = types.decode(in, float4())

  let assert True = out == result
}

pub fn decode_float4_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid float4"

  let assert Error(msg) = types.decode(in, float4())

  let assert True = out == msg
}

pub fn decode_float8_test() {
  use valid <- list.map([0.0, 3.14159, -42.989283])

  let in = <<valid:big-float-size(64)>>
  let out = dynamic.float(valid)

  let assert Ok(result) = types.decode(in, float8())

  let assert True = out == result
}

pub fn decode_float8_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid float8"

  let assert Error(msg) = types.decode(in, float8())

  let assert True = out == msg
}

pub fn decode_varchar_test() {
  use valid <- list.map(["hello", "", "PostgreSQL"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, varchar())

  let assert True = out == result
}

pub fn decode_varchar_error_test() {
  let in = <<255, 255, 255, 255>>
  let out = "invalid varchar"

  let assert Error(msg) = types.decode(in, varchar())

  let assert True = out == msg
}

pub fn decode_text_test() {
  use valid <- list.map(["hello world", "", "PostgreSQL Database"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, text())

  let assert True = out == result
}

pub fn decode_text_error_test() {
  let in = <<255, 255, 255, 255>>
  let out = "invalid text"

  let assert Error(msg) = types.decode(in, text())

  let assert True = out == msg
}

pub fn decode_bytea_test() {
  use valid <- list.map([<<1, 2, 3, 4, 5>>, <<>>, <<255, 0, 128>>])

  let in = valid
  let out = dynamic.bit_array(valid)

  let assert Ok(result) = types.decode(in, bytea())

  let assert True = out == result
}

pub fn decode_char_test() {
  use valid <- list.map(["A", "x"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, char())

  let assert True = out == result
}

pub fn decode_name_test() {
  use valid <- list.map(["table_name", "", "column_name"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, name())

  let assert True = out == result
}

pub fn decode_time_test() {
  use #(microseconds, expected) <- list.map([
    #(
      79_000_000,
      dynamic.array([
        dynamic.int(0),
        dynamic.int(1),
        dynamic.int(19),
        dynamic.int(0),
      ]),
    ),
    #(
      0,
      dynamic.array([
        dynamic.int(0),
        dynamic.int(0),
        dynamic.int(0),
        dynamic.int(0),
      ]),
    ),
    #(
      86_399_000_000,
      dynamic.array([
        dynamic.int(23),
        dynamic.int(59),
        dynamic.int(59),
        dynamic.int(0),
      ]),
    ),
  ])

  let in = <<microseconds:big-int-size(64)>>

  let assert Ok(result) = types.decode(in, time())

  let assert True = expected == result
}

pub fn decode_time_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid time"

  let assert Error(msg) = types.decode(in, time())

  let assert True = out == msg
}

pub fn decode_date_test() {
  use #(days, expected) <- list.map([
    #(-10_957, [1970, 1, 1]),
    #(0, [2000, 1, 1]),
    #(366, [2001, 1, 1]),
  ])

  let in = <<days:big-int-size(32)>>
  let out = dynamic.array(list.map(expected, dynamic.int))

  let assert Ok(result) = types.decode(in, date())

  let assert True = out == result
}

pub fn decode_date_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid date"

  let assert Error(msg) = types.decode(in, date())

  let assert True = out == msg
}

pub fn array_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid array"

  let assert Error(msg) = types.decode(in, array(int2()))

  let assert True = out == msg
}

// Encode tests //

pub fn encode_bool_test() {
  use valid <- list.map([#(value.true, 1), #(value.false, 0)])

  let in = valid.0
  let expected = <<1:big-int-size(32), valid.1:big-int-size(8)>>

  let assert Ok(out) = types.encode(in, bool())

  let assert True = expected == out
}

pub fn encode_int2_test() {
  use valid <- list.map([32_767, 0, -32_768])

  let in = value.int(valid)
  let expected = <<2:big-int-size(32), valid:big-int-size(16)>>

  let assert Ok(out) = types.encode(in, int2())

  let assert True = expected == out
}

pub fn encode_int2_error_test() {
  use invalid <- list.map([-100_000, 100_000, 32_768, -32_769])

  let in = value.int(invalid)
  let expected = "Out of range for int2"

  let assert Error(msg) = types.encode(in, int2())

  let assert True = expected == msg
}

pub fn encode_int4_test() {
  use valid <- list.map([2_147_483_647, 0, -2_147_483_648])

  let in = value.int(valid)
  let expected = <<4:big-int-size(32), valid:big-int-size(32)>>

  let assert Ok(out) = types.encode(in, int4())

  let assert True = expected == out
}

pub fn encode_int4_error_test() {
  use invalid <- list.map([2_147_483_648, -2_147_483_649])

  let in = value.int(invalid)
  let expected = "Out of range for int4"

  let assert Error(msg) = types.encode(in, int4())

  let assert True = expected == msg
}

pub fn encode_int8_test() {
  use valid <- list.map([
    9_223_372_036_854_775_807,
    0,
    -9_223_372_036_854_775_808,
  ])

  let in = value.int(valid)
  let expected = <<8:big-int-size(32), valid:big-int-size(64)>>

  let assert Ok(out) = types.encode(in, int8())

  let assert True = expected == out
}

pub fn encode_int8_error_test() {
  use invalid <- list.map([
    9_223_372_036_854_775_807 + 1,
    -9_223_372_036_854_775_808 - 1,
  ])

  let in = value.int(invalid)
  let expected = "Out of range for int8"

  let assert Error(msg) = types.encode(in, int8())

  let assert True = expected == msg
}

pub fn encode_float4_test() {
  use valid <- list.map([0.0, 1.0, -1.0, 3.14, -42.5, 1.23e38])

  let in = value.float(valid)
  let expected = <<4:big-int-size(32), valid:float-size(32)>>

  let assert Ok(out) = types.encode(in, float4())

  let assert True = expected == out
}

pub fn encode_float8_test() {
  use valid <- list.map([0.0, 1.0, -1.0, 3.14, -42.5, 1.23e308])

  let in = value.float(valid)
  let expected = <<8:big-int-size(32), valid:float-size(64)>>

  let assert Ok(out) = types.encode(in, float8())

  let assert True = expected == out
}

pub fn encode_oid_test() {
  use valid <- list.map([0, 1042, 4_294_967_295])

  let in = value.int(valid)
  let expected = <<4:big-int-size(32), valid:big-int-size(32)>>

  let assert Ok(out) = types.encode(in, oid())

  let assert True = expected == out
}

pub fn encode_oid_error_test() {
  use invalid <- list.map([-1, 4_294_967_296])

  let in = value.int(invalid)
  let expected = "Out of range for oid"

  let assert Error(msg) = types.encode(in, oid())

  let assert True = expected == msg
}

pub fn encode_varchar_test() {
  use valid <- list.map([#("hello", 5), #("", 0), #("PostgreSQL", 10)])

  let in = value.text(valid.0)
  let expected = <<valid.1:big-int-size(32), valid.0:utf8>>

  let assert Ok(out) = types.encode(in, varchar())

  let assert True = expected == out
}

pub fn encode_date_test() {
  let assert Ok(#(in, _tod)) =
    timestamp.parse_rfc3339("1970-01-01T00:00:00Z")
    |> result.map(timestamp.to_calendar(_, calendar.utc_offset))

  let expected = <<4:big-int-size(32), -10_957:big-int-size(32)>>

  let assert Ok(out) = types.encode(value.date(in), date())

  let assert True = expected == out
}

pub fn encode_time_test() {
  let tod =
    calendar.TimeOfDay(hours: 0, minutes: 1, seconds: 19, nanoseconds: 0)

  let in = value.time(tod)
  let expected = <<8:big-int-size(32), 79_000_000:big-int-size(64)>>

  let assert Ok(out) = types.encode(in, time())

  let assert True = expected == out
}

pub fn encode_timestamp_test() {
  let ts = timestamp.from_unix_seconds(1)

  let in = value.timestamp(ts)
  let expected = <<8:big-int-size(32), -946_684_799_000_000:big-int-size(64)>>

  let assert Ok(out) = types.encode(in, timestamp())

  let assert True = expected == out
}

fn microseconds(count: Int) -> duration.Duration {
  count
  |> int.multiply(1000)
  |> duration.nanoseconds
}

fn to_microseconds(
  kind: a,
  to_seconds_and_nanoseconds: fn(a) -> #(Int, Int),
) -> Int {
  let #(seconds, nanoseconds) = to_seconds_and_nanoseconds(kind)

  { seconds * internal.usecs_per_sec }
  + { nanoseconds / internal.nsecs_per_usec }
}

pub fn encode_interval_test() {
  let usecs =
    duration.hours(24 * 14)
    |> duration.add(microseconds(79_000))

  let microseconds =
    usecs
    |> to_microseconds(duration.to_seconds_and_nanoseconds)

  let expected = <<
    16:big-int-size(32),
    microseconds:big-int-size(64),
    0:big-int-size(32),
    0:big-int-size(32),
  >>

  let assert Ok(out) = types.encode(value.interval(usecs), interval())

  let assert True = expected == out
}

pub fn encode_timestamptz_test() {
  let expected_utc_int = -946_684_799_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset = duration.hours(0)
  let expected = <<
    8:big-int-size(32),
    expected_utc_int:big-int-size(64),
  >>

  let in = timestamp.add(ts, offset) |> value.timestamp

  let assert Ok(out) = types.encode(in, timestamptz())

  let assert True = expected == out
}

pub fn encode_positive_offtimestamptz_test() {
  let expected_utc_int = -946_684_800_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset = duration.hours(10)
  let ten_hours =
    offset
    |> timestamp.add(ts, _)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)
    |> int.add(expected_utc_int)

  let expected = <<
    8:big-int-size(32),
    ten_hours:big-int-size(64),
  >>

  let in = timestamp.add(ts, offset) |> value.timestamp

  let assert Ok(out) = types.encode(in, timestamptz())

  let assert True = expected == out
}

pub fn encode_negative_offtimestamptz_test() {
  let expected_utc_int = -946_684_800_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset =
    duration.hours(-2)
    |> duration.add(duration.minutes(30))

  let minus_two_thirty =
    offset
    |> timestamp.add(ts, _)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)
    |> int.add(expected_utc_int)

  let expected = <<
    8:big-int-size(32),
    minus_two_thirty:big-int-size(64),
  >>

  let in = timestamp.add(ts, offset) |> value.timestamp

  let assert Ok(out) = types.encode(in, timestamptz())

  let assert True = expected == out
}

pub fn empty_array_test() {
  let expected = <<
    12:big-int-size(32), 0:big-int-size(32), 0:big-int-size(32),
    21:big-int-size(32),
  >>

  let assert Ok(out) =
    types.encode(value.array([], of: value.int), array(int2()))

  let assert True = expected == out
}

pub fn string_array_test() {
  let in = value.array(["hello", "world"], of: value.text)

  let expected = <<
    38:big-int-size(32), 1:big-int-size(32), 0:big-int-size(32),
    25:big-int-size(32), 2:big-int-size(32), 1:big-int-size(32),
    5:big-int-size(32), "hello":utf8, 5:big-int-size(32), "world":utf8,
  >>

  let assert Ok(out) = types.encode(in, array(text()))

  let assert True = expected == out
}

pub fn int_array_test() {
  let in = value.array([42], of: value.int)
  let expected = <<
    28:big-int-size(32), 1:big-int-size(32), 0:big-int-size(32),
    23:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    4:big-int-size(32), 42:big-int-size(32),
  >>

  let assert Ok(out) = types.encode(in, array(int4()))

  let assert True = expected == out
}

pub fn null_array_test() {
  let in = value.array([value.null], of: function.identity)

  let expected = <<
    24:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    23:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    -1:big-int-size(32),
  >>

  let assert Ok(out) = types.encode(in, array(int4()))

  let assert True = expected == out
}

pub fn nested_array_test() {
  let in = value.Array([value.array([12, 23], of: value.int)])

  let expected = <<
    // total size of encoded array
    68:big-int-size(32),
    // number of dimensions
    2:big-int-size(32),
    // flags (has_nulls)
    0:big-int-size(32),
    // element OID
    143:big-int-size(32),
    // size of first dimension
    1:big-int-size(32),
    // lower bound
    1:big-int-size(32),
    // size of second dimension
    2:big-int-size(32),
    // lower bound
    1:big-int-size(32),
    // elems (nested array)
    36:big-int-size(32),
    // number of dimensions
    1:big-int-size(32),
    // flags (has_nulls)
    0:big-int-size(32),
    // element OID
    23:big-int-size(32),
    // size of second dimension
    2:big-int-size(32),
    // lower bound
    1:big-int-size(32),
    // elems (int4)
    // size of element
    4:big-int-size(32),
    // element
    12:big-int-size(32),
    // size of element
    4:big-int-size(32),
    // element
    23:big-int-size(32),
  >>

  let assert Ok(out) = types.encode(in, array(array(int4())))

  let assert True = expected == out
}

// TypeInfo helpers

fn oid() {
  types.info(26)
  |> types.typesend("oidsend")
  |> types.typereceive("oidrecv")
}

fn bool() {
  types.info(16)
  |> types.typesend("boolsend")
  |> types.typereceive("boolrecv")
}

fn int2() {
  types.info(21)
  |> types.typesend("int2send")
  |> types.typereceive("int2recv")
}

fn int4() {
  types.info(23)
  |> types.typesend("int4send")
  |> types.typereceive("int4recv")
}

fn int8() {
  types.info(20)
  |> types.typesend("int8send")
  |> types.typereceive("int8recv")
}

fn float4() {
  types.info(700)
  |> types.typesend("float4send")
  |> types.typereceive("float4recv")
}

fn float8() {
  types.info(701)
  |> types.typesend("float8send")
  |> types.typereceive("float8recv")
}

fn varchar() {
  types.info(1043)
  |> types.typesend("varcharsend")
  |> types.typereceive("varcharrecv")
}

fn text() {
  types.info(25)
  |> types.typesend("textsend")
  |> types.typereceive("textrecv")
}

fn bytea() {
  types.info(17)
  |> types.typesend("byteasend")
  |> types.typereceive("bytearecv")
}

fn char() {
  types.info(18)
  |> types.typesend("charsend")
  |> types.typereceive("charrecv")
}

fn name() {
  types.info(19)
  |> types.typesend("namesend")
  |> types.typereceive("namerecv")
}

fn time() {
  types.info(1083)
  |> types.typesend("time_send")
  |> types.typereceive("time_recv")
}

fn date() {
  types.info(1082)
  |> types.typesend("date_send")
  |> types.typereceive("date_recv")
}

fn timestamp() {
  types.info(1114)
  |> types.typesend("timestamp_send")
  |> types.typereceive("timestamp_recv")
}

fn timestamptz() {
  types.info(1184)
  |> types.typesend("timestamptz_send")
  |> types.typereceive("timestamptz_recv")
}

fn interval() {
  types.info(1186)
  |> types.typesend("interval_send")
  |> types.typereceive("interval_recv")
}

fn array(ti: types.TypeInfo) -> types.TypeInfo {
  types.info(143)
  |> types.typesend("array_send")
  |> types.typereceive("array_recv")
  |> types.elem_type(Some(ti))
}
