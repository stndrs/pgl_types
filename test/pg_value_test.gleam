import gleam/dynamic
import gleam/function
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import gleeunit
import pg_value as value
import pg_value/interval
import pg_value/type_info

pub fn main() -> Nil {
  gleeunit.main()
}

// Value tests

pub fn null_to_string_test() {
  assert "NULL" == value.null |> value.to_string
}

pub fn bool_to_string_test() {
  assert "TRUE" == value.bool(True) |> value.to_string
  assert "FALSE" == value.bool(False) |> value.to_string
}

pub fn int_to_string_test() {
  assert "42" == value.int(42) |> value.to_string
  assert "0" == value.int(0) |> value.to_string
  assert "-123" == value.int(-123) |> value.to_string
}

pub fn float_to_string_test() {
  assert "3.14" == value.float(3.14) |> value.to_string
  assert "0.0" == value.float(0.0) |> value.to_string
  assert "-2.5" == value.float(-2.5) |> value.to_string
}

pub fn text_to_string_test() {
  assert "'hello'" == value.text("hello") |> value.to_string
  assert "''" == value.text("") |> value.to_string
  assert "'It\\'s working'" == value.text("It's working") |> value.to_string
  assert "'Say \\'hello\\''" == value.text("Say 'hello'") |> value.to_string
}

pub fn bytea_to_string_test() {
  assert "'\\x48656C6C6F'" == value.bytea(<<"Hello":utf8>>) |> value.to_string
  assert "'\\x'" == value.bytea(<<>>) |> value.to_string
  assert "'\\xDEADBEEF'"
    == value.bytea(<<0xDE, 0xAD, 0xBE, 0xEF>>) |> value.to_string
}

pub fn time_to_string_test() {
  assert "'14:30:45'"
    == value.time(calendar.TimeOfDay(14, 30, 45, 0)) |> value.to_string
  assert "'00:00:00'"
    == value.time(calendar.TimeOfDay(0, 0, 0, 0)) |> value.to_string
  assert "'23:59:59.123'"
    == value.time(calendar.TimeOfDay(23, 59, 59, 123_456_000))
    |> value.to_string
  assert "'09:05:03'"
    == value.time(calendar.TimeOfDay(9, 5, 3, 0)) |> value.to_string
  assert "'09:05:03.400'"
    == value.time(calendar.TimeOfDay(9, 5, 3, 400_000_000))
    |> value.to_string
  assert "'09:05:03.012'"
    == value.time(calendar.TimeOfDay(9, 5, 3, 12_000_000)) |> value.to_string
  assert "'09:05:03.007'"
    == value.time(calendar.TimeOfDay(9, 5, 3, 7_000_000)) |> value.to_string
}

pub fn date_to_string_test() {
  assert "'2025-01-15'"
    == value.date(calendar.Date(2025, calendar.January, 15))
    |> value.to_string
  assert "'1990-02-09'"
    == value.date(calendar.Date(1990, calendar.February, 9))
    |> value.to_string
  assert "'2000-12-31'"
    == value.date(calendar.Date(2000, calendar.December, 31))
    |> value.to_string
}

pub fn timestamp_to_string_test() {
  let assert Ok(ts) = timestamp.parse_rfc3339("2025-01-15T14:30:45Z")
  assert "'2025-01-15T14:30:45Z'" == value.timestamp(ts) |> value.to_string

  let assert Ok(ts2) = timestamp.parse_rfc3339("2000-12-31T23:59:59.123456789Z")
  assert "'2000-12-31T23:59:59.123456789Z'"
    == value.timestamp(ts2) |> value.to_string
}

pub fn timestamptz_to_string_test() {
  let assert Ok(ts) = timestamp.parse_rfc3339("2025-01-15T14:30:45Z")
  let offset = value.offset(0)

  assert "'2025-01-15T14:30:45Z'"
    == value.timestamptz(ts, offset) |> value.to_string

  let assert Ok(ts2) = timestamp.parse_rfc3339("2000-12-31T23:59:59.123456789Z")
  let offset = value.offset(0)

  assert "'2000-12-31T23:59:59.123456789Z'"
    == value.timestamptz(ts2, offset) |> value.to_string
}

pub fn timestamptz_with_positive_offset_to_string_test() {
  let assert Ok(ts) = timestamp.parse_rfc3339("2025-01-15T14:30:45Z")
  let offset = value.offset(10) |> value.minutes(30)

  assert "'2025-01-15T04:00:45Z'"
    == value.timestamptz(ts, offset) |> value.to_string
}

pub fn timestamptz_with_negative_offset_to_string_test() {
  let assert Ok(ts) = timestamp.parse_rfc3339("2025-01-15T14:30:45Z")
  let offset =
    value.offset(-6)
    |> value.minutes(30)

  assert "'2025-01-15T21:00:45Z'"
    == value.timestamptz(ts, offset) |> value.to_string
}

pub fn interval_to_string_test() {
  assert "'P1DT300S'"
    == interval.Interval(months: 0, days: 1, seconds: 300, microseconds: 0)
    |> value.interval
    |> value.to_string

  assert "'PT0S'"
    == interval.seconds(0)
    |> value.interval
    |> value.to_string

  assert "'P5MT30S'"
    == interval.months(5)
    |> interval.add(interval.seconds(30))
    |> value.interval
    |> value.to_string
}

// Decode tests

const postgres_gs_epoch = 63_113_904_000

const gs_to_unix_epoch = 62_167_219_200

const int8_max = 0x7FFFFFFFFFFFFFFF

const int8_min = 0x8000000000000000

const usecs_per_sec = 1_000_000

const nsecs_per_usec = 1000

pub fn decode_timestamp_test() {
  let ts_value = { 1 - postgres_gs_epoch + gs_to_unix_epoch } * 1_000_000

  let in = <<ts_value:big-int-size(64)>>
  let out = dynamic.int(1_000_000)

  let assert Ok(ts) = value.decode(in, timestamp())
  assert out == ts
}

pub fn decode_timestamp_pos_infinity_test() {
  let in = <<int8_max:big-int-size(64)>>
  let out = dynamic.string("infinity")

  let assert Ok(ts) = value.decode(in, timestamp())

  assert out == ts
}

pub fn decode_timestamp_neg_infinity_test() {
  let in = <<-int8_min:big-int-size(64)>>
  let out = dynamic.string("-infinity")

  let assert Ok(ts) = value.decode(in, timestamp())

  assert out == ts
}

pub fn decode_oid_test() {
  use valid <- list.map([23, 1042, 0])

  let in = <<valid:big-int-size(32)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = value.decode(in, oid())

  assert out == result
}

pub fn decode_bool_test() {
  use #(byte, expected) <- list.map([#(1, True), #(0, False)])

  let in = <<byte:big-int-size(8)>>
  let out = dynamic.bool(expected)

  let assert Ok(result) = value.decode(in, bool())

  assert out == result
}

pub fn decode_int2_test() {
  use valid <- list.map([32_767, 0, -32_768])

  let in = <<valid:big-int-size(16)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = value.decode(in, int2())

  assert out == result
}

pub fn decode_int2_error_test() {
  let in = <<1:big-int-size(8)>>
  let out = "invalid int2"

  let assert Error(msg) = value.decode(in, int2())

  assert out == msg
}

pub fn decode_int4_test() {
  use valid <- list.map([2_147_483_647, 0, -2_147_483_648])

  let in = <<valid:big-int-size(32)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = value.decode(in, int4())

  assert out == result
}

pub fn decode_int4_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid int4"

  let assert Error(msg) = value.decode(in, int4())

  assert out == msg
}

pub fn decode_int8_test() {
  use valid <- list.map([
    9_223_372_036_854_775_807,
    0,
    -9_223_372_036_854_775_808,
  ])

  let in = <<valid:big-int-size(64)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = value.decode(in, int8())

  assert out == result
}

pub fn decode_int8_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid int8"

  let assert Error(msg) = value.decode(in, int8())

  assert out == msg
}

pub fn decode_float4_test() {
  use valid <- list.map([0.0, 3.14, -42.5])

  let in = <<valid:big-float-size(32)>>
  let out = dynamic.float(valid)

  let assert Ok(result) = value.decode(in, float4())

  assert out == result
}

pub fn decode_float4_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid float4"

  let assert Error(msg) = value.decode(in, float4())

  assert out == msg
}

pub fn decode_float8_test() {
  use valid <- list.map([0.0, 3.14159, -42.989283])

  let in = <<valid:big-float-size(64)>>
  let out = dynamic.float(valid)

  let assert Ok(result) = value.decode(in, float8())

  assert out == result
}

pub fn decode_float8_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid float8"

  let assert Error(msg) = value.decode(in, float8())

  assert out == msg
}

pub fn decode_varchar_test() {
  use valid <- list.map(["hello", "", "PostgreSQL"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = value.decode(in, varchar())

  assert out == result
}

pub fn decode_varchar_error_test() {
  let in = <<255, 255, 255, 255>>
  let out = "invalid varchar"

  let assert Error(msg) = value.decode(in, varchar())

  assert out == msg
}

pub fn decode_text_test() {
  use valid <- list.map(["hello world", "", "PostgreSQL Database"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = value.decode(in, text())

  assert out == result
}

pub fn decode_text_error_test() {
  let in = <<255, 255, 255, 255>>
  let out = "invalid text"

  let assert Error(msg) = value.decode(in, text())

  assert out == msg
}

pub fn decode_bytea_test() {
  use valid <- list.map([<<1, 2, 3, 4, 5>>, <<>>, <<255, 0, 128>>])

  let in = valid
  let out = dynamic.bit_array(valid)

  let assert Ok(result) = value.decode(in, bytea())

  assert out == result
}

pub fn decode_char_test() {
  use valid <- list.map(["A", "x"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = value.decode(in, char())

  assert out == result
}

pub fn decode_name_test() {
  use valid <- list.map(["table_name", "", "column_name"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = value.decode(in, name())

  assert out == result
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

  let assert Ok(result) = value.decode(in, time())

  assert expected == result
}

pub fn decode_time_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid time"

  let assert Error(msg) = value.decode(in, time())

  assert out == msg
}

pub fn decode_date_test() {
  use #(days, expected) <- list.map([
    #(-10_957, [1970, 1, 1]),
    #(0, [2000, 1, 1]),
    #(366, [2001, 1, 1]),
  ])

  let in = <<days:big-int-size(32)>>
  let out = dynamic.array(list.map(expected, dynamic.int))

  let assert Ok(result) = value.decode(in, date())

  assert out == result
}

pub fn decode_date_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid date"

  let assert Error(msg) = value.decode(in, date())

  assert out == msg
}

pub fn array_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid array"

  let assert Error(msg) = value.decode(in, array(int2()))

  assert out == msg
}

// Encode tests //

pub fn encode_null_test() {
  let assert Ok(encoded) = value.encode(value.null, int2())

  assert <<-1:int-size(32)>> == encoded
}

pub fn encode_bool_test() {
  use valid <- list.map([#(value.true, 1), #(value.false, 0)])

  let in = valid.0
  let expected = <<1:big-int-size(32), valid.1:big-int-size(8)>>

  let assert Ok(out) = value.encode(in, bool())

  assert expected == out
}

pub fn encode_bool_validation_error_test() {
  let assert Error(msg) = value.encode(value.true, int2())

  assert msg == "Attempted to encode boolsend as int2send"
}

pub fn encode_int2_test() {
  use valid <- list.map([32_767, 0, -32_768])

  let in = value.int(valid)
  let expected = <<2:big-int-size(32), valid:big-int-size(16)>>

  let assert Ok(out) = value.encode(in, int2())

  assert expected == out
}

pub fn encode_int2_error_test() {
  use invalid <- list.map([-100_000, 100_000, 32_768, -32_769])

  let in = value.int(invalid)
  let expected = "Out of range for int2"

  let assert Error(msg) = value.encode(in, int2())

  assert expected == msg
}

pub fn encode_int_validation_error_test() {
  let assert Error(msg) = value.encode(value.int(33), float4())

  assert msg == "Attempted to encode 33 as float4send"
}

pub fn encode_int4_test() {
  use valid <- list.map([2_147_483_647, 0, -2_147_483_648])

  let in = value.int(valid)
  let expected = <<4:big-int-size(32), valid:big-int-size(32)>>

  let assert Ok(out) = value.encode(in, int4())

  assert expected == out
}

pub fn encode_int4_error_test() {
  use invalid <- list.map([2_147_483_648, -2_147_483_649])

  let in = value.int(invalid)
  let expected = "Out of range for int4"

  let assert Error(msg) = value.encode(in, int4())

  assert expected == msg
}

pub fn encode_int8_test() {
  use valid <- list.map([
    9_223_372_036_854_775_807,
    0,
    -9_223_372_036_854_775_808,
  ])

  let in = value.int(valid)
  let expected = <<8:big-int-size(32), valid:big-int-size(64)>>

  let assert Ok(out) = value.encode(in, int8())

  assert expected == out
}

pub fn encode_int8_error_test() {
  use invalid <- list.map([
    9_223_372_036_854_775_807 + 1,
    -9_223_372_036_854_775_808 - 1,
  ])

  let in = value.int(invalid)
  let expected = "Out of range for int8"

  let assert Error(msg) = value.encode(in, int8())

  assert expected == msg
}

pub fn encode_float4_test() {
  use valid <- list.map([0.0, 1.0, -1.0, 3.14, -42.5, 1.23e38])

  let in = value.float(valid)
  let expected = <<4:big-int-size(32), valid:float-size(32)>>

  let assert Ok(out) = value.encode(in, float4())

  assert expected == out
}

pub fn encode_float8_test() {
  use valid <- list.map([0.0, 1.0, -1.0, 3.14, -42.5, 1.23e308])

  let in = value.float(valid)
  let expected = <<8:big-int-size(32), valid:float-size(64)>>

  let assert Ok(out) = value.encode(in, float8())

  assert expected == out
}

pub fn encode_float_validation_error_test() {
  let assert Error(msg) = value.encode(value.float(33.5), varchar())

  assert msg == "Unsupported float type"
}

pub fn encode_oid_test() {
  use valid <- list.map([0, 1042, 4_294_967_295])

  let in = value.int(valid)
  let expected = <<4:big-int-size(32), valid:big-int-size(32)>>

  let assert Ok(out) = value.encode(in, oid())

  assert expected == out
}

pub fn encode_oid_error_test() {
  use invalid <- list.map([-1, 4_294_967_296])

  let in = value.int(invalid)
  let expected = "Out of range for oid"

  let assert Error(msg) = value.encode(in, oid())

  assert expected == msg
}

pub fn encode_varchar_test() {
  use valid <- list.map([#("hello", 5), #("", 0), #("PostgreSQL", 10)])

  let in = value.text(valid.0)
  let expected = <<valid.1:big-int-size(32), valid.0:utf8>>

  let assert Ok(out) = value.encode(in, varchar())

  assert expected == out
}

pub fn encode_text_validation_error_test() {
  let assert Error(msg) = value.encode(value.text("some text"), float4())

  assert msg == "Attempted to encode 'some text' as float4send"
}

pub fn encode_date_test() {
  let assert Ok(#(in, _tod)) =
    timestamp.parse_rfc3339("1970-01-01T00:00:00Z")
    |> result.map(timestamp.to_calendar(_, calendar.utc_offset))

  let expected = <<4:big-int-size(32), -10_957:big-int-size(32)>>

  let assert Ok(out) = value.encode(value.date(in), date())

  assert expected == out
}

pub fn encode_date_validation_error_test() {
  let assert Error(msg) =
    value.date(calendar.Date(2025, calendar.January, 10))
    |> value.encode(float4())

  assert msg == "Attempted to encode date_send as float4send"
}

pub fn encode_time_test() {
  let tod =
    calendar.TimeOfDay(hours: 0, minutes: 1, seconds: 19, nanoseconds: 0)

  let in = value.time(tod)
  let expected = <<8:big-int-size(32), 79_000_000:big-int-size(64)>>

  let assert Ok(out) = value.encode(in, time())

  assert expected == out
}

pub fn encode_time_validation_error_test() {
  let assert Error(msg) =
    value.encode(value.time(calendar.TimeOfDay(20, 10, 30, 0)), float4())

  assert msg == "Attempted to encode time_send as float4send"
}

pub fn encode_timestamp_test() {
  let ts = timestamp.from_unix_seconds(1)

  let in = value.timestamp(ts)
  let expected = <<8:big-int-size(32), -946_684_799_000_000:big-int-size(64)>>

  let assert Ok(out) = value.encode(in, timestamp())

  assert expected == out
}

pub fn encode_timestamp_validation_error_test() {
  let assert Error(msg) =
    value.encode(value.timestamp(timestamp.system_time()), float4())

  assert msg == "Attempted to encode timestamp_send as float4send"
}

fn to_microseconds(
  kind: a,
  to_seconds_and_nanoseconds: fn(a) -> #(Int, Int),
) -> Int {
  let #(seconds, nanoseconds) = to_seconds_and_nanoseconds(kind)

  { seconds * usecs_per_sec } + { nanoseconds / nsecs_per_usec }
}

pub fn encode_interval_test() {
  let val =
    interval.days(14)
    |> interval.add(interval.microseconds(79_000))
    |> value.interval

  let expected = <<
    16:big-int-size(32),
    79_000:big-int-size(64),
    14:big-int-size(32),
    0:big-int-size(32),
  >>

  let assert Ok(out) = value.encode(val, interval())

  assert expected == out
}

pub fn encode_interval_validation_error_test() {
  let val = interval.days(7) |> value.interval

  let assert Error(msg) = value.encode(val, float4())

  assert msg == "Attempted to encode interval_send as float4send"
}

pub fn encode_timestamptz_test() {
  let expected_utc_int = -946_684_799_000_000
  let ts = timestamp.from_unix_seconds(1)
  let offset = value.offset(0)

  let expected = <<
    8:big-int-size(32),
    expected_utc_int:big-int-size(64),
  >>

  let in = value.timestamptz(ts, offset)

  let assert Ok(out) = value.encode(in, timestamptz())

  assert expected == out
}

pub fn encode_positive_offset_timestamptz_test() {
  let expected_utc_int = -946_684_800_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset = value.offset(10)

  let dur =
    int.multiply(10, 60)
    |> int.negate
    |> duration.minutes

  let ten_hours =
    timestamp.add(ts, dur)
    |> to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)
    |> int.add(expected_utc_int)

  let expected = <<
    8:big-int-size(32),
    ten_hours:big-int-size(64),
  >>

  let in = value.timestamptz(ts, offset)

  let assert Ok(out) = value.encode(in, timestamptz())

  assert expected == out
}

pub fn encode_negative_offset_timestamptz_test() {
  let expected_utc_int = -946_684_800_000_000
  let ts = timestamp.from_unix_seconds(1)
  let offset =
    value.offset(-2)
    |> value.minutes(30)

  let dur =
    int.multiply(2, 60)
    |> int.add(30)
    |> duration.minutes

  let minus_two_thirty =
    timestamp.add(ts, dur)
    |> to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)
    |> int.add(expected_utc_int)

  let expected = <<
    8:big-int-size(32),
    minus_two_thirty:big-int-size(64),
  >>

  let in = value.timestamptz(ts, offset)

  let assert Ok(out) = value.encode(in, timestamptz())

  assert expected == out
}

pub fn encode_timestamptz_validation_error_test() {
  let assert Error(msg) =
    value.timestamptz(timestamp.system_time(), value.offset(8))
    |> value.encode(float4())

  assert msg == "Attempted to encode timestamptz_send as float4send"
}

pub fn empty_array_test() {
  let expected = <<
    12:big-int-size(32), 0:big-int-size(32), 0:big-int-size(32),
    21:big-int-size(32),
  >>

  let assert Ok(out) =
    value.encode(value.array([], of: value.int), array(int2()))

  assert expected == out
}

pub fn string_array_test() {
  let in = value.array(["hello", "world"], of: value.text)

  let expected = <<
    38:big-int-size(32), 1:big-int-size(32), 0:big-int-size(32),
    25:big-int-size(32), 2:big-int-size(32), 1:big-int-size(32),
    5:big-int-size(32), "hello":utf8, 5:big-int-size(32), "world":utf8,
  >>

  let assert Ok(out) = value.encode(in, array(text()))

  assert expected == out
}

pub fn int_array_test() {
  let in = value.array([42], of: value.int)
  let expected = <<
    28:big-int-size(32), 1:big-int-size(32), 0:big-int-size(32),
    23:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    4:big-int-size(32), 42:big-int-size(32),
  >>

  let assert Ok(out) = value.encode(in, array(int4()))

  assert expected == out
}

pub fn null_array_test() {
  let in = value.array([value.null], of: function.identity)

  let expected = <<
    24:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    23:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    -1:big-int-size(32),
  >>

  let assert Ok(out) = value.encode(in, array(int4()))

  assert expected == out
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

  let assert Ok(out) = value.encode(in, array(array(int4())))

  assert expected == out
}

pub fn encode_array_validation_error_test() {
  let assert Error(msg) =
    value.encode(value.array([10, 12], of: value.int), float4())

  assert msg == "Attempted to encode array_send as float4send"
}

// TypeInfo helpers

fn oid() {
  type_info.new(26)
  |> type_info.typesend("oidsend")
  |> type_info.typereceive("oidrecv")
}

fn bool() {
  type_info.new(16)
  |> type_info.typesend("boolsend")
  |> type_info.typereceive("boolrecv")
}

fn int2() {
  type_info.new(21)
  |> type_info.typesend("int2send")
  |> type_info.typereceive("int2recv")
}

fn int4() {
  type_info.new(23)
  |> type_info.typesend("int4send")
  |> type_info.typereceive("int4recv")
}

fn int8() {
  type_info.new(20)
  |> type_info.typesend("int8send")
  |> type_info.typereceive("int8recv")
}

fn float4() {
  type_info.new(700)
  |> type_info.typesend("float4send")
  |> type_info.typereceive("float4recv")
}

fn float8() {
  type_info.new(701)
  |> type_info.typesend("float8send")
  |> type_info.typereceive("float8recv")
}

fn varchar() {
  type_info.new(1043)
  |> type_info.typesend("varcharsend")
  |> type_info.typereceive("varcharrecv")
}

fn text() {
  type_info.new(25)
  |> type_info.typesend("textsend")
  |> type_info.typereceive("textrecv")
}

fn bytea() {
  type_info.new(17)
  |> type_info.typesend("byteasend")
  |> type_info.typereceive("bytearecv")
}

fn char() {
  type_info.new(18)
  |> type_info.typesend("charsend")
  |> type_info.typereceive("charrecv")
}

fn name() {
  type_info.new(19)
  |> type_info.typesend("namesend")
  |> type_info.typereceive("namerecv")
}

fn time() {
  type_info.new(1083)
  |> type_info.typesend("time_send")
  |> type_info.typereceive("time_recv")
}

fn date() {
  type_info.new(1082)
  |> type_info.typesend("date_send")
  |> type_info.typereceive("date_recv")
}

fn timestamp() {
  type_info.new(1114)
  |> type_info.typesend("timestamp_send")
  |> type_info.typereceive("timestamp_recv")
}

fn timestamptz() {
  type_info.new(1184)
  |> type_info.typesend("timestamptz_send")
  |> type_info.typereceive("timestamptz_recv")
}

fn interval() {
  type_info.new(1186)
  |> type_info.typesend("interval_send")
  |> type_info.typereceive("interval_recv")
}

fn array(ti: type_info.TypeInfo) -> type_info.TypeInfo {
  type_info.new(143)
  |> type_info.typesend("array_send")
  |> type_info.typereceive("array_recv")
  |> type_info.elem_type(Some(ti))
}
