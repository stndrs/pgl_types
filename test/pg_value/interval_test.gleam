import gleam/dynamic
import gleam/dynamic/decode
import gleam/list
import pg_value/interval

pub fn add_test() {
  let left =
    interval.Interval(months: 3, days: 2, seconds: 10, microseconds: 1000)

  let right =
    interval.Interval(months: 2, days: 8, seconds: 40, microseconds: 3000)

  let sum = interval.add(left, right)

  let assert 5 = sum.months
  let assert 10 = sum.days
  let assert 50 = sum.seconds
  let assert 4000 = sum.microseconds
}

pub fn empty_interval_to_iso8601_string_test() {
  let assert "PT0S" = interval.seconds(0) |> interval.to_iso8601_string
}

pub fn months_interval_to_iso8601_string_test() {
  let assert "P3M" = interval.months(3) |> interval.to_iso8601_string
}

pub fn days_interval_to_iso8601_string_test() {
  let assert "P7D" = interval.days(7) |> interval.to_iso8601_string
}

pub fn seconds_interval_to_iso8601_string_test() {
  let assert "PT30S" = interval.seconds(30) |> interval.to_iso8601_string
}

pub fn microseconds_interval_to_iso8601_string_test() {
  let cases = [
    #(200_000, "PT0.2S"),
    #(20_000, "PT0.02S"),
    #(2000, "PT0.002S"),
    #(200, "PT0.0002S"),
    #(20, "PT0.00002S"),
    #(2, "PT0.000002S"),
  ]

  use #(usecs, expected) <- list.each(cases)

  let iso8601 = interval.microseconds(usecs) |> interval.to_iso8601_string

  let assert True = expected == iso8601
}

pub fn interval_to_iso8601_string_test() {
  let assert "P3M7DT30.2S" =
    interval.Interval(months: 3, days: 7, seconds: 30, microseconds: 200_000)
    |> interval.to_iso8601_string
}

pub fn decoder_test() {
  let cases = [
    interval.Interval(months: 10, days: 7, seconds: 2, microseconds: 500_000),
    interval.Interval(months: 0, days: 7, seconds: 2, microseconds: 500_000),
    interval.Interval(months: 0, days: 0, seconds: 2, microseconds: 500_000),
    interval.Interval(months: 0, days: 0, seconds: 0, microseconds: 500_000),
    interval.Interval(months: 0, days: 0, seconds: 0, microseconds: 50_000),
    interval.Interval(months: 0, days: 0, seconds: 0, microseconds: 5000),
    interval.Interval(months: 0, days: 0, seconds: 0, microseconds: 500),
    interval.Interval(months: 0, days: 0, seconds: 0, microseconds: 50),
    interval.Interval(months: 0, days: 0, seconds: 0, microseconds: 5),
  ]

  let assert Ok(decoded) = {
    use val <- list.try_map(cases)

    val
    |> interval_to_dynamic
    |> decode.run(interval.decoder())
  }
  let assert True = cases == decoded
}

fn interval_to_dynamic(val: interval.Interval) {
  let microseconds = { val.seconds * 1_000_000 } + val.microseconds

  dynamic.array([
    dynamic.int(val.months),
    dynamic.int(val.days),
    dynamic.int(microseconds),
  ])
}
