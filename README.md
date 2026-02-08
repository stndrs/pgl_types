# pg_value

[![test](https://github.com/stndrs/pg_value/actions/workflows/test.yml/badge.svg)](https://github.com/stndrs/pg_value/actions/workflows/test.yml)
[![Package Version](https://img.shields.io/hexpm/v/pg_value)](https://hex.pm/packages/pg_value)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/pg_value/)

```gleam
import pg_value as value

pub fn main() -> Nil {
  let int4_type_info = get_type_info("int4")

  // Encode an integer as int4
  let assert Ok(encoded) = value.encode(value.int(10), int4_type_info)

  // Decode a bit array as an int4 into a dynamic value
  let assert Ok(int_dynamic) = value.decode(encoded, int4_type_info)

  // Create a list of `pg_value.Value`s
  let params = [value.int(10), value.null, value.text("text")]
}
```

Further documentation can be found at <https://hexdocs.pm/pg_value>.

## Installation

```sh
gleam add pg_value
```

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

## Acknowledgements

Much thanks to [`pg_types`](https://github.com/tsloughter/pg_types) for encoding and decoding logic.
