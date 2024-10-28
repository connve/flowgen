# serde_serializer_quick_unsupported

[![Crates.io](https://img.shields.io/crates/v/serde_serializer_quick_unsupported.svg)](https://crates.io/crates/serde_serializer_quick_unsupported)
[![License](https://img.shields.io/github/license/Ten0/serde_serializer_quick_unsupported)](LICENSE)

Helper for implementing a serializer that supports a reduced subset of the serde data model

Often times you need to implement a serializer where a bunch of input types are
unexpected/unsupported This convenience macro allows you to declare those more easily:

```rust
use serde_serializer_quick_unsupported::serializer_unsupported;

struct MySerializer;
impl serde::Serializer for MySerializer {
	type Ok = ();
	type Error = serde::de::value::Error;
	serializer_unsupported! {
		err = (<Self::Error as serde::ser::Error>::custom("Unexpected input"));
		bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant i128 u128
	}
}
```
Just remove the stuff you want to not error
