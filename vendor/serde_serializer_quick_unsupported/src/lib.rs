//! Often times you need to implement a serializer where a bunch of input types are
//! unexpected/unsupported This convenience macro allows you to declare those more easily

#![no_std]

/// Often times you need to implement a serializer where a bunch of input types are
/// unexpected/unsupported This convenience macro allows you to declare those more easily:
///
/// ```rust
/// use serde_serializer_quick_unsupported::serializer_unsupported;
///
/// struct MySerializer;
/// impl serde::Serializer for MySerializer {
/// 	type Ok = ();
/// 	type Error = serde::de::value::Error;
/// 	serializer_unsupported! {
/// 		err = (<Self::Error as serde::ser::Error>::custom("Unexpected input"));
/// 		bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes none some unit unit_struct
/// 		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
/// 		struct_variant i128 u128
/// 	}
/// }
/// ```
/// Just remove the stuff you want to not error

#[macro_export]
macro_rules! serializer_unsupported {
	(err = ($($err: tt)*); bool $($rest: tt)*) => {
		fn serialize_bool(self, v: bool) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); i8 $($rest: tt)*) => {
		fn serialize_i8(self, v: i8) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); i16 $($rest: tt)*) => {
		fn serialize_i16(self, v: i16) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); i32 $($rest: tt)*) => {
		fn serialize_i32(self, v: i32) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); i64 $($rest: tt)*) => {
		fn serialize_i64(self, v: i64) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); u8 $($rest: tt)*) => {
		fn serialize_u8(self, v: u8) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); u16 $($rest: tt)*) => {
		fn serialize_u16(self, v: u16) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); u32 $($rest: tt)*) => {
		fn serialize_u32(self, v: u32) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); u64 $($rest: tt)*) => {
		fn serialize_u64(self, v: u64) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); f32 $($rest: tt)*) => {
		fn serialize_f32(self, v: f32) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); f64 $($rest: tt)*) => {
		fn serialize_f64(self, v: f64) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); char $($rest: tt)*) => {
		fn serialize_char(self, v: char) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); str $($rest: tt)*) => {
		fn serialize_str(self, v: &str) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); bytes $($rest: tt)*) => {
		fn serialize_bytes(self, v: &[u8]) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = v;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); none $($rest: tt)*) => {
		fn serialize_none(self) -> ::core::result::Result<Self::Ok, Self::Error> {
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); some $($rest: tt)*) => {
		fn serialize_some<T: ?Sized>(self, value: &T) -> ::core::result::Result<Self::Ok, Self::Error>
		where
			T: serde::Serialize,
		{
			let _ = value;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); unit $($rest: tt)*) => {
		fn serialize_unit(self) -> ::core::result::Result<Self::Ok, Self::Error> {
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); unit_struct $($rest: tt)*) => {
		fn serialize_unit_struct(self, name: &'static str) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = name;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); unit_variant $($rest: tt)*) => {
		fn serialize_unit_variant(
			self,
			name: &'static str,
			variant_index: u32,
			variant: &'static str,
		) -> ::core::result::Result<Self::Ok, Self::Error> {
			let _ = name;
			let _ = variant_index;
			let _ = variant;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); newtype_struct $($rest: tt)*) => {
		fn serialize_newtype_struct<T: ?Sized>(self, name: &'static str, value: &T) -> ::core::result::Result<Self::Ok, Self::Error>
		where
			T: serde::Serialize,
		{
			let _ = name;
			let _ = value;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); newtype_variant $($rest: tt)*) => {
		fn serialize_newtype_variant<T: ?Sized>(
			self,
			name: &'static str,
			variant_index: u32,
			variant: &'static str,
			value: &T,
		) -> ::core::result::Result<Self::Ok, Self::Error>
		where
			T: serde::Serialize,
		{
			let _ = name;
			let _ = variant_index;
			let _ = variant;
			let _ = value;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); seq $($rest: tt)*) => {
		type SerializeSeq = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_seq(self, len: Option<usize>) -> ::core::result::Result<Self::SerializeSeq, Self::Error> {
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); tuple $($rest: tt)*) => {
		type SerializeTuple = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_tuple(self, len: usize) -> ::core::result::Result<Self::SerializeTuple, Self::Error> {
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); tuple_struct $($rest: tt)*) => {
		type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_tuple_struct(self, name: &'static str, len: usize) -> ::core::result::Result<Self::SerializeTupleStruct, Self::Error> {
			let _ = name;
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); tuple_variant $($rest: tt)*) => {
		type SerializeTupleVariant = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_tuple_variant(
			self,
			name: &'static str,
			variant_index: u32,
			variant: &'static str,
			len: usize,
		) -> ::core::result::Result<Self::SerializeTupleVariant, Self::Error> {
			let _ = name;
			let _ = variant_index;
			let _ = variant;
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); map $($rest: tt)*) => {
		type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_map(self, len: Option<usize>) -> ::core::result::Result<Self::SerializeMap, Self::Error> {
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); struct $($rest: tt)*) => {
		type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_struct(self, name: &'static str, len: usize) -> ::core::result::Result<Self::SerializeStruct, Self::Error> {
			let _ = name;
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); struct_variant $($rest: tt)*) => {
		type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;
		fn serialize_struct_variant(
			self,
			name: &'static str,
			variant_index: u32,
			variant: &'static str,
			len: usize,
		) -> ::core::result::Result<Self::SerializeStructVariant, Self::Error> {
			let _ = name;
			let _ = variant_index;
			let _ = variant;
			let _ = len;
			Err(($($err)*))
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); i128 $($rest: tt)*) => {
		serde::serde_if_integer128! {
			fn serialize_i128(self, v: i128) -> ::core::result::Result<Self::Ok, Self::Error> {
				let _ = v;
				Err(($($err)*))
			}
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*); u128 $($rest: tt)*) => {
		serde::serde_if_integer128! {
			fn serialize_u128(self, v: u128) -> ::core::result::Result<Self::Ok, Self::Error> {
				let _ = v;
				Err(($($err)*))
			}
		}
		$crate::serializer_unsupported!{ err = ($($err)*); $($rest)* }
	};
	(err = ($($err: tt)*);) => {};
}
