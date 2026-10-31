//! Conversion of PostgreSQL rows to Arrow record batches.

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::fmt::Write;
use std::sync::Arc;
use tokio_postgres::types::{FromSql, Kind, Type};
use tokio_postgres::{Column, Row};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Days from the Unix epoch to the PostgreSQL epoch, 2000-01-01.
const EPOCH_DAYS: i32 = 10_957;
/// Microseconds from the Unix epoch to the PostgreSQL epoch, 2000-01-01.
const EPOCH_MICROS: i64 = 946_684_800_000_000;

const NUMERIC_NEGATIVE: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_INFINITY: u16 = 0xD000;
const NUMERIC_NEGATIVE_INFINITY: u16 = 0xF000;

/// Errors converting rows.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Column `{column}` has type `{type_name}`, which has no Arrow mapping; cast it in the query, for example `{column}::text`")]
    UnsupportedType { column: String, type_name: String },
    #[error("Failed to read column `{column}`: {source}")]
    Value {
        column: String,
        #[source]
        source: BoxError,
    },
    #[error("Failed to build record batch: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
}

/// How a column's binary values become an Arrow array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decoder {
    Bool,
    Char,
    Int16,
    Int32,
    Int64,
    Oid,
    Float32,
    Float64,
    Numeric,
    Text,
    Jsonb,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Bytea,
}

impl Decoder {
    fn for_type(ty: &Type) -> Option<Self> {
        match *ty {
            Type::BOOL => Some(Self::Bool),
            Type::CHAR => Some(Self::Char),
            Type::INT2 => Some(Self::Int16),
            Type::INT4 => Some(Self::Int32),
            Type::INT8 => Some(Self::Int64),
            Type::OID => Some(Self::Oid),
            Type::FLOAT4 => Some(Self::Float32),
            Type::FLOAT8 => Some(Self::Float64),
            Type::NUMERIC => Some(Self::Numeric),
            Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
            | Type::JSON
            | Type::XML
            | Type::UNKNOWN => Some(Self::Text),
            Type::JSONB => Some(Self::Jsonb),
            Type::UUID => Some(Self::Uuid),
            Type::DATE => Some(Self::Date),
            Type::TIME => Some(Self::Time),
            Type::TIMESTAMP => Some(Self::Timestamp),
            Type::TIMESTAMPTZ => Some(Self::TimestampTz),
            Type::BYTEA => Some(Self::Bytea),
            _ => match (ty.kind(), ty.name()) {
                (Kind::Enum(_), _) | (_, "citext") => Some(Self::Text),
                _ => None,
            },
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Char => DataType::Utf8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Oid => DataType::UInt32,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Numeric | Self::Text | Self::Jsonb | Self::Uuid => DataType::Utf8,
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time64(TimeUnit::Microsecond),
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Bytea => DataType::Binary,
        }
    }

    fn array(self, rows: &[Row], index: usize) -> Result<ArrayRef, BoxError> {
        Ok(match self {
            Self::Bool => Arc::new(BooleanArray::from(values(rows, index, |raw| {
                Ok(fixed::<1>(raw)?[0] != 0)
            })?)),
            Self::Char => Arc::new(StringArray::from(values(rows, index, |raw| {
                Ok(char::from(fixed::<1>(raw)?[0]).to_string())
            })?)),
            Self::Int16 => Arc::new(Int16Array::from(values(rows, index, |raw| {
                Ok(i16::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Int32 => Arc::new(Int32Array::from(values(rows, index, |raw| {
                Ok(i32::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Int64 => Arc::new(Int64Array::from(values(rows, index, |raw| {
                Ok(i64::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Oid => Arc::new(UInt32Array::from(values(rows, index, |raw| {
                Ok(u32::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Float32 => Arc::new(Float32Array::from(values(rows, index, |raw| {
                Ok(f32::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Float64 => Arc::new(Float64Array::from(values(rows, index, |raw| {
                Ok(f64::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Numeric => Arc::new(StringArray::from(values(rows, index, numeric)?)),
            Self::Text => Arc::new(StringArray::from(values(rows, index, |raw| {
                Ok(std::str::from_utf8(raw)?.to_owned())
            })?)),
            Self::Jsonb => Arc::new(StringArray::from(values(rows, index, jsonb)?)),
            Self::Uuid => Arc::new(StringArray::from(values(rows, index, |raw| {
                Ok(uuid::Uuid::from_slice(raw)?.to_string())
            })?)),
            Self::Date => Arc::new(Date32Array::from(nullable_values(rows, index, date)?)),
            Self::Time => Arc::new(Time64MicrosecondArray::from(values(rows, index, |raw| {
                Ok(i64::from_be_bytes(fixed(raw)?))
            })?)),
            Self::Timestamp => Arc::new(TimestampMicrosecondArray::from(nullable_values(
                rows, index, timestamp,
            )?)),
            Self::TimestampTz => Arc::new(
                TimestampMicrosecondArray::from(nullable_values(rows, index, timestamp)?)
                    .with_timezone("UTC"),
            ),
            Self::Bytea => Arc::new(BinaryArray::from_iter(values(rows, index, |raw| {
                Ok(raw.to_vec())
            })?)),
        })
    }
}

/// Builds record batches with the columns of one statement.
#[derive(Debug, Clone)]
pub struct Converter {
    schema: SchemaRef,
    columns: Vec<(String, Decoder)>,
}

impl Converter {
    /// Maps each result column to an Arrow field.
    pub fn new(columns: &[Column]) -> Result<Self, Error> {
        let columns = columns
            .iter()
            .map(|column| match Decoder::for_type(column.type_()) {
                Some(decoder) => Ok((column.name().to_string(), decoder)),
                None => Err(Error::UnsupportedType {
                    column: column.name().to_string(),
                    type_name: column.type_().name().to_string(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, decoder)| Field::new(name, decoder.data_type(), true))
            .collect();
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            columns,
        })
    }

    /// One record batch holding `rows`; empty rows give an empty batch with the schema.
    pub fn batch(&self, rows: &[Row]) -> Result<RecordBatch, Error> {
        let arrays = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, (name, decoder))| {
                decoder.array(rows, index).map_err(|source| Error::Value {
                    column: name.clone(),
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(|source| Error::Arrow { source })
    }
}

/// A value's bytes in the binary wire format, whatever its type.
struct Raw<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, BoxError> {
        Ok(Self(raw))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

fn values<T>(
    rows: &[Row],
    index: usize,
    decode: impl Fn(&[u8]) -> Result<T, BoxError>,
) -> Result<Vec<Option<T>>, BoxError> {
    nullable_values(rows, index, |raw| decode(raw).map(Some))
}

/// Like `values`, with `decode` turning some values into null.
fn nullable_values<T>(
    rows: &[Row],
    index: usize,
    decode: impl Fn(&[u8]) -> Result<Option<T>, BoxError>,
) -> Result<Vec<Option<T>>, BoxError> {
    rows.iter()
        .map(|row| match row.try_get::<_, Option<Raw>>(index)? {
            Some(Raw(raw)) => decode(raw),
            None => Ok(None),
        })
        .collect()
}

fn fixed<const N: usize>(raw: &[u8]) -> Result<[u8; N], BoxError> {
    Ok(raw.try_into()?)
}

/// Days since the Unix epoch; null for `infinity`, `-infinity` and dates Arrow can't hold.
fn date(raw: &[u8]) -> Result<Option<i32>, BoxError> {
    match i32::from_be_bytes(fixed(raw)?) {
        i32::MAX | i32::MIN => Ok(None),
        days => Ok(days.checked_add(EPOCH_DAYS)),
    }
}

/// Microseconds since the Unix epoch; null for `infinity`, `-infinity` and
/// timestamps Arrow can't hold.
fn timestamp(raw: &[u8]) -> Result<Option<i64>, BoxError> {
    match i64::from_be_bytes(fixed(raw)?) {
        i64::MAX | i64::MIN => Ok(None),
        micros => Ok(micros.checked_add(EPOCH_MICROS)),
    }
}

fn jsonb(raw: &[u8]) -> Result<String, BoxError> {
    match raw.split_first() {
        Some((1, json)) => Ok(std::str::from_utf8(json)?.to_owned()),
        _ => Err("Unsupported jsonb format version".into()),
    }
}

/// The decimal text of a numeric value: base-10000 digit groups, the first
/// one at `weight`, cut to `scale` fraction digits.
fn numeric(raw: &[u8]) -> Result<String, BoxError> {
    if !raw.len().is_multiple_of(2) {
        return Err("Truncated numeric value".into());
    }
    let words: Vec<u16> = raw
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u16::from_be_bytes(*pair))
        .collect();
    let [count, weight, sign, scale, digits @ ..] = words.as_slice() else {
        return Err("Truncated numeric value".into());
    };
    if digits.len() != usize::from(*count) {
        return Err("Truncated numeric value".into());
    }
    match *sign {
        NUMERIC_NAN => return Ok("NaN".to_string()),
        NUMERIC_INFINITY => return Ok("Infinity".to_string()),
        NUMERIC_NEGATIVE_INFINITY => return Ok("-Infinity".to_string()),
        _ => {}
    }
    let weight = i32::from(*weight as i16);
    let digit = |position: i32| match usize::try_from(position)
        .ok()
        .and_then(|position| digits.get(position))
    {
        Some(digit) => *digit,
        None => 0,
    };

    let mut text = String::new();
    if *sign == NUMERIC_NEGATIVE {
        text.push('-');
    }
    match weight {
        weight if weight < 0 => text.push('0'),
        weight => {
            write!(text, "{}", digit(0))?;
            for position in 1..=weight {
                write!(text, "{:04}", digit(position))?;
            }
        }
    }
    let scale = usize::from(*scale);
    if scale > 0 {
        let mut fraction = String::new();
        let groups = i32::try_from(scale.div_ceil(4))?;
        for position in weight + 1..=weight + groups {
            write!(fraction, "{:04}", digit(position))?;
        }
        fraction.truncate(scale);
        text.push('.');
        text.push_str(&fraction);
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric_bytes(weight: i16, sign: u16, scale: u16, digits: &[u16]) -> Vec<u8> {
        [digits.len() as u16, weight as u16, sign, scale]
            .iter()
            .chain(digits)
            .flat_map(|word| word.to_be_bytes())
            .collect()
    }

    #[test]
    fn numeric_with_integer_and_fraction() {
        let raw = numeric_bytes(1, 0, 3, &[1, 2345, 6780]);
        assert_eq!(numeric(&raw).unwrap(), "12345.678");
    }

    #[test]
    fn numeric_below_one() {
        let raw = numeric_bytes(-1, 0, 4, &[12]);
        assert_eq!(numeric(&raw).unwrap(), "0.0012");
    }

    #[test]
    fn numeric_with_leading_zero_groups() {
        let raw = numeric_bytes(-2, 0, 8, &[1234]);
        assert_eq!(numeric(&raw).unwrap(), "0.00001234");
    }

    #[test]
    fn numeric_negative_with_trailing_zero_groups() {
        let raw = numeric_bytes(2, NUMERIC_NEGATIVE, 0, &[12]);
        assert_eq!(numeric(&raw).unwrap(), "-1200000000");
    }

    #[test]
    fn numeric_zero_keeps_its_scale() {
        let raw = numeric_bytes(0, 0, 2, &[]);
        assert_eq!(numeric(&raw).unwrap(), "0.00");
    }

    #[test]
    fn numeric_special_values() {
        assert_eq!(
            numeric(&numeric_bytes(0, NUMERIC_NAN, 0, &[])).unwrap(),
            "NaN"
        );
        assert_eq!(
            numeric(&numeric_bytes(0, NUMERIC_INFINITY, 0, &[])).unwrap(),
            "Infinity"
        );
        assert_eq!(
            numeric(&numeric_bytes(0, NUMERIC_NEGATIVE_INFINITY, 0, &[])).unwrap(),
            "-Infinity"
        );
    }

    #[test]
    fn numeric_with_missing_digits_is_rejected() {
        let mut raw = numeric_bytes(1, 0, 0, &[1, 2]);
        raw.truncate(raw.len() - 2);
        assert!(numeric(&raw).is_err());
    }

    #[test]
    fn jsonb_strips_the_format_version() {
        assert_eq!(jsonb(b"\x01{\"a\":1}").unwrap(), "{\"a\":1}");
        assert!(jsonb(b"\x02{}").is_err());
    }

    #[test]
    fn timestamps_shift_to_the_unix_epoch() {
        assert_eq!(timestamp(&0i64.to_be_bytes()).unwrap(), Some(EPOCH_MICROS));
        assert_eq!(date(&0i32.to_be_bytes()).unwrap(), Some(EPOCH_DAYS));
    }

    #[test]
    fn infinite_dates_and_timestamps_are_null() {
        assert_eq!(timestamp(&i64::MAX.to_be_bytes()).unwrap(), None);
        assert_eq!(timestamp(&i64::MIN.to_be_bytes()).unwrap(), None);
        assert_eq!(date(&i32::MAX.to_be_bytes()).unwrap(), None);
        assert_eq!(date(&i32::MIN.to_be_bytes()).unwrap(), None);
        assert_eq!(timestamp(&(i64::MAX - 1).to_be_bytes()).unwrap(), None);
    }

    #[test]
    fn types_without_a_mapping_have_no_decoder() {
        assert_eq!(Decoder::for_type(&Type::INTERVAL), None);
        assert_eq!(Decoder::for_type(&Type::TEXT_ARRAY), None);
        assert_eq!(Decoder::for_type(&Type::JSONB), Some(Decoder::Jsonb));
    }

    #[test]
    fn an_empty_result_keeps_the_schema() {
        let converter = Converter {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("created_at", Decoder::TimestampTz.data_type(), true),
            ])),
            columns: vec![
                ("id".to_string(), Decoder::Int64),
                ("created_at".to_string(), Decoder::TimestampTz),
            ],
        };
        let batch = converter.batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), converter.schema);
    }
}
