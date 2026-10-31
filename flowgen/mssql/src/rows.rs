//! Conversion of SQL Server rows to Arrow record batches.

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use std::sync::Arc;
use tiberius::{Column, ColumnData, ColumnType, FromSql, Row};

/// Errors converting rows.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Column `{column}` has type {type_name}, which has no Arrow mapping; convert it in the query, for example `CAST({column} AS nvarchar(max))`")]
    UnsupportedType { column: String, type_name: String },
    #[error("Failed to read column `{column}`: {source}")]
    Value {
        column: String,
        #[source]
        source: tiberius::error::Error,
    },
    #[error("Failed to build record batch: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
}

/// How a column's values become an Arrow array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decoder {
    Bool,
    UInt8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Text,
    Date,
    Time,
    Timestamp,
    /// Read as `DateTime<FixedOffset>`: tiberius applies the offset twice
    /// when it reads a `datetimeoffset` as `DateTime<Utc>`.
    TimestampTz,
    Binary,
}

impl Decoder {
    /// A variable-length integer or float can hold any width, so it maps to
    /// the widest one.
    fn for_type(ty: ColumnType) -> Option<Self> {
        match ty {
            ColumnType::Bit | ColumnType::Bitn => Some(Self::Bool),
            ColumnType::Int1 => Some(Self::UInt8),
            ColumnType::Int2 => Some(Self::Int16),
            ColumnType::Int4 => Some(Self::Int32),
            ColumnType::Int8 | ColumnType::Intn => Some(Self::Int64),
            ColumnType::Float4 => Some(Self::Float32),
            ColumnType::Float8 | ColumnType::Floatn | ColumnType::Money | ColumnType::Money4 => {
                Some(Self::Float64)
            }
            ColumnType::Decimaln | ColumnType::Numericn => Some(Self::Decimal),
            ColumnType::Null
            | ColumnType::Guid
            | ColumnType::BigVarChar
            | ColumnType::BigChar
            | ColumnType::NVarchar
            | ColumnType::NChar
            | ColumnType::Xml
            | ColumnType::Text
            | ColumnType::NText => Some(Self::Text),
            ColumnType::Daten => Some(Self::Date),
            ColumnType::Timen => Some(Self::Time),
            ColumnType::Datetime
            | ColumnType::Datetime4
            | ColumnType::Datetimen
            | ColumnType::Datetime2 => Some(Self::Timestamp),
            ColumnType::DatetimeOffsetn => Some(Self::TimestampTz),
            ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => Some(Self::Binary),
            ColumnType::Udt | ColumnType::SSVariant => None,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::UInt8 => DataType::UInt8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Decimal | Self::Text => DataType::Utf8,
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time64(TimeUnit::Microsecond),
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Binary => DataType::Binary,
        }
    }

    fn array(self, rows: &[Row], index: usize) -> tiberius::Result<ArrayRef> {
        Ok(match self {
            Self::Bool => Arc::new(BooleanArray::from(values(rows, index, |v: bool| v)?)),
            Self::UInt8 => Arc::new(UInt8Array::from(values(rows, index, |v: u8| v)?)),
            Self::Int16 => Arc::new(Int16Array::from(values(rows, index, |Int16(v)| v)?)),
            Self::Int32 => Arc::new(Int32Array::from(values(rows, index, |Int32(v)| v)?)),
            Self::Int64 => Arc::new(Int64Array::from(values(rows, index, |Int64(v)| v)?)),
            Self::Float32 => Arc::new(Float32Array::from(values(rows, index, |v: f32| v)?)),
            Self::Float64 => Arc::new(Float64Array::from(values(rows, index, |Float64(v)| v)?)),
            Self::Decimal => Arc::new(StringArray::from(values(rows, index, |Decimal(v)| v)?)),
            Self::Text => Arc::new(StringArray::from(values(rows, index, |Text(v)| v)?)),
            Self::Date => Arc::new(Date32Array::from(values(
                rows,
                index,
                |date: NaiveDate| {
                    (date - chrono::DateTime::UNIX_EPOCH.date_naive()).num_days() as i32
                },
            )?)),
            Self::Time => Arc::new(Time64MicrosecondArray::from(values(
                rows,
                index,
                |time: NaiveTime| {
                    i64::from(time.num_seconds_from_midnight()) * 1_000_000
                        + i64::from(time.nanosecond() / 1_000)
                },
            )?)),
            Self::Timestamp => Arc::new(TimestampMicrosecondArray::from(values(
                rows,
                index,
                |at: NaiveDateTime| at.and_utc().timestamp_micros(),
            )?)),
            Self::TimestampTz => Arc::new(
                TimestampMicrosecondArray::from(values(
                    rows,
                    index,
                    |at: chrono::DateTime<chrono::FixedOffset>| at.timestamp_micros(),
                )?)
                .with_timezone("UTC"),
            ),
            Self::Binary => Arc::new(BinaryArray::from(values(rows, index, |v: &[u8]| v)?)),
        })
    }
}

/// The values of column `index` read as `T` and mapped by `into`, `None` for null.
fn values<'a, T: FromSql<'a>, U>(
    rows: &'a [Row],
    index: usize,
    into: impl Fn(T) -> U,
) -> tiberius::Result<Vec<Option<U>>> {
    rows.iter()
        .map(|row| Ok(row.try_get::<T, _>(index)?.map(&into)))
        .collect()
}

/// Builds record batches with the columns of one result.
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
            .map(|column| match Decoder::for_type(column.column_type()) {
                Some(decoder) => Ok((column.name().to_string(), decoder)),
                None => Err(Error::UnsupportedType {
                    column: column.name().to_string(),
                    type_name: format!("{:?}", column.column_type()),
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

/// An unexpected value for the column's type.
fn mismatch(value: &ColumnData<'static>, expected: &str) -> tiberius::error::Error {
    tiberius::error::Error::Conversion(format!("expected {expected}, got {value:?}").into())
}

/// A small integer, also from a narrower column.
struct Int16(i16);

impl FromSql<'_> for Int16 {
    fn from_sql(value: &ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            ColumnData::I16(value) => Ok(value.map(Self)),
            ColumnData::U8(value) => Ok(value.map(|value| Self(value.into()))),
            value => Err(mismatch(value, "a smallint")),
        }
    }
}

/// An integer, also from a narrower column.
struct Int32(i32);

impl FromSql<'_> for Int32 {
    fn from_sql(value: &ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            ColumnData::I32(value) => Ok(value.map(Self)),
            ColumnData::I16(value) => Ok(value.map(|value| Self(value.into()))),
            ColumnData::U8(value) => Ok(value.map(|value| Self(value.into()))),
            value => Err(mismatch(value, "an int")),
        }
    }
}

/// A bigint, or any narrower integer of a variable-length column.
struct Int64(i64);

impl FromSql<'_> for Int64 {
    fn from_sql(value: &ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            ColumnData::I64(value) => Ok(value.map(Self)),
            ColumnData::I32(value) => Ok(value.map(|value| Self(value.into()))),
            ColumnData::I16(value) => Ok(value.map(|value| Self(value.into()))),
            ColumnData::U8(value) => Ok(value.map(|value| Self(value.into()))),
            value => Err(mismatch(value, "a bigint")),
        }
    }
}

/// A float, also from a real of a variable-length column.
struct Float64(f64);

impl FromSql<'_> for Float64 {
    fn from_sql(value: &ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            ColumnData::F64(value) => Ok(value.map(Self)),
            ColumnData::F32(value) => Ok(value.map(|value| Self(value.into()))),
            value => Err(mismatch(value, "a float")),
        }
    }
}

/// A decimal as exact text.
struct Decimal(String);

impl FromSql<'_> for Decimal {
    fn from_sql(value: &ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            ColumnData::Numeric(value) => {
                Ok(value.map(|value| Self(decimal_text(value.value(), value.scale()))))
            }
            value => Err(mismatch(value, "a decimal")),
        }
    }
}

/// `value` scaled down by `scale` decimal places, written out exactly.
fn decimal_text(value: i128, scale: u8) -> String {
    let digits = value.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let sign = match value < 0 {
        true => "-",
        false => "",
    };
    match scale {
        0 => format!("{sign}{digits}"),
        _ => {
            let digits = format!("{digits:0>width$}", width = scale + 1);
            let (int, fraction) = digits.split_at(digits.len() - scale);
            format!("{sign}{int}.{fraction}")
        }
    }
}

/// Text of a string, GUID or XML column.
struct Text(String);

impl FromSql<'_> for Text {
    fn from_sql(value: &ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            ColumnData::String(value) => Ok(value.as_ref().map(|value| Self(value.to_string()))),
            ColumnData::Guid(value) => Ok(value.map(|value| Self(value.to_string()))),
            ColumnData::Xml(value) => Ok(value.as_ref().map(|value| Self(value.to_string()))),
            ColumnData::I32(None) | ColumnData::U8(None) => Ok(None),
            value => Err(mismatch(value, "text")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_are_written_out_exactly() {
        assert_eq!(decimal_text(12_345_678, 3), "12345.678");
        assert_eq!(decimal_text(-5, 2), "-0.05");
        assert_eq!(decimal_text(42, 0), "42");
        assert_eq!(
            decimal_text(i128::from(i64::MAX), 4),
            "922337203685477.5807"
        );
    }

    #[test]
    fn variable_length_columns_map_to_the_widest_type() {
        assert_eq!(Decoder::for_type(ColumnType::Intn), Some(Decoder::Int64));
        assert_eq!(
            Decoder::for_type(ColumnType::Floatn),
            Some(Decoder::Float64)
        );
        assert_eq!(Decoder::for_type(ColumnType::Int1), Some(Decoder::UInt8));
        assert_eq!(Decoder::for_type(ColumnType::SSVariant), None);
    }

    #[test]
    fn narrower_integers_widen_and_other_values_are_errors() {
        assert_eq!(
            Int64::from_sql(&ColumnData::I16(Some(-7)))
                .unwrap()
                .map(|v| v.0),
            Some(-7)
        );
        assert_eq!(
            Int64::from_sql(&ColumnData::I64(None))
                .unwrap()
                .map(|v| v.0),
            None
        );
        assert!(Int64::from_sql(&ColumnData::F64(Some(1.0))).is_err());
    }
}
