readme: str = """*NativeTransfer* - class for working
with Clickhouse Native Format.

Description of the format on the official website
(https://clickhouse.com/docs/en/interfaces/formats#native):

```quote
The most efficient format. Data is written and read by blocks in binary format.
For each block, the number of rows, number of columns, column names and types,
and parts of columns in this block are recorded one after another.
In other words, this format is “columnar” – it does not
convert columns to rows.
This is the format used in the native interface for
interaction between servers,
for using the command-line client, and for C++ clients.

You can use this format to quickly generate dumps that can only
be read by the ClickHouse DBMS.
It does not make sense to work with this format yourself.
```

This library allows for data exchange between
Clickhouse Native Format and pandas/polars DataFrame.

Unsupported data types (at the moment):
Tuple                   # Tuple(T1, T2, ...).
Map                     # Map(K, V).
Variant                 # Variant(T1, T2, ...).
AggregateFunction       # (name, types_of_arguments...) — parametric data type.
SimpleAggregateFunction # (name, types_of_arguments...) data type stores
current value (intermediate state) of the aggregate function.
Point                   # stored as a Tuple(Float64, Float64).
Ring                    # stored as an array of points: Array(Point).
LineString              # stored as an array of points: Array(Point).
MultiLineString         # is multiple lines stored as an array
of LineString: Array(LineString).
Polygon                 # stored as an array of rings: Array(Ring).
MultiPolygon            # stored as an array of polygons: Array(Polygon).
Expression              # used for representing lambdas
in high-order functions.
Set                     # Used for the right half of an IN expression.
Domains                 # You can use domains anywhere
corresponding base type can be used.
Nested                  # Nested(name1 Type1, Name2 Type2, ...).
Dynamic                 # This type allows to store values of any type
inside it without knowing all of them in advance.
JSON                    # Stores JavaScript Object Notation (JSON)
documents in a single column.

Supported data types:
┌───────────────────────┬────────┬────────┬───────────────────────────────┐
│ Clickhouse data type  │ Read   │ Write  │ Python data type (Read/Write) │
╞═══════════════════════╪════════╪════════╪═══════════════════════════════╡
│ UInt8                 │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ UInt16                │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ UInt32                │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ UInt64                │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ UInt128               │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ UInt256               │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Int8                  │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Int16                 │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Int32                 │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Int64                 │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Int128                │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Int256                │ +      │ +      │ int/int                       │
│-----------------------+--------+--------+-------------------------------│
│ Float32               │ +      │ +      │ float/float                   │
│-----------------------+--------+--------+-------------------------------│
│ Float64               │ +      │ +      │ float/float                   │
│-----------------------+--------+--------+-------------------------------│
│ BFloat16              │ +      │ +      │ float/float                   │
│-----------------------+--------+--------+-------------------------------│
│ Decimal(P, S)         │ +      │ +      │ float/float                   │
│-----------------------+--------+--------+-------------------------------│
│ Decimal32(S)          │ +      │ -      │ float/-                       │
│-----------------------+--------+--------+-------------------------------│
│ Decimal64(S)          │ +      │ -      │ float/-                       │
│-----------------------+--------+--------+-------------------------------│
│ Decimal128(S)         │ +      │ -      │ float/-                       │
│-----------------------+--------+--------+-------------------------------│
│ Decimal256(S)         │ +      │ -      │ float/-                       │
│-----------------------+--------+--------+-------------------------------│
│ String                │ +      │ +      │ str/str                       │
│-----------------------+--------+--------+-------------------------------│
│ FixedString(N)        │ +      │ +      │ str/str                       │
│-----------------------+--------+--------+-------------------------------│
│ Date                  │ +      │ +      │ date/date                     │
│-----------------------+--------+--------+-------------------------------│
│ Date32                │ +      │ +      │ date/date                     │
│-----------------------+--------+--------+-------------------------------│
│ DateTime              │ +      │ +      │ datetime/datetime             │
│-----------------------+--------+--------+-------------------------------│
│ DateTime64            │ +      │ +      │ datetime/datetime             │
│-----------------------+--------+--------+-------------------------------│
│ Enum                  │ +      │ +      │ str/int|Enum                  │
│-----------------------+--------+--------+-------------------------------│
│ Bool                  │ +      │ +      │ bool/bool                     │
│-----------------------+--------+--------+-------------------------------│
│ UUID                  │ +      │ +      │ UUID/UUID                     │
│-----------------------+--------+--------+-------------------------------│
│ IPv4                  │ +      │ +      │ IPv4Address/IPv4Address       │
│-----------------------+--------+--------+-------------------------------│
│ IPv6                  │ +      │ +      │ IPv6Address/IPv6Address       │
│-----------------------+--------+--------+-------------------------------│
│ Array(T)              │ +      │ +      │ List[T*]/List[T*]             │
│-----------------------+--------+--------+-------------------------------│
│ LowCardinality(T)     │ +      │ -      │ str|date|datetime|int|float/- │
│-----------------------+--------+--------+-------------------------------│
│ Nullable(T)           │ +      │ +      │ Optional[T*]/Optional[T*]     │
│-----------------------+--------+--------+-------------------------------│
│ Nothing               │ +      │ +      │ None/None                     │
│-----------------------+--------+--------+-------------------------------│
│ Interval<Type**>      │ +      │ +      │ int/int                       │
└───────────────────────┴────────┴────────┴───────────────────────────────┘
*T - any simple data type from those listed in the table
**Type - interval type: Nanosecond, Microsecond, Millisecond,
Second, Minute, Hour, Day, Week, Month, Quarter, Year

Implementation features of some data types:

DateTime64.
This type requires specifying precision for the accuracy
of values and a time zone;
however, datetime does not have nanoseconds and microseconds attributes,
so when extracting from the class, precision is lost.
When packing back without explicitly specifying precision,
the format DateTime64(3, ) will be chosen.

Decimal(P, S).
The type cannot be automatically determined when performing the make operation.
To save as Decimal, it is necessary to explicitly pass
the data type to the dtypes parameter.

Enum.
When using this type in Clickhouse, indexing starts at 1,
while the standard is considered to be 0.
Additionally, the name may include prohibited names,
such as an empty string and "mro."
Therefore, to prevent conversion issues, the column with Enum
is explicitly converted to strings corresponding to the Enum names.
For reverse writing, the column with Enum values will be
explicitly converted to the data types Int8/Int16.

IPv4/IPv6.
These data types may be implicitly converted
to strings when reading into a DataFrame,
which in turn will lead to a change in the data type
of the column during the write operation.

LowCardinality(T).
Reading from this format is performed in a derived format;
repacking back into the LowCardinality(T) format is not provided.

Base class NativeFormat:

Optional parameters during initialization:
* block_rows - the maximum number of rows in one block when packing
a DataFrame into Native. Range [1:1048576]. Default is 65400.
* logs - an instance of the logging.Logger class.

Static Methods of the Class and Their Parameters:

open
* file - Native file. You can specify the path to the file, pass bytes,
an open file, a file-like object, or GzipFile.
* mode - file operation mode. Reading "rb", writing "wb". Default is "rb".
* write_compressed - boolean, compress the file when creating
Native from DataFrame - True, no - False. Default is False.

Returns an object of type io.BufferedIOBase | gzip.GzipFile.

info
* file - data object
io.BufferedIOBase | gzip.GzipFile | pandas.DataFrame | polars.DataFrame.

Returns an object of type DataInfo.

Main Methods of the Class and Their Parameters:

make
* frame - input data DataFrame pandas.DataFrame | polars.DataFrame.
* file - file object for writing io.BufferedIOBase | gzip.GzipFile.
* columns - [optional] list of column names if you need to change some names
without altering the DataFrame.
Must fully match the number of columns in the DataFrame.
* dtypes - [optional] list of data types for the columns.
If empty, data types will be determined automatically.

As a result, a Native file will be created from the DataFrame;
the method does not return anything additionally.

extract_block
* file - file object for reading io.BufferedIOBase | gzip.GzipFile.
* frame_type - an object of the FrameType class to determine the output format.
Default is FrameType.Pandas.

As a result, an object of type pandas.DataFrame | polars.DataFrame
will be returned, containing one block from Native.

extract
* file - file object for reading io.BufferedIOBase | gzip.GzipFile.
* frame_type - an object of the FrameType class to determine the output format.
Default is FrameType.Pandas.

As a result, an object of type pandas.DataFrame | polars.DataFrame
will be returned, containing the entire Native file.

Errors returned by NativeFormat class:

* NativeError - Base error
* NativeDateError - Date/Date32 error
* NativeDateTimeError - DateTime/DateTime64 error
* NativeDTypeError - Data Type error
* NativeEnumError - Enum error
* NativePrecissionError - Value precission error
* NativeReadError - Read error
* NativeWriteError - Write error

Additional classes:

* DataFormat - Enum for defining the format of the data being processed.
It is an attribute of the DataInfo class.

Possible values:
+ Native = 0,
+ GzipNative = 1,
+ Pandas = 2,
+ Polars = 3

* DataInfo - NamedTuple with an assigned string representation.

Class attributes:
+ data_format - DataFormat object,
+ columns - list columns,
+ dtypes - list Clickhouse data types,
+ total_rows - count of rows in DataFormat object

* FrameType - Enum to select reading format.

Possible values:
+ Pandas = 0,
+ Polars = 1
"""
