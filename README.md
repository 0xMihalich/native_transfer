# NativeTransfer

## класс для работы с Clickhouse Native Format

Описание формата на [официальном сайте](https://clickhouse.com/docs/ru/interfaces/formats#native):

```quote
Самый эффективный формат. Данные пишутся и читаются блоками в бинарном виде.
Для каждого блока пишется количество строк, количество столбцов, имена и типы столбцов,
а затем кусочки столбцов этого блока, один за другим.
То есть, этот формат является «столбцовым» - не преобразует столбцы в строки.
Именно этот формат используется в родном интерфейсе - при межсерверном взаимодействии,
при использовании клиента командной строки, при работе клиентов, написанных на C++.

Вы можете использовать этот формат для быстрой генерации дампов,
которые могут быть прочитаны только СУБД ClickHouse.
Вряд ли имеет смысл работать с этим форматом самостоятельно.
```

Данная библиотека позволяет выполнить обмен данными между Clickhouse Native Format и pandas/polars DataFrame.

## Неподдерживаемые типы данных (на данный момент)

* Tuple # Tuple(T1, T2, ...).
* Map # Map(K, V).
* Variant # Variant(T1, T2, ...).
* AggregateFunction # (name, types_of_arguments...) — parametric data type.
* SimpleAggregateFunction # (name, types_of_arguments...) data type stores current value (intermediate state) of the aggregate function.
* Point # stored as a Tuple(Float64, Float64).
* Ring # stored as an array of points: Array(Point).
* LineString # stored as an array of points: Array(Point).
* MultiLineString # is multiple lines stored as an array of LineString: Array(LineString).
* Polygon # stored as an array of rings: Array(Ring).
* MultiPolygon # stored as an array of polygons: Array(Polygon).
* Expression # used for representing lambdas in high-order functions.
* Set # Used for the right half of an IN expression.
* Domains # You can use domains anywhere corresponding base type can be used.
* Nested # Nested(name1 Type1, Name2 Type2, ...).
* Dynamic # This type allows to store values of any type inside it without knowing all of them in advance.
* JSON # Stores JavaScript Object Notation (JSON) documents in a single column.

## Поддерживаемые типы данных

| Тип данных Clickhouse | Чтение | Запись | Тип данных Python (Чтение/запись)    |
|:----------------------|:------:|:------:|:-------------------------------------|
| UInt8                 | +      | +      | int/int                              |
| UInt16                | +      | +      | int/int                              |
| UInt32                | +      | +      | int/int                              |
| UInt64                | +      | +      | int/int                              |
| UInt128               | +      | +      | int/int                              |
| UInt256               | +      | +      | int/int                              |
| Int8                  | +      | +      | int/int                              |
| Int16                 | +      | +      | int/int                              |
| Int32                 | +      | +      | int/int                              |
| Int64                 | +      | +      | int/int                              |
| Int128                | +      | +      | int/int                              |
| Int256                | +      | +      | int/int                              |
| Float32               | +      | +      | float/float                          |
| Float64               | +      | +      | float/float                          |
| BFloat16              | +      | +      | float/float                          |
| Decimal(P, S)         | +      | +      | float/float                          |
| Decimal32(S)          | +      | -      | float/-                              |
| Decimal64(S)          | +      | -      | float/-                              |
| Decimal128(S)         | +      | -      | float/-                              |
| Decimal256(S)         | +      | -      | float/-                              |
| String                | +      | +      | str/str                              |
| FixedString(N)        | +      | +      | str/str                              |
| Date                  | +      | +      | date/date                            |
| Date32                | +      | +      | date/date                            |
| DateTime              | +      | +      | datetime/datetime                    |
| DateTime64            | +      | +      | datetime/datetime                    |
| Enum                  | +      | +      | str/Union[int,Enum]                  |
| Bool                  | +      | +      | bool/bool                            |
| UUID                  | +      | +      | UUID/UUID                            |
| IPv4                  | +      | +      | IPv4Address/IPv4Address              |
| IPv6                  | +      | +      | IPv6Address/IPv6Address              |
| Array(T)              | +      | +      | List[T*]/List[T*]                    |
| LowCardinality(T)     | +      | -      | Union[str,date,datetime,int,float]/- |
| Nullable(T)           | +      | +      | Optional[T*]/Optional[T*]            |
| Nothing               | +      | +      | None/None                            |
| Interval<Type**>      | +      | +      | int/int                              |

*T - любой простой тип данных из перечисленных в таблице

**Type - тип интервала: Nanosecond, Microsecond, Millisecond, Second, Minute, Hour, Day, Week, Month, Quarter, Year

## Особенности реализации некоторых типов данных

**DateTime64**.

Данный тип требует указания precission для точности значений и часовой пояс, при этом datetime не имеет аттрибутов nanoseconds и microseconds, поэтому при извлечении из класса теряется точность, при упаковке назад без явного указания precission будет выбран формат DateTime64(3, <Часовой пояс из объекта datetime>)

**Decimal(P, S)**.

Данный тип невозможно определить автоматически при выполнении операции make. Для сохранения в Decimal необходимо явно передать тип данных в параметр dtypes.

**Enum**.

При использовании данного типа в Clickhouse индексация начинается с 1, в то время как стандартом считается значение 0.
Так же в name могут попадать запрещенные имена, такие как пустая строка и mro. Поэтому, для предотвращения проблемы конвертации,
колонка с Enum явно преобразуется в строки, соответствующие именам Enum. Для обратной записи колонка с Enum значениями напротив
будет явно преобразована в тип данных Int8/Int16.

**IPv4/IPv6**.

Данные типы данных при чтении в DataFrame могут неявно преобразовываться в строки,
что в свою очередь повлечет за собой смену типа данных колонки во время операции записи.

**LowCardinality(T)**.

Чтение из данного формата выполняется в наследованный формат, упаковка обратно в формат LowCardinality(T) не предусмотрена.

## Основной класс NativeFormat

### Не обязательные параметры при инициализации

* block_rows - максимальное количество строк в одном блоке при упаковке DataFrame в Native. Диапазон [1:1048576]. По умолчанию 65400
* logs - экземпляр класса логирования logging.Logger

### Статические методы класса и их параметры

open

* file - файл Native. Можно указать путь до файла, передать байты, открытый файл, файлоподобный объект или GzipFile
* mode - режим работы с файлом. Чтение "rb", запись "wb". По умолчанию "rb"
* write_compressed - булево, сжимать файл при создании Native из DataFrame - True, нет - False. По умолчанию False

Возвращает объект io.BufferedIOBase | gzip.GzipFile

info

* file - объект с данными io.BufferedIOBase | gzip.GzipFile | pandas.DataFrame | polars.DataFrame

Возвращает объект DataInfo

### Основные методы класса и их параметры

make

* frame - датафрейм входных данных pandas.DataFrame | polars.DataFrame
* file - объект файла для записи io.BufferedIOBase | gzip.GzipFile
* columns - [не обязательно] список имен колонок если нужно изменить некоторые названия без изменения датафрейм. Должен полностью совпадать с количеством колонок в DataFrame
* dtypes - [не обязательно] список типов данных для колонок. Если пусто типы данных будут определены автоматически

В результате работы будет создан файл Native из DataFrame, дополнительно метод ничего не возвращает

extract_block

* file - объект файла для чтения io.BufferedIOBase | gzip.GzipFile
* frame_type - объект класса FrameType для определения выходного формата. По умолчанию FrameType.Pandas

В результате работы будет возвращен объект pandas.DataFrame | polars.DataFrame, содержащий один блок из Native

extract

* file - объект файла для чтения io.BufferedIOBase | gzip.GzipFile
* frame_type - объект класса FrameType для определения выходного формата. По умолчанию FrameType.Pandas

В результате работы будет возвращен объект pandas.DataFrame | polars.DataFrame, содержащий весь файл Native

## Ошибки, возвращаемые классом NativeFormat

* NativeError - Базовая ошибка
* NativeDateError - Ошибка при получении Date/Date32
* NativeDateTimeError - Ошибка при получении DateTime/DateTime64
* NativeDTypeError - Неверный Data Type
* NativeEnumError - Неверный тип Enum
* NativePrecissionError - Неверный precission
* NativeReadError - Ошибка чтения
* NativeWriteError - Ошибка записи

## Дополнительные классы

* DataFormat - Enum для определения формата обрабатываемых данных. Является атрибутом класса DataInfo.

Возможные значения:
**Native** = 0,
**GzipNative** = 1,
**Pandas** = 2,
**Polars** = 3

* DataInfo - NamedTuple с назначенным строковым представлением.

Атрибуты класса:
**data_format** - объект DataFormat,
**columns** - список колонок,
**dtypes** - список типов данных Clickhouse,
**total_rows** - количество строк в объекте

Пример строкового представления класса DataInfo:

```bash
Data info:
──────────
Format: Native      
Total columns: 15   
Total rows: 69592   

Columns description:
────────────────────
  1. Period [ Date ]
  2. Data [ Date ]
  3. BranchGuid [ FixedString(36) ]
  4. BranchName [ String ]
  5. UserGuid [ FixedString(36) ]
  6. UserName [ String ]
  7. ProductGuid [ FixedString(36) ]
  8. NumberProduct [ LowCardinality(String) ]
  9. Product [ String ]
 10. DisplayAmount [ Int32 ]
 11. RemovingAmount [ Int32 ]
 12. Script [ Enum8('Продажи' = 1, 'Отгрузка' = 2, 'Нет' = 3) ]
 13. ProductGroup [ Enum8('ТВ/Монитор' = 1, 'КБТ' = 2, 'Средний товар' = 3, 'Мелкий товар' = 4, 'Товар с термоценником' = 5, 'Неизвестно' = 6) ]
 14. RemovingSystem [ Enum8('1С' = 1, 'Web' = 2, 'Автокасса' = 3, 'МП' = 4, 'СП' = 5, 'СЯХ' = 6, '' = 7) ]
 15. RemovingTool [ LowCardinality(String) ]
```

* FrameType - Enum для выбора формата чтения.

Возможные значения:
**Pandas** = 0,
**Polars** = 1

## Установка библиотеки

```bash
pip install .
```

## Работа с классом NativeTransfer

```python
# Импортировать логгер.
import logging
# Импортировать класс из библиотеки.
from native_transfer import NativeTransfer, FrameType
# Установить INFO level для логгера.
logging.basicConfig()
logger = logging.getLogger("NativeTransfer")
logger.setLevel(logging.INFO)
# Два необязательных параметра:
# block_rows - сколько максимально строк из DataFrame писать в один блок.
# logs - передать экземпляр логгера для логирования событий.
nt = NativeTransfer(block_rows=65_400, logs=logger)
print(nt)
# Вывод в консоль:
# ┌────────────────────────────────┐
# | NativeTransfer ver 0.0.1       |
# ╞════════════════════════════════╡
# | Write Rows Per Block : 65400   |
# └────────────────────────────────┘
# Инициализировать файл Native для чтения
file = nt.open("examples/test_read.native")
# Прочитать информацию о файле
print(nt.info(file))
# Прочитать из Native в pandas.DataFrame
frame_pandas = nt.extract(file)
print(frame_pandas)
# Прочитать информацию о датафрейм
print(nt.info(frame_pandas))
# Выбрать формат polars.DataFrame
frame_type = FrameType.Polars
# Прочитать из Native в polars.DataFrame
frame_polars = nt.extract(file, frame_type)
print(frame_polars)
# Закрыть файл
file.close()
# Создание Native файла из DataFrame
write = nt.open("examples/test_write.native.gz", "wb", write_compressed=True) # Создаваемый файл будет упакован в архив
# Создать Native файл из polars.DataFrame
nt.make(frame_polars, write)
# Закрыть файл
write.close()
# Можно еще раз открыть только записанный файл в режиме чтения
file = nt.open("examples/test_write.native.gz")
# И допустим прочитать информацию о файле
print(nt.info(file))
# И допустим еще раз распаковать датафрейм чтобы посмотреть что данные совпадают
print(nt.extract(file))
```
