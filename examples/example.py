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
nt = NativeTransfer(block_rows=65_536, logs=logger)
print(nt)
# Вывод в консоль:
# ┌────────────────────────────────┐
# | NativeTransfer ver 0.0.1       |
# ╞════════════════════════════════╡
# | Write Rows Per Block : 65536   |
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
