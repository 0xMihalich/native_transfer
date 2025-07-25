from native_transfer import NativeTransfer, CompressionMethod
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# file = open("c:/projects/native_compress/хлам/bin/potential_sec.zstd", "rb")
file = open("c:/projects/native_compress/хлам/bin/potential_sec.lz4hc", "rb")
nt = NativeTransfer(
    logs=logger,
    make_compress=True,
    compress_method=CompressionMethod.LZ4,
)
# file = open("test.bin", "rb")
print(nt.info(file))
df = nt.extract(file)
print(df)
with open("test.bin", "wb") as f:
    nt.make(df, f)
