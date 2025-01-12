# Import the logger.
import logging
# Import the class from the library.
from native_transfer import NativeTransfer, FrameType
# Set the INFO level for the logger.
logging.basicConfig()
logger = logging.getLogger("NativeTransfer")
logger.setLevel(logging.INFO)
# Two optional parameters:
# block_rows - the maximum number of rows from the DataFrame to write in one block.
# logs - pass an instance of the logger for logging events.
nt = NativeTransfer(block_rows=65_400, logs=logger)
print(nt)
# Console output:
# ┌────────────────────────────────┐
# | NativeTransfer ver 0.0.1       |
# ╞════════════════════════════════╡
# | Write Rows Per Block : 65400   |
# └────────────────────────────────┘
# Initialize the Native file for reading
file = nt.open("examples/test_read.native")
# Read information about the file
print(nt.info(file))
# Read from Native into pandas.DataFrame
frame_pandas = nt.extract(file)
print(frame_pandas)
# Read information about the DataFrame
print(nt.info(frame_pandas))
# Choose the format polars.DataFrame
frame_type = FrameType.Polars
# Read from Native into polars.DataFrame
frame_polars = nt.extract(file, frame_type)
print(frame_polars)
# Close the file
file.close()
# Create a Native file from the DataFrame
write = nt.open("examples/test_write.native.gz", "wb", write_compressed=True) # The created file will be compressed in an archive
# Create a Native file from polars.DataFrame
nt.make(frame_polars, write)
# Close the file
write.close()
# You can open the just written file again in read mode
file = nt.open("examples/test_write.native.gz")
# And let's say read information about the file
print(nt.info(file))
# And let's say unpack the DataFrame again to see that the data matches
print(nt.extract(file))
