from typing import NamedTuple
from native_transfer import NativeTransfer
from requests import post
from uuid import uuid4

class Connect(NamedTuple):
    host: str
    port: int
    database: str
    user: str
    password: str

ch_conn = Connect("localhost", 8123, "default", "default", "")

nt = NativeTransfer()
data = nt.open("test.native.gz", "rb")  # pack native into gzip format and open it with NativeTransfer.open

# create table test_table with columns such your native file

headers = {
    "X-ClickHouse-User": ch_conn.user,
    "X-ClickHouse-Key" : ch_conn.password,
    "X-Content-Type-Options": "nosniff",
    "X-ClickHouse-Format": "Native",
    "Content-Encoding": "gzip",
}

url: str = (f"""http://{ch_conn.host}:8123/?enable_http_compression=1&http_zlib_compression_level=9""")
params={
    "database"  : ch_conn.database,
    "query"     : "INSERT INTO test_table FORMAT Native",
    "session_id": str(uuid4()),
}
resp = post(url=url, headers=headers, data=data.read(), params=params,)
print(resp)  # 200
