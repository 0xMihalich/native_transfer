from enum import Enum


class CompressionMethod(Enum):
    """List of compression codecs."""

    NONE = 0x02
    LZ4 = 0x82
    ZSTD = 0x90
    Multiple = 0x91
    Delta = 0x92
    T64 = 0x93
    DoubleDelta = 0x94
    Gorilla = 0x95
    AES_128_GCM_SIV = 0x96
    AES_256_GCM_SIV = 0x97
    FPC = 0x98
    DeflateQpl = 0x99
    GCD = 0x9a
    ZSTD_QPL = 0x9b
    SZ3 = 0x9c
