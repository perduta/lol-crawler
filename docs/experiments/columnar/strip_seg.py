#!/usr/bin/env python3
"""Strip 16-byte block headers from a .seg file, emitting bare zstd frames."""
import struct, sys

src, dst = sys.argv[1], sys.argv[2]
blocks = 0
with open(src, "rb") as f, open(dst, "wb") as out:
    while True:
        hdr = f.read(16)
        if len(hdr) < 16:
            break
        magic, crc, clen, ulen = struct.unpack("<IIII", hdr)
        frame = f.read(clen)
        if len(frame) < clen:
            break
        out.write(frame)
        blocks += 1
print(f"{blocks} blocks", file=sys.stderr)
