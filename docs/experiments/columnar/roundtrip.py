#!/usr/bin/env python3
"""Prove the columnar transform is lossless: columnarize records, then
reconstruct each record's exact protobuf bytes from the columns alone and
compare against the originals.

The rebuilder consumes only the column streams (via read cursors), never
the source bytes, so byte equality is a real roundtrip proof.
"""
import struct, sys, time
from collections import defaultdict

# ---------- wire helpers ----------

def rvarint(buf, i):
    r = 0; s = 0
    while True:
        b = buf[i]; i += 1
        r |= (b & 0x7F) << s
        if not b & 0x80:
            return r, i
        s += 7

def evarint(v):
    out = bytearray()
    while True:
        x = v & 0x7F
        v >>= 7
        if v:
            out.append(x | 0x80)
        else:
            out.append(x)
            return bytes(out)

def zz_enc(v):
    return (v << 1) ^ (v >> 63) if v < 0 else v << 1

def zz_dec(v):
    return (v >> 1) ^ -(v & 1)

def parse_msg(buf):
    out = defaultdict(list)
    i = 0
    while i < len(buf):
        tag, i = rvarint(buf, i)
        fno, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = rvarint(buf, i)
        elif wt == 2:
            ln, i = rvarint(buf, i)
            v = buf[i:i+ln]; i += ln
        else:
            raise ValueError(f"unexpected wiretype {wt} field {fno}")
        out[fno].append((wt, v))
    return out

def packed(buf):
    vals = []; i = 0
    while i < len(buf):
        v, i = rvarint(buf, i)
        vals.append(v)
    return vals

# ---------- column store ----------

class Cols:
    def __init__(self):
        self.w = defaultdict(bytearray)   # write side
        self.rpos = {}                    # read cursors

    def wv(self, col, v):
        b = self.w[col]
        while True:
            x = v & 0x7F
            v >>= 7
            if v:
                b.append(x | 0x80)
            else:
                b.append(x)
                return

    def wz(self, col, v):
        self.wv(col, zz_enc(v))

    def wbytes(self, col, b):
        self.w[col] += b

    def rv(self, col):
        v, self.rpos[col] = rvarint(self.w[col], self.rpos.get(col, 0))
        return v

    def rz(self, col):
        return zz_dec(self.rv(col))

    def rbytes(self, col, n):
        i = self.rpos.get(col, 0)
        b = bytes(self.w[col][i:i+n])
        self.rpos[col] = i + n
        return b

# ---------- encode: record -> columns ----------

def enc_record(rec, C, st8):
    m = parse_msg(rec)
    C.wv("schema", m[1][0][1] if 1 in m else 0)
    gid = m[2][0][1] if 2 in m else 0
    C.wz("game_id_d", gid - st8["gid"]); st8["gid"] = gid
    plat = m[3][0][1] if 3 in m else b""
    C.wv("plat_len", len(plat)); C.wbytes("plat", plat)
    C.wv("queue", m[4][0][1] if 4 in m else 0)
    st = m[5][0][1] if 5 in m else 0
    C.wz("start_d", st - st8["start"]); st8["start"] = st
    C.wv("dur", m[6][0][1] if 6 in m else 0)
    C.wv("blue_won", m[7][0][1] if 7 in m else 0)
    gv = m[8][0][1] if 8 in m else b""
    C.wv("gv_len", len(gv)); C.wbytes("gv", gv)
    C.wv("patch_maj", m[9][0][1] if 9 in m else 0)
    C.wv("patch_min", m[10][0][1] if 10 in m else 0)
    bans = packed(m[13][0][1]) if 13 in m else []
    C.wv("bans_n", len(bans))
    for b in bans: C.wv("bans", b)

    parts = [parse_msg(v) for _, v in m.get(11, [])]
    C.wv("n_parts", len(parts))
    for p in parts:
        C.wv("p_player", p[1][0][1] if 1 in p else 0)
        C.wv("p_champ", p[2][0][1] if 2 in p else 0)
        C.wv("p_pos", p[3][0][1] if 3 in p else 0)
        C.wv("p_sp1", p[4][0][1] if 4 in p else 0)
        C.wv("p_sp2", p[5][0][1] if 5 in p else 0)
        runes = packed(p[6][0][1]) if 6 in p else []
        C.wv("p_runes_n", len(runes))
        for ri, rv_ in enumerate(runes):
            C.wv(f"p_rune_{ri:02d}", rv_)
        stats = [zz_dec(v) for v in packed(p[7][0][1])] if 7 in p else []
        C.wv("p_stats_n", len(stats))
        for si, sv in enumerate(stats):
            C.wz(f"stat_{si:03d}", sv)

    C.wv("tl_present", 1 if 12 in m else 0)
    if 12 in m:
        tl = parse_msg(m[12][0][1])
        frames = [parse_msg(v) for _, v in tl.get(1, [])]
        C.wv("n_frames", len(frames))
        prev = {}
        for f in frames:
            C.wv("f_minute", f[1][0][1] if 1 in f else 0)
            for fno, name in ((2, "gold"), (3, "xp"), (4, "cs"), (5, "dmg")):
                vals = packed(f[fno][0][1]) if fno in f else []
                C.wv(f"f_{name}_n", len(vals))
                pv = prev.get(name, [])
                for j, v in enumerate(vals):
                    C.wz(f"f_{name}", v - (pv[j] if j < len(pv) else 0))
                prev[name] = vals
        kills = [parse_msg(v) for _, v in tl.get(2, [])]
        C.wv("n_kills", len(kills))
        pt = 0
        for k in kills:
            t = k[1][0][1] if 1 in k else 0
            C.wz("k_t", t - pt); pt = t
            C.wv("k_killer", k[2][0][1] if 2 in k else 0)
            C.wv("k_victim", k[3][0][1] if 3 in k else 0)
            C.wv("k_mask", k[4][0][1] if 4 in k else 0)
            C.wv("k_x", k[5][0][1] if 5 in k else 0)
            C.wv("k_y", k[6][0][1] if 6 in k else 0)
        objs = [parse_msg(v) for _, v in tl.get(3, [])]
        C.wv("n_objs", len(objs))
        pt = 0
        for o in objs:
            t = o[1][0][1] if 1 in o else 0
            C.wz("o_t", t - pt); pt = t
            for fno, nm in ((2, "o_kind"), (3, "o_killer"), (4, "o_team"), (5, "o_lane")):
                C.wv(nm, o[fno][0][1] if fno in o else 0)
        wards = [parse_msg(v) for _, v in tl.get(4, [])]
        C.wv("n_wards", len(wards))
        pt = 0
        for w in wards:
            t = w[1][0][1] if 1 in w else 0
            C.wz("w_t", t - pt); pt = t
            C.wv("w_kind", w[2][0][1] if 2 in w else 0)
            C.wv("w_part", w[3][0][1] if 3 in w else 0)

# ---------- decode: columns -> record bytes ----------

def fv(fno, v):
    """proto3 scalar: omit default 0."""
    if v == 0:
        return b""
    return evarint(fno << 3) + evarint(v)

def fpacked(fno, vals):
    if not vals:
        return b""
    body = b"".join(evarint(v) for v in vals)
    return evarint(fno << 3 | 2) + evarint(len(body)) + body

def fbytes(fno, b):
    if not b:
        return b""
    return evarint(fno << 3 | 2) + evarint(len(b)) + b

def dec_record(C, st8):
    out = bytearray()
    out += fv(1, C.rv("schema"))
    gid = st8["gid"] + C.rz("game_id_d"); st8["gid"] = gid
    out += fv(2, gid)
    out += fbytes(3, C.rbytes("plat", C.rv("plat_len")))
    out += fv(4, C.rv("queue"))
    st = st8["start"] + C.rz("start_d"); st8["start"] = st
    out += fv(5, st)
    out += fv(6, C.rv("dur"))
    out += fv(7, C.rv("blue_won"))
    out += fbytes(8, C.rbytes("gv", C.rv("gv_len")))
    out += fv(9, C.rv("patch_maj"))
    out += fv(10, C.rv("patch_min"))

    for _ in range(C.rv("n_parts")):
        p = bytearray()
        p += fv(1, C.rv("p_player"))
        p += fv(2, C.rv("p_champ"))
        p += fv(3, C.rv("p_pos"))
        p += fv(4, C.rv("p_sp1"))
        p += fv(5, C.rv("p_sp2"))
        p += fpacked(6, [C.rv(f"p_rune_{ri:02d}") for ri in range(C.rv("p_runes_n"))])
        p += fpacked(7, [zz_enc(C.rz(f"stat_{si:03d}")) for si in range(C.rv("p_stats_n"))])
        out += fbytes(11, bytes(p))

    if C.rv("tl_present"):
        tl = bytearray()
        prev = {}
        for _ in range(C.rv("n_frames")):
            f = bytearray()
            f += fv(1, C.rv("f_minute"))
            for fno, name in ((2, "gold"), (3, "xp"), (4, "cs"), (5, "dmg")):
                n = C.rv(f"f_{name}_n")
                pv = prev.get(name, [])
                vals = []
                for j in range(n):
                    vals.append(C.rz(f"f_{name}") + (pv[j] if j < len(pv) else 0))
                prev[name] = vals
                f += fpacked(fno, vals)
            tl += fbytes(1, bytes(f))
        pt = 0
        for _ in range(C.rv("n_kills")):
            k = bytearray()
            pt += C.rz("k_t")
            k += fv(1, pt)
            k += fv(2, C.rv("k_killer"))
            k += fv(3, C.rv("k_victim"))
            k += fv(4, C.rv("k_mask"))
            k += fv(5, C.rv("k_x"))
            k += fv(6, C.rv("k_y"))
            tl += fbytes(2, bytes(k))
        pt = 0
        for _ in range(C.rv("n_objs")):
            o = bytearray()
            pt += C.rz("o_t")
            o += fv(1, pt)
            o += fv(2, C.rv("o_kind"))
            o += fv(3, C.rv("o_killer"))
            o += fv(4, C.rv("o_team"))
            o += fv(5, C.rv("o_lane"))
            tl += fbytes(3, bytes(o))
        pt = 0
        for _ in range(C.rv("n_wards")):
            w = bytearray()
            pt += C.rz("w_t")
            w += fv(1, pt)
            w += fv(2, C.rv("w_kind"))
            w += fv(3, C.rv("w_part"))
            tl += fbytes(4, bytes(w))
        out += fbytes(12, bytes(tl))

    # bans written last on the wire (field 13)
    bans = [C.rv("bans") for _ in range(C.rv("bans_n"))]
    out += fpacked(13, bans)
    return bytes(out)

# ---------- main ----------

data = open(sys.argv[1], "rb").read()
limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10**9

t0 = time.time()
C = Cols()
recs = []
off = 0
st8 = {"gid": 0, "start": 0}
while off + 4 <= len(data) and len(recs) < limit:
    rlen = struct.unpack_from("<I", data, off)[0]; off += 4
    rec = data[off:off+rlen]; off += rlen
    recs.append(rec)
    enc_record(rec, C, st8)
t_enc = time.time() - t0

t0 = time.time()
st8 = {"gid": 0, "start": 0}
bad = 0
for idx, orig in enumerate(recs):
    rebuilt = dec_record(C, st8)
    if rebuilt != orig:
        bad += 1
        if bad <= 3:
            print(f"MISMATCH record {idx}: orig {len(orig)}B rebuilt {len(rebuilt)}B")
            for i, (a, b) in enumerate(zip(orig, rebuilt)):
                if a != b:
                    print(f"  first diff at byte {i}: {a:02x} vs {b:02x}")
                    print(f"  orig   ...{orig[max(0,i-8):i+8].hex()}")
                    print(f"  rebuilt...{rebuilt[max(0,i-8):i+8].hex()}")
                    break
t_dec = time.time() - t0

n = len(recs)
mb = sum(len(r) for r in recs) / 1e6
print(f"records={n} bytes={mb:.1f}MB encode={t_enc:.1f}s decode+verify={t_dec:.1f}s "
      f"({mb/t_dec:.0f} MB/s rebuild)")
print("LOSSLESS: all records byte-identical" if bad == 0 else f"FAILED: {bad} mismatches")
