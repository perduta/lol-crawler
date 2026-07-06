// Benchmark: parse MatchRecord protobufs and transpose to columnar streams.
// Std-only port of columnar.py's encoder. Usage: transpose_bench <raw> <out>
use std::time::Instant;

struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cur<'a> {
    #[inline]
    fn varint(&mut self) -> u64 {
        let mut r: u64 = 0;
        let mut s = 0;
        loop {
            let x = self.b[self.i];
            self.i += 1;
            r |= ((x & 0x7f) as u64) << s;
            if x & 0x80 == 0 {
                return r;
            }
            s += 7;
        }
    }
    #[inline]
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let r = &self.b[self.i..self.i + n];
        self.i += n;
        r
    }
    #[inline]
    fn done(&self) -> bool {
        self.i >= self.b.len()
    }
}

#[inline]
fn wv(col: &mut Vec<u8>, mut v: u64) {
    loop {
        let x = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            col.push(x | 0x80);
        } else {
            col.push(x);
            return;
        }
    }
}

#[inline]
fn wz(col: &mut Vec<u8>, v: i64) {
    wv(col, ((v << 1) ^ (v >> 63)) as u64);
}

#[inline]
fn zz(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

const NSTAT: usize = 160;
const NRUNE: usize = 16;

struct Cols {
    misc: Vec<Vec<u8>>, // fixed named columns
    stat: Vec<Vec<u8>>,
    rune: Vec<Vec<u8>>,
}

// misc column indices
const SCHEMA: usize = 0;
const GID_D: usize = 1;
const PLAT_LEN: usize = 2;
const PLAT: usize = 3;
const QUEUE: usize = 4;
const START_D: usize = 5;
const DUR: usize = 6;
const BLUE: usize = 7;
const GV_LEN: usize = 8;
const GV: usize = 9;
const PMAJ: usize = 10;
const PMIN: usize = 11;
const BANS_N: usize = 12;
const BANS: usize = 13;
const NPARTS: usize = 14;
const P_PLAYER: usize = 15;
const P_CHAMP: usize = 16;
const P_POS: usize = 17;
const P_SP1: usize = 18;
const P_SP2: usize = 19;
const P_RUNES_N: usize = 20;
const P_STATS_N: usize = 21;
const TL_PRESENT: usize = 22;
const NFRAMES: usize = 23;
const F_MINUTE: usize = 24;
const F_GOLD_N: usize = 25;
const F_GOLD: usize = 26;
const F_XP_N: usize = 27;
const F_XP: usize = 28;
const F_CS_N: usize = 29;
const F_CS: usize = 30;
const F_DMG_N: usize = 31;
const F_DMG: usize = 32;
const NKILLS: usize = 33;
const K_T: usize = 34;
const K_KILLER: usize = 35;
const K_VICTIM: usize = 36;
const K_MASK: usize = 37;
const K_X: usize = 38;
const K_Y: usize = 39;
const NOBJS: usize = 40;
const O_T: usize = 41;
const O_KIND: usize = 42;
const O_KILLER: usize = 43;
const O_TEAM: usize = 44;
const O_LANE: usize = 45;
const NWARDS: usize = 46;
const W_T: usize = 47;
const W_KIND: usize = 48;
const W_PART: usize = 49;
const NMISC: usize = 50;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data = std::fs::read(&args[1]).unwrap();
    let t0 = Instant::now();

    let mut c = Cols {
        misc: (0..NMISC).map(|_| Vec::new()).collect(),
        stat: (0..NSTAT).map(|_| Vec::new()).collect(),
        rune: (0..NRUNE).map(|_| Vec::new()).collect(),
    };

    let mut off = 0usize;
    let mut nrec = 0u64;
    let mut prev_gid: u64 = 0;
    let mut prev_start: u64 = 0;
    // reusable frame-series state: [series][participant]
    let mut prev_series = [[0u64; 10]; 4];
    let mut series_len = [0usize; 4];

    while off + 4 <= data.len() {
        let rlen = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        let rec = &data[off..off + rlen];
        off += rlen;
        nrec += 1;

        let mut cur = Cur { b: rec, i: 0 };
        let (mut schema, mut gid, mut queue, mut start, mut dur, mut blue) =
            (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
        let (mut pmaj, mut pmin) = (0u64, 0u64);
        let mut plat: &[u8] = &[];
        let mut gv: &[u8] = &[];
        let mut nparts = 0u64;
        let mut tl_present = 0u64;

        // buffered so scalar columns stay in record order regardless of wire order
        while !cur.done() {
            let tag = cur.varint();
            let (fno, wt) = (tag >> 3, tag & 7);
            match (fno, wt) {
                (1, 0) => schema = cur.varint(),
                (2, 0) => gid = cur.varint(),
                (3, 2) => {
                    let n = cur.varint() as usize;
                    plat = cur.bytes(n);
                }
                (4, 0) => queue = cur.varint(),
                (5, 0) => start = cur.varint(),
                (6, 0) => dur = cur.varint(),
                (7, 0) => blue = cur.varint(),
                (8, 2) => {
                    let n = cur.varint() as usize;
                    gv = cur.bytes(n);
                }
                (9, 0) => pmaj = cur.varint(),
                (10, 0) => pmin = cur.varint(),
                (11, 2) => {
                    let n = cur.varint() as usize;
                    let mut p = Cur { b: cur.bytes(n), i: 0 };
                    nparts += 1;
                    while !p.done() {
                        let t = p.varint();
                        match (t >> 3, t & 7) {
                            (1, 0) => wv(&mut c.misc[P_PLAYER], p.varint()),
                            (2, 0) => wv(&mut c.misc[P_CHAMP], p.varint()),
                            (3, 0) => wv(&mut c.misc[P_POS], p.varint()),
                            (4, 0) => wv(&mut c.misc[P_SP1], p.varint()),
                            (5, 0) => wv(&mut c.misc[P_SP2], p.varint()),
                            (6, 2) => {
                                let n = p.varint() as usize;
                                let mut q = Cur { b: p.bytes(n), i: 0 };
                                let mut ri = 0;
                                while !q.done() {
                                    wv(&mut c.rune[ri], q.varint());
                                    ri += 1;
                                }
                                wv(&mut c.misc[P_RUNES_N], ri as u64);
                            }
                            (7, 2) => {
                                let n = p.varint() as usize;
                                let mut q = Cur { b: p.bytes(n), i: 0 };
                                let mut si = 0;
                                while !q.done() {
                                    // wire is zigzag varint; column keeps same encoding
                                    wv(&mut c.stat[si], q.varint());
                                    si += 1;
                                }
                                wv(&mut c.misc[P_STATS_N], si as u64);
                            }
                            _ => panic!("participant field"),
                        }
                    }
                }
                (12, 2) => {
                    tl_present = 1;
                    let n = cur.varint() as usize;
                    let mut tl = Cur { b: cur.bytes(n), i: 0 };
                    let (mut nf, mut nk, mut no, mut nw) = (0u64, 0u64, 0u64, 0u64);
                    let (mut kt, mut ot, mut wt_) = (0u64, 0u64, 0u64);
                    prev_series = [[0u64; 10]; 4];
                    series_len = [0; 4];
                    // column layout requires counts before payload; buffer sub-events
                    // are appended directly since events arrive in order
                    let mut fcols: [Vec<u8>; 4] =
                        [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
                    let mut fminutes = Vec::new();
                    let mut fns: [Vec<u8>; 4] =
                        [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
                    let mut kcols: [Vec<u8>; 6] = Default::default();
                    let mut ocols: [Vec<u8>; 5] = Default::default();
                    let mut wcols: [Vec<u8>; 3] = Default::default();
                    while !tl.done() {
                        let t = tl.varint();
                        match (t >> 3, t & 7) {
                            (1, 2) => {
                                nf += 1;
                                let n = tl.varint() as usize;
                                let mut f = Cur { b: tl.bytes(n), i: 0 };
                                while !f.done() {
                                    let t2 = f.varint();
                                    match (t2 >> 3, t2 & 7) {
                                        (1, 0) => wv(&mut fminutes, f.varint()),
                                        (s @ 2..=5, 2) => {
                                            let si = (s - 2) as usize;
                                            let n = f.varint() as usize;
                                            let mut q = Cur { b: f.bytes(n), i: 0 };
                                            let mut j = 0;
                                            while !q.done() {
                                                let v = q.varint();
                                                let pv = if j < series_len[si] {
                                                    prev_series[si][j]
                                                } else {
                                                    0
                                                };
                                                wz(&mut fcols[si], v as i64 - pv as i64);
                                                prev_series[si][j] = v;
                                                j += 1;
                                            }
                                            series_len[si] = j;
                                            wv(&mut fns[si], j as u64);
                                        }
                                        _ => panic!("frame field"),
                                    }
                                }
                            }
                            (2, 2) => {
                                nk += 1;
                                let n = tl.varint() as usize;
                                let mut k = Cur { b: tl.bytes(n), i: 0 };
                                while !k.done() {
                                    let t2 = k.varint();
                                    match (t2 >> 3, t2 & 7) {
                                        (1, 0) => {
                                            let t3 = k.varint();
                                            wz(&mut kcols[0], t3 as i64 - kt as i64);
                                            kt = t3;
                                        }
                                        (f @ 2..=6, 0) => {
                                            wv(&mut kcols[(f - 1) as usize], k.varint())
                                        }
                                        _ => panic!("kill field"),
                                    }
                                }
                            }
                            (3, 2) => {
                                no += 1;
                                let n = tl.varint() as usize;
                                let mut o = Cur { b: tl.bytes(n), i: 0 };
                                while !o.done() {
                                    let t2 = o.varint();
                                    match (t2 >> 3, t2 & 7) {
                                        (1, 0) => {
                                            let t3 = o.varint();
                                            wz(&mut ocols[0], t3 as i64 - ot as i64);
                                            ot = t3;
                                        }
                                        (f @ 2..=5, 0) => {
                                            wv(&mut ocols[(f - 1) as usize], o.varint())
                                        }
                                        _ => panic!("obj field"),
                                    }
                                }
                            }
                            (4, 2) => {
                                nw += 1;
                                let n = tl.varint() as usize;
                                let mut w = Cur { b: tl.bytes(n), i: 0 };
                                while !w.done() {
                                    let t2 = w.varint();
                                    match (t2 >> 3, t2 & 7) {
                                        (1, 0) => {
                                            let t3 = w.varint();
                                            wz(&mut wcols[0], t3 as i64 - wt_ as i64);
                                            wt_ = t3;
                                        }
                                        (f @ 2..=3, 0) => {
                                            wv(&mut wcols[(f - 1) as usize], w.varint())
                                        }
                                        _ => panic!("ward field"),
                                    }
                                }
                            }
                            _ => panic!("timeline field"),
                        }
                    }
                    wv(&mut c.misc[NFRAMES], nf);
                    c.misc[F_MINUTE].extend_from_slice(&fminutes);
                    for (si, (dst_n, dst)) in [
                        (F_GOLD_N, F_GOLD),
                        (F_XP_N, F_XP),
                        (F_CS_N, F_CS),
                        (F_DMG_N, F_DMG),
                    ]
                    .iter()
                    .enumerate()
                    {
                        c.misc[*dst_n].extend_from_slice(&fns[si]);
                        c.misc[*dst].extend_from_slice(&fcols[si]);
                    }
                    wv(&mut c.misc[NKILLS], nk);
                    for (j, col) in [K_T, K_KILLER, K_VICTIM, K_MASK, K_X, K_Y]
                        .iter()
                        .enumerate()
                    {
                        c.misc[*col].extend_from_slice(&kcols[j]);
                    }
                    wv(&mut c.misc[NOBJS], no);
                    for (j, col) in [O_T, O_KIND, O_KILLER, O_TEAM, O_LANE].iter().enumerate() {
                        c.misc[*col].extend_from_slice(&ocols[j]);
                    }
                    wv(&mut c.misc[NWARDS], nw);
                    for (j, col) in [W_T, W_KIND, W_PART].iter().enumerate() {
                        c.misc[*col].extend_from_slice(&wcols[j]);
                    }
                }
                (13, 2) => {
                    let n = cur.varint() as usize;
                    let mut q = Cur { b: cur.bytes(n), i: 0 };
                    let mut bn = 0;
                    while !q.done() {
                        wv(&mut c.misc[BANS], q.varint());
                        bn += 1;
                    }
                    wv(&mut c.misc[BANS_N], bn as u64);
                }
                _ => panic!("record field {} wt {}", fno, wt),
            }
        }

        wv(&mut c.misc[SCHEMA], schema);
        wz(&mut c.misc[GID_D], gid as i64 - prev_gid as i64);
        prev_gid = gid;
        wv(&mut c.misc[PLAT_LEN], plat.len() as u64);
        c.misc[PLAT].extend_from_slice(plat);
        wv(&mut c.misc[QUEUE], queue);
        wz(&mut c.misc[START_D], start as i64 - prev_start as i64);
        prev_start = start;
        wv(&mut c.misc[DUR], dur);
        wv(&mut c.misc[BLUE], blue);
        wv(&mut c.misc[GV_LEN], gv.len() as u64);
        c.misc[GV].extend_from_slice(gv);
        wv(&mut c.misc[PMAJ], pmaj);
        wv(&mut c.misc[PMIN], pmin);
        wv(&mut c.misc[NPARTS], nparts);
        wv(&mut c.misc[TL_PRESENT], tl_present);
        let _ = zz(0); // keep helper linked
    }

    let mut blob = Vec::new();
    for col in c.misc.iter().chain(c.stat.iter()).chain(c.rune.iter()) {
        blob.extend_from_slice(col);
    }
    let dt = t0.elapsed().as_secs_f64();
    eprintln!(
        "records={} in={} out={} parse+transpose={:.3}s ({:.0} MB/s)",
        nrec,
        data.len(),
        blob.len(),
        dt,
        data.len() as f64 / 1e6 / dt
    );
    std::fs::write(&args[2], &blob).unwrap();
}
