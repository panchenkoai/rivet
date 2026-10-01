//! Value canonicalisation the ledger's `render.canon` names, shared by the parity driver and the ledger guard.

/// Every `render.canon` value `canon` implements.
pub const CANONS: [&str; 6] = [
    "number",
    "timestamp",
    "float32",
    "float64",
    "interval",
    "round_micros",
];

/// `v` canonicalised as the row's render says.
pub fn canon(v: &Option<String>, how: &Option<String>) -> Option<String> {
    v.as_ref().map(|s| match how.as_deref() {
        Some("number") => canon_num(s),
        Some("timestamp") => canon_ts(s.trim_start_matches('+').trim().trim_end_matches("+00")),
        Some("float32") => s
            .parse::<f32>()
            .map_or_else(|e| panic!("{s}: {e}"), |f| f.to_string()),
        Some("float64") => s
            .parse::<f64>()
            .map_or_else(|e| panic!("{s}: {e}"), |f| f.to_string()),
        Some("interval") => canon_interval(s),
        Some("round_micros") => round_micros(s),
        Some(other) => panic!("unknown canon `{other}`"),
        None => s.clone(),
    })
}

/// A timestamp's text rounded half-up to the microsecond (SQL Server renders a DATETIME tick at 100 ns).
pub fn round_micros(s: &str) -> String {
    use chrono::Timelike;
    let t = chrono::NaiveDateTime::parse_from_str(
        s.trim_end_matches("+00").trim(),
        "%Y-%m-%d %H:%M:%S%.f",
    )
    .unwrap_or_else(|e| panic!("{s}: {e}"));
    let sub = i64::from(t.nanosecond() % 1_000);
    let t = t + chrono::TimeDelta::nanoseconds(if sub >= 500 { 1_000 - sub } else { -sub });
    t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// An ISO 8601 duration (`P1Y2M3DT4H5M6.5S`) or DuckDB's interval text (`1 year 2 months 3 days 04:05:06.5`) as `<months>m<days>d<micros>us`.
pub fn canon_interval(s: &str) -> String {
    let (mut months, mut days, mut micros) = (0i64, 0i64, 0i64);
    let secs = |v: &str| -> i64 {
        let (neg, v) = v.strip_prefix('-').map_or((false, v), |r| (true, r));
        let (w, f) = v.split_once('.').unwrap_or((v, ""));
        let us =
            w.parse::<i64>().unwrap() * 1_000_000 + format!("{f:0<6}")[..6].parse::<i64>().unwrap();
        if neg { -us } else { us }
    };
    if let Some(iso) = s.strip_prefix('P') {
        let (date, time) = iso.split_once('T').unwrap_or((iso, ""));
        let mut num = String::new();
        for ch in date.chars() {
            match ch {
                'Y' => months += 12 * std::mem::take(&mut num).parse::<i64>().unwrap(),
                'M' => months += std::mem::take(&mut num).parse::<i64>().unwrap(),
                'D' => days += std::mem::take(&mut num).parse::<i64>().unwrap(),
                c => num.push(c),
            }
        }
        for ch in time.chars() {
            match ch {
                'H' => micros += 3_600_000_000 * std::mem::take(&mut num).parse::<i64>().unwrap(),
                'M' => micros += 60_000_000 * std::mem::take(&mut num).parse::<i64>().unwrap(),
                'S' => micros += secs(&std::mem::take(&mut num)),
                c => num.push(c),
            }
        }
    } else {
        let words: Vec<&str> = s.split_whitespace().collect();
        let mut i = 0;
        while i < words.len() {
            if let Some((h, rest)) = words[i].split_once(':') {
                let (m, sec) = rest.split_once(':').unwrap();
                let neg = h.starts_with('-');
                let us = h.trim_start_matches('-').parse::<i64>().unwrap() * 3_600_000_000
                    + m.parse::<i64>().unwrap() * 60_000_000
                    + secs(sec);
                micros += if neg { -us } else { us };
                i += 1;
                continue;
            }
            let v: i64 = words[i].parse().unwrap();
            match words[i + 1].trim_end_matches('s') {
                "year" => months += 12 * v,
                "month" | "mon" => months += v,
                "day" => days += v,
                unit => panic!("interval unit `{unit}` in `{s}`"),
            }
            i += 2;
        }
    }
    format!("{months}m{days}d{micros}us")
}

/// An exact decimal string in canonical form: no exponent, no trailing fraction zeros.
pub fn canon_num(s: &str) -> String {
    let s = s.trim();
    let (neg, s) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    let (mant, exp) = match s.split_once(['E', 'e']) {
        Some((m, e)) => (m, e.parse::<i64>().unwrap()),
        None => (s, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    let all = format!("{int}{frac}");
    let zeros = all.len() - all.trim_start_matches('0').len();
    let digits = &all[zeros..];
    // Position of the decimal point within `digits`.
    let point = int.len() as i64 + exp - zeros as i64;
    let (i, f) = if digits.is_empty() {
        ("0".to_string(), String::new())
    } else if point <= 0 {
        (
            "0".to_string(),
            format!("{}{digits}", "0".repeat((-point) as usize)),
        )
    } else if point as usize >= digits.len() {
        (
            format!("{digits}{}", "0".repeat(point as usize - digits.len())),
            String::new(),
        )
    } else {
        (
            digits[..point as usize].to_string(),
            digits[point as usize..].to_string(),
        )
    };
    let f = f.trim_end_matches('0');
    let body = if f.is_empty() { i } else { format!("{i}.{f}") };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// A timestamp's text with trailing fractional zeros (and a bare `.`) dropped.
pub fn canon_ts(s: &str) -> String {
    match s.split_once('.') {
        Some((a, f)) if f.trim_end_matches('0').is_empty() => a.to_string(),
        Some((a, f)) => format!("{a}.{}", f.trim_end_matches('0')),
        None => s.to_string(),
    }
}
