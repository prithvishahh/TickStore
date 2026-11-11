/// Small deterministic xorshift generator. The dataset and the tests share it
/// so failures reproduce from a seed without pulling in the `rand` crate.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // Zero is a fixed point of xorshift.
        Self(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform in `[0, 1)`.
    pub fn f64(&mut self) -> f64 {
        let x = self.next_u64() >> 11; // 53 bits
        (x as f64) * (1.0 / 9_007_199_254_740_992.0)
    }

    pub fn uniform_i64(&mut self, lo: i64, hi: i64) -> i64 {
        let span = (hi - lo).max(1) as u128;
        let r = (self.next_u64() >> 11) as u128;
        lo + ((r * span) >> 53) as i64
    }

    /// Box-Muller. `u1` is kept off {0, 1} so `ln` stays finite.
    pub fn normal(&mut self) -> f64 {
        let u1 = self.f64().clamp(1e-12, 1.0 - 1e-12);
        let u2 = self.f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}
