use std::cmp::Ordering;
use std::f64::NAN;

pub struct Dataset {
    values: Vec<f64>,
    sum: f64,
    sorted: bool,
}

fn cmp_f64(a: &f64, b: &f64) -> Ordering {
    assert!(!a.is_nan() && !b.is_nan());

    if a < b {
        return Ordering::Less;
    } else if a > b {
        return Ordering::Greater;
    } else {
        return Ordering::Equal;
    }
}

impl Dataset {
    pub fn new() -> Self {
        Dataset {
            values: Vec::new(),
            sum: 0.0,
            sorted: false,
        }
    }

    pub fn add(&mut self, value: f64) {
        self.values.push(value);
        self.sum += value;
        self.sorted = false;
    }

    // pub fn quantile(&mut self, q: f64) -> f64 {
    // self.lower_quantile(q)
    // }

    pub fn lower_quantile(&mut self, q: f64) -> f64 {
        if q < 0.0 || q > 1.0 || self.values.len() == 0 {
            return NAN;
        }

        self.sort();
        let rank = q * (self.values.len() - 1) as f64;

        self.values[rank.floor() as usize]
    }

    pub fn upper_quantile(&mut self, q: f64) -> f64 {
        if q < 0.0 || q > 1.0 || self.values.len() == 0 {
            return NAN;
        }

        self.sort();
        let rank = q * (self.values.len() - 1) as f64;
        self.values[rank.ceil() as usize]
    }

    pub fn min(&mut self) -> f64 {
        self.sort();
        self.values[0]
    }

    pub fn max(&mut self) -> f64 {
        self.sort();
        self.values[self.values.len() - 1]
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    pub fn count(&self) -> usize {
        self.values.len()
    }

    fn sort(&mut self) {
        if self.sorted {
            return;
        }

        self.values.sort_by(cmp_f64);
        self.sorted = true;
    }
}
