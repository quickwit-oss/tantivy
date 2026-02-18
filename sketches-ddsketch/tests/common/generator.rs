extern crate rand;
extern crate rand_distr;

use rand::prelude::*;

pub trait Generator {
    fn generate(&mut self) -> f64;
}

//
// Constant generator
//
pub struct Constant {
    value: f64,
}
impl Constant {
    pub fn new(value: f64) -> Self {
        Constant { value }
    }
}
impl Generator for Constant {
    fn generate(&mut self) -> f64 {
        self.value
    }
}

//
// Linear generator
//
pub struct Linear {
    current_value: f64,
    step: f64,
}
impl Linear {
    pub fn new(start_value: f64, step: f64) -> Self {
        Linear {
            current_value: start_value,
            step,
        }
    }
}
impl Generator for Linear {
    fn generate(&mut self) -> f64 {
        let value = self.current_value;
        self.current_value += self.step;
        value
    }
}

//
// Normal distribution generator
//
pub struct Normal {
    distr: rand_distr::Normal<f64>,
}
impl Normal {
    pub fn new(mean: f64, stddev: f64) -> Self {
        Normal {
            distr: rand_distr::Normal::new(mean, stddev).unwrap(),
        }
    }
}
impl Generator for Normal {
    fn generate(&mut self) -> f64 {
        self.distr.sample(&mut rand::thread_rng())
    }
}

//
// Lognormal distribution generator
//
pub struct Lognormal {
    distr: rand_distr::LogNormal<f64>,
}
impl Lognormal {
    pub fn new(mean: f64, stddev: f64) -> Self {
        Lognormal {
            distr: rand_distr::LogNormal::new(mean, stddev).unwrap(),
        }
    }
}
impl Generator for Lognormal {
    fn generate(&mut self) -> f64 {
        self.distr.sample(&mut rand::thread_rng())
    }
}

//
// Exponential distribution generator
//
pub struct Exponential {
    distr: rand_distr::Exp<f64>,
}
impl Exponential {
    pub fn new(lambda: f64) -> Self {
        Exponential {
            distr: rand_distr::Exp::new(lambda).unwrap(),
        }
    }
}
impl Generator for Exponential {
    fn generate(&mut self) -> f64 {
        self.distr.sample(&mut rand::thread_rng())
    }
}
