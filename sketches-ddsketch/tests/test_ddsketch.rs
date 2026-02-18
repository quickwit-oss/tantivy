mod common;
use std::time::Instant;

use common::dataset::Dataset;
use common::generator;
use common::generator::Generator;
use sketches_ddsketch::{Config, DDSketch};

const TEST_ALPHA: f64 = 0.01;
const TEST_MAX_BINS: u32 = 1024;
const TEST_MIN_VALUE: f64 = 1.0e-9;

// Used for float equality
const TEST_ERROR_THRESH: f64 = 1.0e-9;

const TEST_SIZES: [usize; 5] = [3, 5, 10, 100, 1000];
const TEST_QUANTILES: [f64; 10] = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1.0];

#[test]
fn test_constant() {
    evaluate_sketches(|| Box::new(generator::Constant::new(42.0)));
}

#[test]
fn test_linear() {
    evaluate_sketches(|| Box::new(generator::Linear::new(0.0, 1.0)));
}

#[test]
fn test_normal() {
    evaluate_sketches(|| Box::new(generator::Normal::new(35.0, 1.0)));
}

#[test]
fn test_lognormal() {
    evaluate_sketches(|| Box::new(generator::Lognormal::new(0.0, 2.0)));
}

#[test]
fn test_exponential() {
    evaluate_sketches(|| Box::new(generator::Exponential::new(2.0)));
}

fn evaluate_test_sizes(f: impl Fn(usize)) {
    for sz in &TEST_SIZES {
        f(*sz);
    }
}

fn evaluate_sketches(gen_factory: impl Fn() -> Box<dyn generator::Generator>) {
    evaluate_test_sizes(|sz: usize| {
        let mut generator = gen_factory();
        evaluate_sketch(sz, &mut generator);
    });
}

fn new_config() -> Config {
    Config::new(TEST_ALPHA, TEST_MAX_BINS, TEST_MIN_VALUE)
}

fn assert_float_eq(a: f64, b: f64) {
    assert!((a - b).abs() < TEST_ERROR_THRESH, "{} != {}", a, b);
}

fn evaluate_sketch(count: usize, generator: &mut Box<dyn generator::Generator>) {
    let c = new_config();
    let mut g = DDSketch::new(c);

    let mut d = Dataset::new();

    for _i in 0..count {
        let value = generator.generate();

        g.add(value);
        d.add(value);
    }

    compare_sketches(&mut d, &g);
}

fn compare_sketches(d: &mut Dataset, g: &DDSketch) {
    for q in &TEST_QUANTILES {
        let lower = d.lower_quantile(*q);
        let upper = d.upper_quantile(*q);

        let min_expected;
        if lower < 0.0 {
            min_expected = lower * (1.0 + TEST_ALPHA);
        } else {
            min_expected = lower * (1.0 - TEST_ALPHA);
        }

        let max_expected;
        if upper > 0.0 {
            max_expected = upper * (1.0 + TEST_ALPHA);
        } else {
            max_expected = upper * (1.0 - TEST_ALPHA);
        }

        let quantile = g.quantile(*q).unwrap().unwrap();

        assert!(
            min_expected <= quantile,
            "Lower than min, quantile: {}, wanted {} <= {}",
            *q,
            min_expected,
            quantile
        );
        assert!(
            quantile <= max_expected,
            "Higher than max, quantile: {}, wanted {} <= {}",
            *q,
            quantile,
            max_expected
        );

        // verify that calls do not modify result (not mut so not possible?)
        let quantile2 = g.quantile(*q).unwrap().unwrap();
        assert_eq!(quantile, quantile2);
    }

    assert_eq!(g.min().unwrap(), d.min());
    assert_eq!(g.max().unwrap(), d.max());
    assert_float_eq(g.sum().unwrap(), d.sum());
    assert_eq!(g.count(), d.count());
}

#[test]
fn test_merge_normal() {
    evaluate_test_sizes(|sz: usize| {
        let c = new_config();
        let mut d = Dataset::new();
        let mut g1 = DDSketch::new(c);

        let mut generator1 = generator::Normal::new(35.0, 1.0);
        for _ in (0..sz).step_by(3) {
            let value = generator1.generate();
            g1.add(value);
            d.add(value);
        }
        let mut g2 = DDSketch::new(c);
        let mut generator2 = generator::Normal::new(50.0, 2.0);
        for _ in (1..sz).step_by(3) {
            let value = generator2.generate();
            g2.add(value);
            d.add(value);
        }
        g1.merge(&g2).unwrap();

        let mut g3 = DDSketch::new(c);
        let mut generator3 = generator::Normal::new(40.0, 0.5);
        for _ in (2..sz).step_by(3) {
            let value = generator3.generate();
            g3.add(value);
            d.add(value);
        }
        g1.merge(&g3).unwrap();

        compare_sketches(&mut d, &g1);
    });
}

#[test]
fn test_merge_empty() {
    evaluate_test_sizes(|sz: usize| {
        let c = new_config();

        let mut d = Dataset::new();

        let mut g1 = DDSketch::new(c);
        let mut g2 = DDSketch::new(c);
        let mut generator = generator::Exponential::new(5.0);

        for _ in 0..sz {
            let value = generator.generate();
            g2.add(value);
            d.add(value);
        }
        g1.merge(&g2).unwrap();
        compare_sketches(&mut d, &g1);

        let g3 = DDSketch::new(c);
        g2.merge(&g3).unwrap();
        compare_sketches(&mut d, &g2);
    });
}

#[test]
fn test_merge_mixed() {
    evaluate_test_sizes(|sz: usize| {
        let c = new_config();
        let mut d = Dataset::new();
        let mut g1 = DDSketch::new(c);

        let mut generator1 = generator::Normal::new(100.0, 1.0);
        for _ in (0..sz).step_by(3) {
            let value = generator1.generate();
            g1.add(value);
            d.add(value);
        }

        let mut g2 = DDSketch::new(c);
        let mut generator2 = generator::Exponential::new(5.0);
        for _ in (1..sz).step_by(3) {
            let value = generator2.generate();
            g2.add(value);
            d.add(value);
        }
        g1.merge(&g2).unwrap();

        let mut g3 = DDSketch::new(c);
        let mut generator3 = generator::Exponential::new(0.1);
        for _ in (2..sz).step_by(3) {
            let value = generator3.generate();
            g3.add(value);
            d.add(value);
        }
        g1.merge(&g3).unwrap();

        compare_sketches(&mut d, &g1);
    })
}

#[test]
fn test_merge_incompatible() {
    let c1 = Config::new(TEST_ALPHA, TEST_MAX_BINS, TEST_MIN_VALUE);
    let c2 = Config::new(TEST_ALPHA * 2.0, TEST_MAX_BINS, TEST_MIN_VALUE);

    let mut d1 = DDSketch::new(c1);
    let d2 = DDSketch::new(c2);

    assert!(d1.merge(&d2).is_err());

    let c3 = Config::new(TEST_ALPHA, TEST_MAX_BINS, TEST_MIN_VALUE * 10.0);
    let d3 = DDSketch::new(c3);

    assert!(d1.merge(&d3).is_err());

    let c4 = Config::new(TEST_ALPHA, TEST_MAX_BINS * 2, TEST_MIN_VALUE);
    let d4 = DDSketch::new(c4);

    assert!(d1.merge(&d4).is_err());

    // the same should work
    let c5 = Config::new(TEST_ALPHA, TEST_MAX_BINS, TEST_MIN_VALUE);
    let dsame = DDSketch::new(c5);
    assert!(d1.merge(&dsame).is_ok());
}

#[test]
#[ignore]
fn test_performance_insert() {
    let c = Config::defaults();
    let mut g = DDSketch::new(c);
    let mut gen = generator::Normal::new(1000.0, 500.0);
    let count = 300_000_000;

    let mut values = Vec::new();
    for _ in 0..count {
        values.push(gen.generate());
    }

    let start_time = Instant::now();
    for value in values {
        g.add(value);
    }

    // This simply ensures the operations don't get optimzed out as ignored
    let quantile = g.quantile(0.50).unwrap().unwrap();

    let elapsed = start_time.elapsed().as_micros() as f64;
    let elapsed = elapsed / 1_000_000.0;

    println!(
        "RESULT: p50={:.2} => Added {}M samples in {:2} secs ({:.2}M samples/sec)",
        quantile,
        count / 1_000_000,
        elapsed,
        (count as f64) / 1_000_000.0 / elapsed
    );
}

#[test]
#[ignore]
fn test_performance_merge() {
    let c = Config::defaults();
    let mut gen = generator::Normal::new(1000.0, 500.0);
    let merge_count = 500_000;
    let sample_count = 1_000;
    let mut sketches = Vec::new();

    for _ in 0..merge_count {
        let mut d = DDSketch::new(c);
        for _ in 0..sample_count {
            d.add(gen.generate());
        }
        sketches.push(d);
    }

    let mut base = DDSketch::new(c);

    let start_time = Instant::now();
    for sketch in &sketches {
        base.merge(sketch).unwrap();
    }

    let elapsed = start_time.elapsed().as_micros() as f64;
    let elapsed = elapsed / 1_000_000.0;

    println!(
        "RESULT: Merged {} sketches in {:2} secs ({:.2} merges/sec)",
        merge_count,
        elapsed,
        (merge_count as f64) / elapsed
    );
}
