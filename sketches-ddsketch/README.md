# sketches-ddsketch

This is a direct port of the [Golang](https://github.com/DataDog/sketches-go) 
[DDSketch](https://arxiv.org/pdf/1908.10693.pdf) quantile sketch implementation 
to Rust. DDSketch is a fully-mergeable quantile sketch with relative-error 
guarantees and is extremely fast.

# DDSketch

* Sketch size automatically grows as needed, starting with 128 bins.
* Extremely fast sample insertion and sketch merges.

## Usage

```rust
use sketches_ddsketch::{Config, DDSketch};

let config = Config::defaults();
let mut sketch = DDSketch::new(c);

sketch.add(1.0);
sketch.add(1.0);
sketch.add(1.0);

// Get p=50%
let quantile = sketch.quantile(0.5).unwrap();
assert_eq!(quantile, Some(1.0));
```

## Performance

No performance tuning has been done with this implementation of the port, so we
would expect similar profiles to the original implementation.

Out of the box we see can achieve over 70M sample inserts/sec and 350K sketch
merges/sec. All tests run on a single core Intel i7 processor with 4.2Ghz max 
clock.