/*!
This crate provides a direct port of the [Golang](https://github.com/DataDog/sketches-go)
[DDSketch](https://arxiv.org/pdf/1908.10693.pdf) implementation to Rust. All efforts
have been made to keep this as close to the original implementation as possible, with a few tweaks to
get closer to idiomatic Rust.

# Usage

Add multiple samples to a DDSketch and invoke the `quantile` method to pull any quantile from
*0.0* to *1.0*.

```rust
use sketches_ddsketch::{Config, DDSketch};

let c = Config::defaults();
let mut d = DDSketch::new(c);

d.add(1.0);
d.add(1.0);
d.add(1.0);

let q = d.quantile(0.50).unwrap();

assert!(q < Some(1.02));
assert!(q > Some(0.98));
```

Sketches can also be merged.

```rust
use sketches_ddsketch::{Config, DDSketch};

let c = Config::defaults();
let mut d1 = DDSketch::new(c);
let mut d2 = DDSketch::new(c);

d1.add(1.0);
d2.add(2.0);
d2.add(2.0);

d1.merge(&d2);

assert_eq!(d1.count(), 3);
```

 */

pub use self::config::Config;
pub use self::ddsketch::{DDSketch, DDSketchError};
pub use self::encoding::DecodeError;

mod config;
mod ddsketch;
pub mod encoding;
mod store;
