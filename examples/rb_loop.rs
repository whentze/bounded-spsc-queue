extern crate rb;
use rb::{RbConsumer, RbProducer, SpscRb, RB};

fn main() {
    let rb = SpscRb::new(512);
    let (prod, cons) = (rb.producer(), rb.consumer());

    for _ in 0..100_000_000 {
        prod.write(&[1]).ok();
        cons.read(&mut [0]).ok();
    };
}