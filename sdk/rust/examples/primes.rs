//! Counts primes below n by trial division. Input: n as u32 little endian; output: the
//! count, u32 little endian. Same contract as examples/tasks/primes.wat.

fn primes(input: &[u8]) -> [u8; 4] {
    let n = u32::from_le_bytes(input[..4].try_into().unwrap());
    let count = (2..n)
        .filter(|&i| (2..).take_while(|d| d * d <= i).all(|d| i % d != 0))
        .count() as u32;
    count.to_le_bytes()
}

meshpool_task::task!(primes);

#[test]
fn works() {
    assert_eq!(primes(&100u32.to_le_bytes()), 25u32.to_le_bytes());
}
