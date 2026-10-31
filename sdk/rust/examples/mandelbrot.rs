//! Renders rows of the Mandelbrot set with f64 maths: replicas agree bit for bit because
//! wasm floats are IEEE 754 and the sandbox canonicalises NaNs.
//! Input: width, height, first row, row count as u32 little endian; output: one byte per
//! pixel (iterations, capped at 255), row by row.

fn mandelbrot(input: &[u8]) -> Vec<u8> {
    let arg = |i: usize| u32::from_le_bytes(input[4 * i..4 * i + 4].try_into().unwrap());
    let (width, height, first, rows) = (arg(0), arg(1), arg(2), arg(3));
    let mut out = Vec::with_capacity((width * rows) as usize);
    for y in first..first + rows {
        let ci = 2.4 * y as f64 / height as f64 - 1.2;
        for x in 0..width {
            let cr = 3.5 * x as f64 / width as f64 - 2.5;
            let (mut zr, mut zi, mut n) = (0.0f64, 0.0f64, 0u8);
            while n < 255 && zr * zr + zi * zi <= 4.0 {
                (zr, zi) = (zr * zr - zi * zi + cr, 2.0 * zr * zi + ci);
                n += 1;
            }
            out.push(n);
        }
    }
    out
}

meshpool_task::task!(mandelbrot);

#[test]
fn works() {
    let input: Vec<u8> = [8u32, 8, 0, 8]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    assert_eq!(mandelbrot(&input).len(), 64);
}
