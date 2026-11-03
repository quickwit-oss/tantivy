use super::*;

#[test]
fn test_from_points() {
    let x = [1.0, 0.0, 0.0];
    let y = [0.0, 1.0, 0.0];
    let a = S1ChordAngle::from_points(&x, &y);
    // 90 degrees = chord^2 of 2
    assert!((a.length2() - 2.0).abs() < 1e-14);
    assert!((a.to_degrees() - 90.0).abs() < 1e-10);
}

#[test]
fn test_sin_cos() {
    let a = S1ChordAngle::from_radians(PI / 3.0); // 60 degrees
    let expected_sin = (PI / 3.0).sin();
    let expected_cos = (PI / 3.0).cos();
    assert!((a.sin() - expected_sin).abs() < 1e-14);
    assert!((a.cos() - expected_cos).abs() < 1e-14);
}

#[test]
fn test_addition() {
    let a = S1ChordAngle::from_degrees(30.0);
    let b = S1ChordAngle::from_degrees(60.0);
    let c = a + b;
    assert!((c.to_degrees() - 90.0).abs() < 1e-10);
}
