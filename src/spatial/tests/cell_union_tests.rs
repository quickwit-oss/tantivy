use super::*;

#[test]
fn test_empty() {
    let cu = CellUnion::new();
    assert!(cu.is_empty());
    assert_eq!(cu.len(), 0);
    assert!(cu.is_valid());
    assert!(cu.is_normalized());
}

#[test]
fn test_whole_sphere() {
    let cu = CellUnion::whole_sphere();
    assert_eq!(cu.len(), 6);
    assert!(cu.is_normalized());
    // Should contain any point
    assert!(cu.contains_point(&[1.0, 0.0, 0.0]));
    assert!(cu.contains_point(&[0.0, 1.0, 0.0]));
    assert!(cu.contains_point(&[0.0, 0.0, 1.0]));
}

#[test]
fn test_normalize_removes_duplicates() {
    let face0 = S2CellId::from_face(0);
    let cu = CellUnion::from_cell_ids(vec![face0, face0, face0]);
    assert_eq!(cu.len(), 1);
    assert_eq!(cu.cell_id(0), face0);
}

#[test]
fn test_normalize_removes_contained() {
    let face0 = S2CellId::from_face(0);
    let child = face0.child(0);
    let grandchild = child.child(0);

    // Parent contains children, should keep only parent
    let cu = CellUnion::from_cell_ids(vec![face0, child, grandchild]);
    assert_eq!(cu.len(), 1);
    assert_eq!(cu.cell_id(0), face0);
}

#[test]
fn test_normalize_merges_siblings() {
    let face0 = S2CellId::from_face(0);
    let children = face0.children();

    // Four siblings should merge to parent
    let cu = CellUnion::from_cell_ids(vec![children[0], children[1], children[2], children[3]]);
    assert_eq!(cu.len(), 1);
    assert_eq!(cu.cell_id(0), face0);
}

#[test]
fn test_normalize_partial_siblings() {
    let face0 = S2CellId::from_face(0);
    let children = face0.children();

    // Three siblings should NOT merge
    let cu = CellUnion::from_cell_ids(vec![children[0], children[1], children[2]]);
    assert_eq!(cu.len(), 3);
}

#[test]
fn test_are_siblings() {
    let face0 = S2CellId::from_face(0);
    let children = face0.children();

    assert!(are_siblings(
        children[0],
        children[1],
        children[2],
        children[3]
    ));

    // Not siblings if one is replaced
    let face1 = S2CellId::from_face(1);
    assert!(!are_siblings(
        children[0],
        children[1],
        children[2],
        face1.child(0)
    ));
}

#[test]
fn test_contains_cell() {
    let face0 = S2CellId::from_face(0);
    let cu = CellUnion::from_cell_ids(vec![face0]);

    // Should contain face0 and all its descendants
    assert!(cu.contains(face0));
    assert!(cu.contains(face0.child(0)));
    assert!(cu.contains(face0.child(0).child(0)));

    // Should not contain other faces
    assert!(!cu.contains(S2CellId::from_face(1)));
}

#[test]
fn test_intersects_cell() {
    let face0 = S2CellId::from_face(0);
    let child = face0.child(0);
    let cu = CellUnion::from_cell_ids(vec![child]);

    // Should intersect with parent (child is contained by parent)
    assert!(cu.intersects(face0));
    // Should intersect with self
    assert!(cu.intersects(child));
    // Should not intersect with sibling
    assert!(!cu.intersects(face0.child(1)));
}

#[test]
fn test_from_min_max() {
    // Create a small range
    let face0 = S2CellId::from_face(0);
    let min = face0.child_begin_at_level(S2CellId::MAX_LEVEL);
    let max = face0
        .child(0)
        .child_end_at_level(S2CellId::MAX_LEVEL)
        .prev();

    let cu = CellUnion::from_min_max(min, max);
    assert!(cu.is_normalized());

    // Should contain the range endpoints
    assert!(cu.contains(min));
    assert!(cu.contains(max));
}

#[test]
fn test_from_begin_end_empty() {
    let face0 = S2CellId::from_face(0);
    let leaf = face0.child_begin_at_level(S2CellId::MAX_LEVEL);

    // Empty range
    let cu = CellUnion::from_begin_end(leaf, leaf);
    assert!(cu.is_empty());
}

#[test]
fn test_denormalize() {
    let face0 = S2CellId::from_face(0);
    let cu = CellUnion::from_cell_ids(vec![face0]);

    // Denormalize to level 2
    let cells = cu.denormalize(2, 1);
    assert_eq!(cells.len(), 16); // 4^2 children at level 2

    for cell in &cells {
        assert_eq!(cell.level(), 2);
    }
}

#[test]
fn test_denormalize_level_mod() {
    let level1_cell = S2CellId::from_face(0).child(0);
    let cu = CellUnion::from_cell_ids(vec![level1_cell]);

    // Denormalize with min_level=0, level_mod=3
    // Level 1 cell should expand to level 3 (next level where (level 0) % 3 == 0)
    let cells = cu.denormalize(0, 3);
    for cell in &cells {
        assert!(cell.level() % 3 == 0 || cell.level() == S2CellId::MAX_LEVEL);
    }
}

#[test]
fn test_iterator() {
    let cu = CellUnion::from_cell_ids(vec![S2CellId::from_face(0), S2CellId::from_face(1)]);

    let mut count = 0;
    for _id in &cu {
        count += 1;
    }
    assert_eq!(count, 2);
}

#[test]
fn test_from_iterator() {
    let cells = vec![S2CellId::from_face(0), S2CellId::from_face(1)];
    let cu: CellUnion = cells.into_iter().collect();
    assert_eq!(cu.len(), 2);
}

#[test]
fn test_maximum_tile() {
    let face0 = S2CellId::from_face(0);
    let begin = face0.child_begin_at_level(S2CellId::MAX_LEVEL);
    let end = face0.child_end_at_level(S2CellId::MAX_LEVEL);

    // maximum_tile of first leaf cell with face0's end as limit shoul be face0
    let tile = begin.maximum_tile(end);
    assert_eq!(tile, face0);

    // maximum_tile of cell with itself as limit returns limit
    assert_eq!(face0.maximum_tile(face0), face0);
}

#[test]
fn test_region_empty_cell_union() {
    let cu = CellUnion::new();

    // Empty cell union has empty cap bound
    let cap = cu.get_cap_bound();
    assert!(cap.is_empty());

    // Empty cell union has empty rect bound
    let rect = cu.get_rect_bound();
    assert!(rect.is_empty());

    // Empty cell union does not contain any cell
    let cell = S2Cell::new(S2CellId::from_face(0));
    assert!(!cu.contains_cell(&cell));
    assert!(!cu.may_intersect(&cell));

    // Empty cell union does not contain any point
    assert!(!cu.contains_point(&[1.0, 0.0, 0.0]));
}

#[test]
fn test_region_whole_sphere() {
    let cu = CellUnion::whole_sphere();

    // Whole sphere should have full cap bound
    let cap = cu.get_cap_bound();
    assert!(cap.is_full());

    // Whole sphere should have full rect bound
    let rect = cu.get_rect_bound();
    assert!(rect.is_full());

    // Whole sphere contains all cells
    for face in 0..6 {
        let cell = S2Cell::new(S2CellId::from_face(face));
        assert!(cu.contains_cell(&cell));
        assert!(cu.may_intersect(&cell));
    }

    // Whole sphere contains all points
    assert!(cu.contains_point(&[1.0, 0.0, 0.0]));
    assert!(cu.contains_point(&[0.0, 1.0, 0.0]));
    assert!(cu.contains_point(&[0.0, 0.0, 1.0]));
}

#[test]
fn test_region_single_face() {
    let cu = CellUnion::from_cell_ids(vec![S2CellId::from_face(0)]);

    // Cap bound should contain face 0
    let cap = cu.get_cap_bound();
    assert!(!cap.is_empty());
    assert!(!cap.is_full());

    // Should contain face 0 cell and children
    let face0 = S2Cell::new(S2CellId::from_face(0));
    assert!(cu.contains_cell(&face0));
    assert!(cu.may_intersect(&face0));

    let child = S2Cell::new(S2CellId::from_face(0).child(0));
    assert!(cu.contains_cell(&child));
    assert!(cu.may_intersect(&child));

    // Should not contain other faces
    let face1 = S2Cell::new(S2CellId::from_face(1));
    assert!(!cu.contains_cell(&face1));
    assert!(!cu.may_intersect(&face1));
}

#[test]
fn test_region_contains_cell_vs_may_intersect() {
    let face0 = S2CellId::from_face(0);
    let child = face0.child(0);
    let cu = CellUnion::from_cell_ids(vec![child]);

    let child_cell = S2Cell::new(child);
    let parent_cell = S2Cell::new(face0);

    // Cell union contains the child cell
    assert!(cu.contains_cell(&child_cell));
    assert!(cu.may_intersect(&child_cell));

    // Cell union does NOT contain the parent (parent is larger)
    assert!(!cu.contains_cell(&parent_cell));
    // But it DOES intersect the parent
    assert!(cu.may_intersect(&parent_cell));
}

#[test]
fn test_region_get_cell_union_bound() {
    let cu = CellUnion::from_cell_ids(vec![S2CellId::from_face(0).child(0)]);

    let mut cell_ids = Vec::new();
    cu.get_cell_union_bound(&mut cell_ids);

    // Should return a small covering
    assert!(!cell_ids.is_empty());
    assert!(cell_ids.len() <= 6); // At most face cells
}

#[test]
fn test_region_cap_bound_contains_all_cells() {
    // Create a cell union with multiple cells
    let cu = CellUnion::from_cell_ids(vec![
        S2CellId::from_face(0).child(0),
        S2CellId::from_face(0).child(1),
        S2CellId::from_face(1).child(0),
    ]);

    let cap = cu.get_cap_bound();

    // Cap should contain all cells' centers
    for &id in cu.cell_ids() {
        let center = id.to_point();
        assert!(
            cap.contains_point(&center),
            "Cap bound should contain cell center"
        );
    }
}

#[test]
fn test_region_rect_bound_contains_all_cells() {
    let cu = CellUnion::from_cell_ids(vec![
        S2CellId::from_face(0).child(0),
        S2CellId::from_face(0).child(1),
    ]);

    let rect = cu.get_rect_bound();

    // Rect should contain all cells' rect bounds
    for &id in cu.cell_ids() {
        let cell = S2Cell::new(id);
        let cell_rect = cell.get_rect_bound();
        assert!(
            rect.contains(&cell_rect),
            "Rect bound should contain cell rect bound"
        );
    }
}
