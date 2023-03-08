use std::sync::Arc;

use super::world::World;

#[test]
fn start_simulation() {
    let world = Arc::new(World::new());
    let client_node = world.new_node();
}
