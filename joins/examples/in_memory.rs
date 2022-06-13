use joins::{EquiJoin, JoinInMemory, SimpleHashJoin};

#[derive(Debug, Clone)]
#[allow(unused)]
struct User {
    id: u64,
    name: String,
}
#[derive(Debug, Clone)]
#[allow(unused)]
struct Post {
    id: u64,
    user_id: u64,
    content: String,
}

fn main() {
    let users = (0..50).map(|id| User { id, name: "".to_string() });
    let posts = (0..50).flat_map(|user_id| [user_id, user_id])
        .enumerate()
        .map(|(id, user_id)| Post { id: id as u64, user_id, content: "".to_string() });

    let join = SimpleHashJoin::build_in_memory(
        users,
        posts,
        EquiJoin::new(|user: &User| user.id, |post: &Post| post.user_id),
        usize::MAX,
    );
    for res in join {
        println!("{res:?}");
    }
}
