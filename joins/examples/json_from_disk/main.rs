use std::io;
use std::marker::PhantomData;
use std::path::Path;
use bytes::Buf;
use futures03::{StreamExt, TryFutureExt};
use futures::{Poll, Stream, Future, try_ready};
use futures03::compat::{Compat, Stream01CompatExt};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::codec::{Decoder, FramedRead};
use thiserror::Error;
use tokio::io::AsyncSeekExt;
use tokio::task::{JoinError, JoinHandle};
use joins::{EquiJoin, Join, Rescan, SimpleHashJoin};

#[derive(Debug, Clone, Deserialize)]
struct User {
    id: u64,
    name: String,
}
#[derive(Debug, Clone, Deserialize)]
struct Post {
    id: u64,
    user_id: u64,
    content: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let users = JsonStreamFile::<User>::new("examples/json_from_disk/users.json").await.unwrap();
    let posts = JsonStreamFile::<Post>::new("examples/json_from_disk/posts.json").await.unwrap();

    let mut joined = SimpleHashJoin::build(
        users,
        posts,
        EquiJoin::new(|user: &User| user.id, |post: &Post| post.user_id),
        (),
        usize::MAX,
    ).compat();

    while let Some(user) = joined.next().await {
        println!("{:?}", user.unwrap());
    }
}

enum Seeking {
    Invalid,
    NotSeeking(File),
    Seeking(Compat<JoinHandle<io::Result<File>>>),
}
pub struct JsonStreamFile<T> {
    stream: Compat<FramedRead<File, StreamingJson<T>>>,
    seeking: Seeking,
}
impl<T: DeserializeOwned> JsonStreamFile<T> {
    pub async fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path).await?;
        let file2 = file.try_clone().await?;
        Ok(JsonStreamFile {
            stream: Compat::new(FramedRead::new(file, StreamingJson::new())),
            seeking: Seeking::NotSeeking(file2),
        })
    }
}

impl<T: DeserializeOwned> Stream for JsonStreamFile<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Seeking::Seeking(join_handle) = &mut self.seeking {
            let file = try_ready!(join_handle.poll())?;
            self.seeking = Seeking::NotSeeking(file);
        }
        self.stream.poll()
    }
}

impl<T: DeserializeOwned> Rescan for JsonStreamFile<T> {
    fn rescan(&mut self) {
        if let Seeking::Seeking(_) = self.seeking {
            return;
        }
        let mut file = match std::mem::replace(&mut self.seeking, Seeking::Invalid) {
            Seeking::Seeking(_) => unreachable!(),
            Seeking::Invalid => panic!("invalid Seeking state"),
            Seeking::NotSeeking(file) => file,
        };
        let join_handle = tokio::spawn(async move {
            file.rewind().await?;
            Ok(file)
        }).compat();
        self.seeking = Seeking::Seeking(join_handle);
    }
}

// utility stuff from https://github.com/serde-rs/json/issues/575#issuecomment-918346326
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to read/write data: {0}")]
    Io(#[from] io::Error),
    #[error("failed to decode/encode json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to join rescan task: {0}")]
    Join(#[from] JoinError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamingJson<T> {
    _type: PhantomData<T>,
}

impl<T> StreamingJson<T> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<T: DeserializeOwned> Decoder for StreamingJson<T> {
    type Item = T;
    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (v, consumed) = {
            let mut i = serde_json::Deserializer::from_slice(src).into_iter();
            let v = match i.next().unwrap_or(Ok(None)) {
                Ok(v) => Ok(v),
                Err(e) => {
                    if e.classify() == serde_json::error::Category::Eof {
                        Ok(None)
                    } else {
                        Err(Error::Json(e))
                    }
                }
            };
            (v, i.byte_offset())
        };
        src.advance(consumed);
        v
    }
}
