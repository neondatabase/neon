use page_cache::FileId;
use virtual_file::VirtualFile;

mod page_cache;
mod virtual_file;

#[tokio::main]
async fn main() {
    page_cache::init(10);

    tokio::spawn(async move {
        let cache = page_cache::get();

        let res = cache
            .read_immutable_buf(page_cache::next_file_id(), 0)
            .await
            .unwrap();
        match res {
            page_cache::ReadBufResult::Found(found) => todo!(),
            page_cache::ReadBufResult::NotFound(write_guard) => {
                let file = VirtualFile::open(camino::Utf8Path::new("foo"))
                    .await
                    .unwrap();
                file.read_exact_at(write_guard, 0).await;
            }
        }
    });
}
