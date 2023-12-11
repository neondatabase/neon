use virtual_file::VirtualFile;

mod virtual_file;

#[tokio::main]
async fn main() {

    let file = VirtualFile::open(camino::Utf8Path::new("foo")).await.unwrap();
    file.read_exact_at().await;

}
