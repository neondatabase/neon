#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Root {
    V1(V1),
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum V1 {
    InProgress(InProgress),
    Done,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InProgress {
    pub s3_uri: String,
}
