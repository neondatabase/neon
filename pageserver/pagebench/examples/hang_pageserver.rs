use std::str::FromStr;

use pageserver_client::page_service::Client;
use utils::{
    id::{TenantId, TimelineId},
    logging::Output,
};

#[tokio::main]
async fn main() {
    utils::logging::init(
        utils::logging::LogFormat::Plain,
        utils::logging::TracingErrorLayerEnablement::Disabled,
        Output::Stderr,
    ).unwrap();

    let client = Client::new("postgresql://localhost:64000".to_owned())
        .await
        .unwrap();

    let mut client = Some(client);
    for i in 1..10 {
        println!("Iteration: {}", i);
        let myclient = client.take().unwrap();

        let pagestream_client = myclient
            .pagestream(
                TenantId::from_str("e0dfa97c9dc84f32ab423fe44f186283").unwrap(),
                TimelineId::from_str("585d77e52a6e43a7099c6ebaea8730c2").unwrap(),
            )
            .await
            .unwrap();

        let myclient = pagestream_client.shutdown().await.unwrap();
        client = Some(myclient);
    }

    let client = client.take().unwrap();
    client.shutdown().await.unwrap();
}
