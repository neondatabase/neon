use std::{
    cmp::{max, min},
    str::FromStr,
    sync::{Arc, Mutex}, time::Duration,
};

use aws_sdk_s3::{types::ObjectIdentifier, Client};
use camino::Utf8Path;
use futures::stream::{StreamExt, TryStreamExt};

use postgres_ffi::{XLogFileName, PG_TLI};
use reqwest::Url;

use safekeeper::patch_control_file;
use serde_json::json;
use tokio::{fs::File, io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, process::Command};
use tokio_postgres::types::PgLsn;
use tracing::{error, info, info_span, Instrument};
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use crate::{
    download_object_to_file, init_remote, metadata_stream::stream_listing, BucketConfig, NodeKind, RootTarget, SafekeeperApiConfig
};

/// Generally we should ask safekeepers, but so far we use everywhere default 16MB.
const WAL_SEGSIZE: usize = 16 * 1024 * 1024;

pub struct SafekeeperClient {
    token: String,
    base_url: Url,
    http_client: reqwest::Client,
}

impl SafekeeperClient {
    pub fn new(config: SafekeeperApiConfig) -> Self {
        Self {
            token: config.token,
            base_url: config.base_url,
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn timeline_status(
        &self,
        ttid: TenantTimelineId,
    ) -> anyhow::Result<safekeeper::http::routes::TimelineStatus> {
        // /v1/tenant/:tenant_id/timeline/:timeline_id
        let req = self
            .http_client
            .get(self.append_url(format!(
                "v1/tenant/{}/timeline/{}",
                ttid.tenant_id, ttid.timeline_id
            )))
            .bearer_auth(&self.token);

        let response = req.send().await?;
        let mut response: safekeeper::http::routes::TimelineStatus = response.json().await?;

        // this field is noisy
        response.acceptor_state.term_history.clear();

        Ok(response)
    }

    pub async fn patch_control_file(
        &self,
        ttid: TenantTimelineId,
        request: patch_control_file::Request,
    ) -> anyhow::Result<patch_control_file::Response> {
        // /v1/tenant/:tenant_id/timeline/:timeline_id/control_file

        let req = self
            .http_client
            .patch(self.append_url(format!(
                "v1/tenant/{}/timeline/{}/control_file",
                ttid.tenant_id, ttid.timeline_id,
            )))
            .json(&request)
            .bearer_auth(&self.token);

        let response = req.send().await?;
        let mut response: patch_control_file::Response = response.json().await?;

        // this field is noisy
        response.old_control_file.acceptor_state.term_history.0.clear();
        response.new_control_file.acceptor_state.term_history.0.clear();

        Ok(response)
    }

    fn append_url(&self, subpath: String) -> Url {
        // TODO fugly, but `.join` does not work when called
        (self.base_url.to_string() + &subpath)
            .parse()
            .unwrap_or_else(|e| panic!("Could not append {subpath} to base url: {e}"))
    }
}

#[derive(Debug)]
struct DumpedTimeline {
    ttid: TenantTimelineId,
    timeline_start_lsn: Lsn,
    local_start_lsn: Lsn,
    backup_lsn: Lsn,
    sk_id: u64,
}

fn parse_commit_lsn(formatted_string: &str) -> Option<Lsn> {
    // Split the string by the underscore '_'
    let parts: Vec<&str> = formatted_string.split('_').collect();
    
    // The fourth element (index 3) is `commit_lsn.0`, formatted as a hexadecimal string
    if parts.len() > 3 {
        Some(Lsn(u64::from_str_radix(parts[3], 16).ok()?))
    } else {
        None
    }
}

/// Check a single safekeeper timeline with local_start_lsn != timeline_start_lsn.
/// The function will find the segment at local_start_lsn and will try to generate a script to verify it.
/// 
/// If backup_lsn > local_start_lsn, this segment is no longer available locally and we have only full segment in S3.
/// In this case, segment will be downloaded with `aws s3 cp` and checked with pg_waldump, zero exitcode means success.
/// 
/// Otherwise, when backup_lsn < local_start_lsn, this segment should be partial and present on 3 safekeepers.
/// Script will download partial segment from S3 for the current safekeeper and other safekeeper. Successful validation
/// means that partial segment on current safekeeper is equal to the partial segment on the other safekeeper.
async fn validate_timeline(
    s3_client: &Client,
    root: &RootTarget,
    tli: DumpedTimeline,
    api_client: &SafekeeperClient,
    shared: Arc<SharedTimelines>,
) -> anyhow::Result<()> {
    info!("found timeline {tli:?}");

    // fetching current status from safekeeper HTTP API
    let res = api_client.timeline_status(tli.ttid).await;
    if res.is_err() {
        info!("skipping, failed to fetch info about timeline: {res:?}");
    }
    let status = res?;
    info!("status from sk: {status:?}");

    // Path to the timeline directory in S3
    let timeline_dir_target = root.safekeeper_timeline_root(&tli.ttid);

    assert!(status.backup_lsn >= tli.backup_lsn);
    assert!(status.timeline_start_lsn == tli.timeline_start_lsn);

    if status.timeline_start_lsn == status.local_start_lsn {
        info!("nothing to do, LSNs are equal");
        return Ok(());
    }

    assert!(status.local_start_lsn == tli.local_start_lsn);
    let timeline_start_lsn = status.timeline_start_lsn;
    let local_start_lsn = status.local_start_lsn;

    let segno = local_start_lsn.segment_number(WAL_SEGSIZE);
    let segfile = XLogFileName(PG_TLI, segno, WAL_SEGSIZE);

    if status.backup_lsn <= status.local_start_lsn {
        // we have partial segments, let's find them in S3 and compare in script
        info!("timeline without full backed up segment");
        let mut target = timeline_dir_target;
        target.delimiter = "".to_string();
        target.prefix_in_bucket += &segfile;
        let vec: Vec<ObjectIdentifier> = stream_listing(s3_client, &target).try_collect().await?;
        info!("found partial files: {:?}", vec);

        let expected_suffix = format!("_sk{}.partial", tli.sk_id);
        let segment_of_interest = vec.iter().find(|obj| obj.key.ends_with(&expected_suffix));

        let segment_of_interest = match segment_of_interest {
            Some(seg) => seg,
            None => {
                info!("haven't found a partial segment, skipping");
                return Ok(());
            }
        };

        let partial_prefix = segment_of_interest
            .key
            .strip_suffix(&expected_suffix)
            .unwrap();

        let another_segment = vec.iter().find(|obj| {
            obj.key != segment_of_interest.key
        });

        if another_segment.is_none() {
            info!("haven't found another partial segment to compare to");
            return Ok(());
        }
        let another_segment = another_segment.unwrap();

        let ours_commit_lsn = parse_commit_lsn(&segment_of_interest.key);
        if ours_commit_lsn.is_none() {
            info!("failed to parse commit_lsn from {}", &segment_of_interest.key);
            return Ok(());
        }
        let ours_commit_lsn = ours_commit_lsn.unwrap();

        let their_commit_lsn = parse_commit_lsn(&another_segment.key);
        if their_commit_lsn.is_none() {
            info!("failed to parse commit_lsn from {}", &another_segment.key);
            return Ok(());
        }
        let their_commit_lsn = their_commit_lsn.unwrap();

        let min_commit_lsn = min(ours_commit_lsn, their_commit_lsn);
        let strip_offset = min_commit_lsn.segment_offset(WAL_SEGSIZE);

        info!("found ours_commit_lsn={ours_commit_lsn}, their_commit_lsn={their_commit_lsn}, min={min_commit_lsn}, offset={strip_offset}");

        let ours_str = format!("./tmp/ours");
        let ours = Utf8Path::new(&ours_str);
        download_object_to_file(
            s3_client,
            root.bucket_name(),
            &segment_of_interest.key,
            None,
            &ours,
        ).await?;

        let their_str = format!("./tmp/their");
        let their = Utf8Path::new(&their_str);
        download_object_to_file(
            s3_client,
            root.bucket_name(),
            &another_segment.key,
            None,
            &their,
        ).await?;

        let cmd = format!(
            "truncate --size={} ./tmp/ours && truncate --size={} ./tmp/their && cmp -l ./tmp/ours ./tmp/their",
            strip_offset,
            strip_offset,
        );

        // Execute the command using `bash -c`
        let mut process = Command::new("bash")
            .arg("-c")
            .arg(&cmd)
            .spawn()?;

        // Await the process to finish and check the exit status
        let status = process.wait().await?;

        // let's delete the file
        tokio::fs::remove_file(ours).await?;
        tokio::fs::remove_file(their).await?;

        if status.success() {
            info!("Command executed successfully.");
            shared.append_script(&[
                format!("echo '{}' >> valid.log", tli.ttid),
            ]);
        } else {
            shared.append_script(&[
                format!("echo '{}' >> invalid.log", tli.ttid),
            ]);

            info!("Command failed with status: {}", status);
            if let Some(code) = status.code() {
                info!("Exit code: {}", code);
            } else {
                info!("Process terminated by signal");
            }
        }

        return Ok(());
    }

    // we have only full segment, let's download it and run pg_waldump
    let key = timeline_dir_target.prefix_in_bucket + &segfile;
    info!(
        "starting s3 download from bucket {}, key {}",
        root.bucket_name(),
        key
    );

    let segment_start_lsn = local_start_lsn.segment_lsn(WAL_SEGSIZE);

    let path_str = format!("./tmp/{}", segfile);
    let path = Utf8Path::new(&path_str);
    download_object_to_file(
        s3_client,
        root.bucket_name(),
        &key,
        None,
        &path,
    ).await?;

    info!("downloaded file to {}", path);

    let waldump_cmd =         format!(
        "./pg_install/v{}/bin/pg_waldump -i{} ./tmp/{}",
        status.pg_info.pg_version / 10000,
        if segment_start_lsn < timeline_start_lsn {
            format!(" -s {}", timeline_start_lsn)
        } else {
            "".to_string()
        },
        segfile,
    );

    info!("running {}", waldump_cmd);

    // Execute the command using `bash -c`
    let mut process = Command::new("bash")
        .arg("-c")
        .arg(&waldump_cmd)
        .spawn()?;

    // Await the process to finish and check the exit status
    let status = process.wait().await?;

    // let's delete the file
    tokio::fs::remove_file(path).await?;

    if status.success() {
        info!("Command executed successfully.");
        shared.append_script(&[
            format!("echo '{}' >> valid.log", tli.ttid),
        ]);
    } else {
        shared.append_script(&[
            format!("echo '{}' >> invalid.log", tli.ttid),
        ]);

        info!("Command failed with status: {}", status);
        if let Some(code) = status.code() {
            info!("Exit code: {}", code);
        } else {
            info!("Process terminated by signal");
        }
    }

    // shared.append_script(&[
    //     String::new(),
    //     format!("aws s3 cp s3://{}/{} {}", root.bucket_name(), key, segfile),
        // format!(
        //     "/usr/local/v{}/bin/pg_waldump{} {} > /dev/null",
        //     status.pg_info.pg_version / 10000,
        //     if segment_start_lsn < timeline_start_lsn {
        //         format!(" -s {}", timeline_start_lsn)
        //     } else {
        //         "".to_string()
        //     },
        //     segfile,
        // ),
    //     "if [ $? -ne 0 ]; then".to_string(),
    //     format!("   echo '{}' >> invalid.log", tli.ttid),
    //     "else".to_string(),
    //     format!("   echo '{}' >> valid.log", tli.ttid),
    //     "fi".to_string(),
    //     format!("rm {}", segfile),
    // ]);

    Ok(())
}

pub struct InternalTimelines {
    validation_script: Vec<String>,
}

// Shared memory for validation tasks, generated script will be inserted here.
pub struct SharedTimelines {
    data: Mutex<InternalTimelines>,
}

impl SharedTimelines {
    fn append_script(&self, arr: &[String]) {
        let mut lock = self.data.lock().unwrap();
        lock.validation_script.extend_from_slice(arr);
    }
}

pub async fn validate_timelines(
    bucket_config: BucketConfig,
    dump_db_connstr: String,
    dump_db_table: String,
    script_file: String,
) -> anyhow::Result<()> {
    info!(
        "checking bucket {}, region {}, dump_db_table {}",
        bucket_config.bucket, bucket_config.region, dump_db_table
    );

    let shared = Arc::new(SharedTimelines {
        data: Mutex::new(InternalTimelines {
            validation_script: vec![
                "#!/bin/bash".to_string(),
                "echo 'Starting the validation script'".to_string(),
            ],
        }),
    });

    // Use the native TLS implementation (Neon requires TLS)
    let tls_connector =
        postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new().unwrap());
    let (client, connection) = tokio_postgres::connect(&dump_db_connstr, tls_connector).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let sk_api_config = SafekeeperApiConfig::from_env()?;
    let sk_api_client = SafekeeperClient::new(sk_api_config);

    let query = format!(
        "
        select
            tenant_id, timeline_id, timeline_start_lsn, local_start_lsn, backup_lsn, sk_id
        from \"{}\"
        where
        timeline_start_lsn != local_start_lsn
AND sk_id = 51
AND timeline_id != '13a865b39537d5538e0ea74c926d9c6f'
AND timeline_id != 'c5e944b5c13628ba8fe128b01e7e663d'
AND timeline_id != 'e7cfa4a2bd15c88ff011d69feef4b076';",
        dump_db_table,
    );

    info!("query is {}", query);
    let timelines = client.query(&query, &[]).await?;
    info!("loaded {} timelines", timelines.len());

    let (s3_client, target) = init_remote(bucket_config, NodeKind::Safekeeper)?;

    let checks = futures::stream::iter(timelines.iter().map(Ok)).map_ok(|row| {
        let tenant_id = TenantId::from_str(row.get(0)).expect("failed to parse tenant_id");
        let timeline_id = TimelineId::from_str(row.get(1)).expect("failed to parse tenant_id");
        let timeline_start_lsn_pg: PgLsn = row.get(2);
        let local_start_lsn_pg: PgLsn = row.get(3);
        let backup_lsn_pg: PgLsn = row.get(4);
        let sk_id: i64 = row.get(5);
        let sk_id = sk_id as u64;

        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
        let shared = shared.clone();

        let dumped_tli = DumpedTimeline {
            ttid,
            timeline_start_lsn: Lsn(u64::from(timeline_start_lsn_pg)),
            local_start_lsn: Lsn(u64::from(local_start_lsn_pg)),
            backup_lsn: Lsn(u64::from(backup_lsn_pg)),
            sk_id,
        };

        validate_timeline(&s3_client, &target, dumped_tli, &sk_api_client, shared)
            .instrument(info_span!("validate", ttid=%ttid))
    });

    // Run tasks concurrently.
    const CONCURRENCY: usize = 1;
    let mut timelines = checks.try_buffered(CONCURRENCY);

    while let Some(r) = timelines.next().await {
        if r.is_err() {
            error!("failed to process the timeline, error: {:?}", r);
        }
    }

    // Save resulting script to the file.
    let mut file = File::create(script_file).await?;
    for line in &shared.data.lock().unwrap().validation_script {
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }
    file.flush().await?;
    drop(file);

    Ok(())
}

pub async fn fix_local_start_lsn(tli_list_file: String) -> anyhow::Result<()> {
    let sk_api_config = SafekeeperApiConfig::from_env()?;
    let sk_api_client = SafekeeperClient::new(sk_api_config);

    let mut tlis: Vec<TenantTimelineId> = Vec::new();

    let file = File::open(tli_list_file).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    
    while let Some(line) = lines.next_line().await? {
        let ttid = TenantTimelineId::from_str(&line)?;
        tlis.push(ttid);
    }
    drop(lines);
    
    info!("Read {} timelines from the file, starting applying new local_start_lsn", tlis.len());

    for ttid in tlis {
        let res = fix_one_timeline(&sk_api_client, ttid).await;
        if res.is_err() {
            error!("error while fixing timeline {}: {:?}", ttid, res);
        }
    }

    Ok(())
}

async fn fix_one_timeline(api_client: &SafekeeperClient, ttid: TenantTimelineId) -> anyhow::Result<()> {
    // fetching current status from safekeeper HTTP API
    let res = api_client.timeline_status(ttid).await;
    if res.is_err() {
        info!("skipping, failed to fetch info about timeline: {res:?}");
    }
    
    let status = res?;
    info!("status from sk: {status:?}");

    if status.timeline_start_lsn == status.local_start_lsn {
        info!("nothing to do, LSNs are equal");
        return Ok(());
    }

    let request = patch_control_file::Request {
        updates: json!({
            "local_start_lsn": status.timeline_start_lsn.to_string(),
        }),
        apply_fields: vec!["local_start_lsn".to_string()],
    };

    info!("sending request {:?}", request);
    let res = api_client.patch_control_file(ttid, request).await?;
    info!("patch finished: {:?}", res);

    Ok(())
}
