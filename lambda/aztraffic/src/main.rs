use std::fs;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_sdk_ec2::types::{
    DestinationFileFormat, DestinationOptionsRequest, FlowLogsResourceType, LogDestinationType,
    TrafficType,
};
use aws_sdk_glue::primitives::Blob;
use aws_sdk_glue::types::{Column, DatabaseInput, SerDeInfo, StorageDescriptor, TableInput};
use aws_sdk_lambda::types::{Environment, FunctionCode, Runtime};
use aws_sdk_scheduler::types::{
    DeadLetterConfig, FlexibleTimeWindow, FlexibleTimeWindowMode, RetryPolicy, Target,
};
use aws_sdk_sfn::types::{CloudWatchLogsLogGroup, LogDestination, LogLevel, LoggingConfiguration};
use clap::Parser;
use serde_json::json;

#[derive(Parser, Clone, Debug)]
struct Args {
    #[arg(long, value_name = "id")]
    account_id: String,
    #[arg(long, value_name = "region")]
    region: String,
    #[arg(long, value_name = "cluster")]
    cluster: String,
    #[arg(long, value_name = "id")]
    vpc_id: Vec<String>,

    #[arg(long, value_name = "arn")]
    log_group_arn: String,
    #[arg(long, value_name = "name")]
    pod_info_s3_bucket_name: String,
    #[arg(
        long,
        value_name = "path",
        default_value = "CrossAZTraffic/pod_info_dumper/pod_info.csv"
    )]
    pod_info_s3_bucket_key: String,
    #[arg(long, value_name = "uri")]
    pod_info_s3_bucket_uri: String,
    #[arg(long, value_name = "uri")]
    vpc_flow_logs_s3_bucket_uri: String,
    #[arg(long, value_name = "uri")]
    results_s3_bucket_uri: String,

    #[arg(
        long,
        value_name = "name",
        default_value = "./target/lambda/pod_info_dumper/bootstrap.zip"
    )]
    lambda_zipfile_path: String,
    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-podinfo-function"
    )]
    lambda_function_name: String,
    #[arg(long, value_name = "arn")]
    lambda_role_arn: String,

    #[arg(long, value_name = "name")]
    glue_database_name: String,
    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-podinfo-table"
    )]
    glue_pod_info_table_name: String,
    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-vpcflowlogs-table"
    )]
    glue_vpc_flow_logs_table_name: String,
    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-results-table"
    )]
    glue_results_table_name: String,

    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-trigger-schedule"
    )]
    schedule_name: String,
    #[arg(long, value_name = "minutes", default_value_t = 60)]
    schedule_interval_minutes: usize,
    #[arg(long, value_name = "arn")]
    schedule_target_state_machine_arn: String,
    #[arg(long, value_name = "arn")]
    schedule_target_role_arn: String,
    #[arg(long, value_name = "arn")]
    schedule_dead_letter_queue_arn: Option<String>,

    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-combine-query"
    )]
    athena_query_name: String,

    #[arg(long, value_name = "uri")]
    vpcflowlogs_destination_s3_bucket_uri: String,

    #[arg(
        long,
        value_name = "name",
        default_value = "CrossAZTraffic-statemachine"
    )]
    statemachine_name: String,
    #[arg(long, value_name = "arn")]
    statemachine_role_arn: String,

    #[arg(long, value_name = "uri")]
    athena_results_s3_bucket_uri: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("{args:#?}");

    // TODO: athena results bucket + lifecycle config
    // TODO: iam role split
    // TODO: iam policy
    // TODO: clusterrole + binding
    // TODO: eks mapping
    // TODO: log group
    // TODO: dlq

    let sdk_config = create_sdk_config(&args).await?;

    LambdaFunction {
        local_zipfile_path: args.lambda_zipfile_path,
        function_name: args.lambda_function_name.clone(),
        role_arn: args.lambda_role_arn,
        account_id: args.account_id,
        region: args.region,
        cluster: args.cluster,
        s3_bucket_name: args.pod_info_s3_bucket_name,
        s3_bucket_key: args.pod_info_s3_bucket_key,
    }
    .create(&sdk_config)
    .await?;

    GlueDatabase {
        database_name: args.glue_database_name.clone(),
        pod_info_table_name: args.glue_pod_info_table_name.clone(),
        pod_info_s3_bucket_uri: args.pod_info_s3_bucket_uri,
        vpc_flow_logs_table_name: args.glue_vpc_flow_logs_table_name.clone(),
        vpc_flow_logs_s3_bucket_uri: args.vpc_flow_logs_s3_bucket_uri,
        results_table_name: args.glue_results_table_name.clone(),
        results_s3_bucket_uri: args.results_s3_bucket_uri,
    }
    .create(&sdk_config)
    .await?;

    let named_query_id = AthenaQuery {
        query_name: args.athena_query_name,
        glue_database: args.glue_database_name.clone(),
        invocation_frequency: args.schedule_interval_minutes,
        athena_results_table_name: args.glue_results_table_name,
        vpc_flow_logs_table_name: args.glue_vpc_flow_logs_table_name,
        pod_info_table_name: args.glue_pod_info_table_name,
    }
    .create(&sdk_config)
    .await?;

    StateMachine {
        name: args.statemachine_name,
        role_arn: args.statemachine_role_arn,
        named_query_id,
        glue_database: args.glue_database_name,
        lambda_function_name: args.lambda_function_name,
        athena_results_s3_bucket_uri: args.athena_results_s3_bucket_uri,
        log_group_arn: args.log_group_arn,
    }
    .create(&sdk_config)
    .await?;

    Schedule {
        name: args.schedule_name,
        interval_minutes: args.schedule_interval_minutes,
        dead_letter_queue_arn: args.schedule_dead_letter_queue_arn,
        target_role_arn: args.schedule_target_role_arn,
        target_state_machine_arn: args.schedule_target_state_machine_arn,
    }
    .create(&sdk_config)
    .await?;

    let flow_log_ids = VpcFlowLogs {
        vpc_ids: args.vpc_id,
        destination_s3_bucket_uri: args.vpcflowlogs_destination_s3_bucket_uri,
    }
    .create(&sdk_config)
    .await?;

    println!("VPC flow log IDs: {:?}", flow_log_ids.as_slice());

    Ok(())
}

async fn create_sdk_config(args: &Args) -> anyhow::Result<aws_config::SdkConfig> {
    let region = aws_config::Region::new(args.region.to_owned());
    let credentials_provider = DefaultCredentialsChain::builder()
        .region(region.clone())
        .build()
        .await;
    Ok(aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .credentials_provider(credentials_provider)
        .load()
        .await)
}

struct LambdaFunction {
    local_zipfile_path: String,
    function_name: String,
    role_arn: String,
    account_id: String,
    region: String,
    cluster: String,
    s3_bucket_name: String,
    s3_bucket_key: String,
}

impl LambdaFunction {
    async fn create(&self, sdk_config: &aws_config::SdkConfig) -> anyhow::Result<()> {
        let code = fs::read(&self.local_zipfile_path)?;

        let client = aws_sdk_lambda::Client::new(sdk_config);
        client
            .delete_function()
            .function_name(&self.function_name)
            .send()
            .await
            .ok();

        client
            .create_function()
            .function_name(&self.function_name)
            .runtime(Runtime::Providedal2023)
            .handler("bootstrap")
            .role(&self.role_arn)
            .code(FunctionCode::builder().zip_file(Blob::new(code)).build())
            .timeout(60)
            .environment(
                Environment::builder()
                    .set_variables(Some(
                        [
                            ("NEON_ACCOUNT_ID", self.account_id.as_str()),
                            ("NEON_REGION", self.region.as_str()),
                            ("NEON_CLUSTER", self.cluster.as_str()),
                            ("NEON_S3_BUCKET_NAME", self.s3_bucket_name.as_str()),
                            ("NEON_S3_BUCKET_KEY", self.s3_bucket_key.as_str()),
                            ("AWS_LAMBDA_LOG_FORMAT", "JSON"),
                            ("AWS_LAMBDA_LOG_LEVEL", "INFO"),
                        ]
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect(),
                    ))
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }
}

struct VpcFlowLogs {
    vpc_ids: Vec<String>,
    destination_s3_bucket_uri: String,
}

impl VpcFlowLogs {
    async fn create(&self, sdk_config: &aws_config::SdkConfig) -> anyhow::Result<Vec<String>> {
        let ec2_client = aws_sdk_ec2::Client::new(sdk_config);

        let flow_logs = ec2_client
        .create_flow_logs()
        .resource_type(FlowLogsResourceType::Vpc)
        .set_resource_ids(Some(self.vpc_ids.clone()))
        .traffic_type(TrafficType::All)
        .log_destination_type(LogDestinationType::S3)
        .log_destination(&self.destination_s3_bucket_uri)
        .destination_options(
            DestinationOptionsRequest::builder()
                .file_format(DestinationFileFormat::Parquet)
                .hive_compatible_partitions(false)
                .per_hour_partition(true)
                .build(),
        )
        .log_format("${region} ${az-id} ${vpc-id} ${flow-direction} ${pkt-srcaddr} ${pkt-dstaddr} ${srcport} ${dstport} ${start} ${bytes}")
        .send()
        .await?;

        if let Some(unsuccessful) = flow_logs
            .unsuccessful
            .as_ref()
            .and_then(|v| if v.is_empty() { None } else { Some(v) })
        {
            anyhow::bail!("VPC flow log creation unsuccessful: {unsuccessful:?}");
        }

        Ok(flow_logs.flow_log_ids().iter().cloned().collect())
    }
}

struct GlueDatabase {
    database_name: String,
    pod_info_table_name: String,
    pod_info_s3_bucket_uri: String,
    vpc_flow_logs_table_name: String,
    vpc_flow_logs_s3_bucket_uri: String,
    results_table_name: String,
    results_s3_bucket_uri: String,
}

impl GlueDatabase {
    async fn create(&self, sdk_config: &aws_config::SdkConfig) -> anyhow::Result<()> {
        let glue_client = aws_sdk_glue::Client::new(sdk_config);

        let db = DatabaseInput::builder().name(&self.database_name).build()?;

        glue_client
            .create_database()
            .database_input(db.clone())
            .send()
            .await?;

        let pod_info_columns = &[
            Column::builder()
                .name("namespace")
                .r#type("string")
                .build()?,
            Column::builder().name("name").r#type("string").build()?,
            Column::builder().name("ip").r#type("string").build()?,
            Column::builder()
                .name("creation_time")
                .r#type("timestamp")
                .build()?,
            Column::builder().name("node").r#type("string").build()?,
            Column::builder().name("az").r#type("string").build()?,
        ];
        glue_client
            .create_table()
            .database_name(db.name())
            .table_input(
                TableInput::builder()
                    .name(&self.pod_info_table_name)
                    .storage_descriptor(
                        StorageDescriptor::builder()
                            .location(&self.pod_info_s3_bucket_uri)
                            .compressed(false)
                            .set_columns(Some(pod_info_columns.into_iter().cloned().collect()))
                            .input_format("org.apache.hadoop.mapred.TextInputFormat")
                            .output_format(
                                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                            )
                            .serde_info(
                                SerDeInfo::builder()
                                    .serialization_library(
                                        "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                                    )
                                    .parameters("separatorChar", ",")
                                    .parameters("quoteChar", "`")
                                    .parameters("escapeChar", r"\")
                                    .build(),
                            )
                            .build(),
                    )
                    .table_type("EXTERNAL_TABLE")
                    .parameters("classification", "csv")
                    .parameters("skip.header.line.count", "1")
                    .retention(0)
                    .build()?,
            )
            .send()
            .await?;

        let vpc_flow_logs_columns = &[
            Column::builder().name("region").r#type("string").build()?,
            Column::builder().name("az_id").r#type("string").build()?,
            Column::builder().name("vpc_id").r#type("string").build()?,
            Column::builder()
                .name("flow_direction")
                .r#type("string")
                .build()?,
            Column::builder()
                .name("pkt_srcaddr")
                .r#type("string")
                .build()?,
            Column::builder()
                .name("pkt_dstaddr")
                .r#type("string")
                .build()?,
            Column::builder().name("srcport").r#type("int").build()?,
            Column::builder().name("dstport").r#type("int").build()?,
            Column::builder().name("start").r#type("bigint").build()?,
            Column::builder().name("bytes").r#type("bigint").build()?,
        ];
        glue_client
        .create_table()
        .database_name(db.name())
        .table_input(
            TableInput::builder()
                .name(&self.vpc_flow_logs_table_name)
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location(&self.vpc_flow_logs_s3_bucket_uri)
                        .compressed(false)
                        .set_columns(Some(vpc_flow_logs_columns.into_iter().cloned().collect()))
                        .input_format(
                            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        )
                        .output_format(
                            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        )
                        .serde_info(
                            SerDeInfo::builder()
                                .serialization_library(
                                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                )
                                .parameters("serialization.format", "1")
                                .build(),
                        )
                        .build(),
                )
                .table_type("EXTERNAL_TABLE")
                .parameters("classification", "parquet")
                .retention(0)
                .build()?,
        )
        .send()
        .await?;

        let athena_results_columns = &[
            Column::builder().name("time").r#type("timestamp").build()?,
            Column::builder().name("traffic").r#type("string").build()?,
            Column::builder()
                .name("total_bytes")
                .r#type("bigint")
                .build()?,
        ];
        glue_client
        .create_table()
        .database_name(db.name())
        .table_input(
            TableInput::builder()
                .name(&self.results_table_name)
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location(&self.results_s3_bucket_uri)
                        .compressed(false)
                        .set_columns(Some(athena_results_columns.into_iter().cloned().collect()))
                        .input_format(
                            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        )
                        .output_format(
                            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        )
                        .serde_info(
                            SerDeInfo::builder()
                                .serialization_library(
                                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                )
                                .parameters("serialization.format", "1")
                                .build(),
                        )
                        .build(),
                )
                .table_type("EXTERNAL_TABLE")
                .parameters("classification", "parquet")
                .retention(0)
                .build()?,
        )
        .send()
        .await?;

        Ok(())
    }
}

struct AthenaQuery {
    query_name: String,
    glue_database: String,
    invocation_frequency: usize,
    athena_results_table_name: String,
    vpc_flow_logs_table_name: String,
    pod_info_table_name: String,
}

impl AthenaQuery {
    async fn create(&self, sdk_config: &aws_config::SdkConfig) -> anyhow::Result<String> {
        let Self {
            athena_results_table_name,
            vpc_flow_logs_table_name,
            pod_info_table_name,
            invocation_frequency,
            ..
        } = self;

        let query_string = format!(
            r#"
INSERT INTO "{athena_results_table_name}"
WITH
  ip_addresses_and_az_mapping AS (
    SELECT
      DISTINCT pkt_srcaddr AS ipaddress,
      az_id
    FROM "{vpc_flow_logs_table_name}"
    WHERE flow_direction = 'egress'
    AND from_unixtime("{vpc_flow_logs_table_name}".start) > (CURRENT_TIMESTAMP - ({invocation_frequency} * interval '1' minute))
  ),
  egress_flows_of_pods_with_status AS (
    SELECT
      "{pod_info_table_name}".name AS srcpodname,
      pkt_srcaddr AS srcaddr,
      pkt_dstaddr AS dstaddr,
      "{vpc_flow_logs_table_name}".az_id AS srcazid,
      bytes,
      start
    FROM "{vpc_flow_logs_table_name}"
    INNER JOIN "{pod_info_table_name}" ON "{vpc_flow_logs_table_name}".pkt_srcaddr = "{pod_info_table_name}".ip
    WHERE flow_direction = 'egress'
    AND from_unixtime("{vpc_flow_logs_table_name}".start) > (CURRENT_TIMESTAMP - ({invocation_frequency} * interval '1' minute))
  ),
  cross_az_traffic_by_pod AS (
    SELECT
      srcaddr,
      srcpodname,
      dstaddr,
      "{pod_info_table_name}".name AS dstpodname,
      srcazid,
      ip_addresses_and_az_mapping.az_id AS dstazid,
      bytes,
      start
    FROM egress_flows_of_pods_with_status
    INNER JOIN "{pod_info_table_name}" ON dstaddr = "{pod_info_table_name}".ip
    LEFT JOIN ip_addresses_and_az_mapping ON dstaddr = ipaddress
    WHERE ip_addresses_and_az_mapping.az_id != srcazid
  )
SELECT
  date_trunc('MINUTE', from_unixtime(start)) AS time,
  CONCAT(srcpodname, ' -> ', dstpodname) AS traffic,
  SUM(bytes) AS total_bytes
FROM cross_az_traffic_by_pod
GROUP BY date_trunc('MINUTE', from_unixtime(start)), CONCAT(srcpodname, ' -> ', dstpodname)
ORDER BY time, total_bytes DESC
"#
        );

        let athena_client = aws_sdk_athena::Client::new(sdk_config);
        let res = athena_client
            .create_named_query()
            .name(&self.query_name)
            .database(&self.glue_database)
            .query_string(query_string)
            .send()
            .await?;

        Ok(res.named_query_id.unwrap())
    }
}

struct StateMachine {
    name: String,
    role_arn: String,
    named_query_id: String,
    glue_database: String,
    lambda_function_name: String,
    athena_results_s3_bucket_uri: String,
    log_group_arn: String,
}

impl StateMachine {
    async fn create(&self, sdk_config: &aws_config::SdkConfig) -> anyhow::Result<()> {
        let sfn_client = aws_sdk_sfn::Client::new(sdk_config);
        sfn_client
            .create_state_machine()
            .name(&self.name)
            .role_arn(&self.role_arn)
            .logging_configuration(
                LoggingConfiguration::builder()
                    .level(LogLevel::All)
                    .destinations(
                        LogDestination::builder()
                            .cloud_watch_logs_log_group(
                                CloudWatchLogsLogGroup::builder()
                                    .log_group_arn(&self.log_group_arn)
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .definition(
                json!(
                  {
                  "StartAt": "Invoke",
                  "States": {
                    "Invoke": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::lambda:invoke",
                      "Output": "{% $states.result.Payload %}",
                      "Arguments": {
                        "FunctionName": self.lambda_function_name,
                        "Payload": json!({
                            "detail-type": "Scheduled Event",
                            "source": "aws.events",
                            "detail": {}
                        }).to_string()
                      },
                      "Retry": [
                        {
                          "ErrorEquals": [
                            "Lambda.ServiceException",
                            "Lambda.AWSLambdaException",
                            "Lambda.SdkClientException",
                            "Lambda.TooManyRequestsException"
                          ],
                          "IntervalSeconds": 1,
                          "MaxAttempts": 3,
                          "BackoffRate": 2,
                          "JitterStrategy": "FULL"
                        }
                      ],
                      "Next": "Check"
                    },
                    "Check": {
                      "Type": "Choice",
                      "Choices": [
                        {
                          "Next": "GetNamedQuery",
                          "Condition": "{% $states.input.statusCode = 200 %}"
                        }
                      ],
                      "Default": "Fail"
                    },
                    "GetNamedQuery": {
                      "Type": "Task",
                      "Arguments": {
                        "NamedQueryId": self.named_query_id
                      },
                      "Resource": "arn:aws:states:::aws-sdk:athena:getNamedQuery",
                      "Output": {
                        "QueryString": "{% $states.result.NamedQuery.QueryString %}"
                      },
                      "Next": "StartQueryExecution"
                    },
                    "StartQueryExecution": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                      "Arguments": {
                        "QueryString": "{% $states.input.QueryString %}",
                        "QueryExecutionContext": {
                          "Database": self.glue_database
                        },
                        "ResultConfiguration": {
                          "OutputLocation": self.athena_results_s3_bucket_uri
                        },
                        "WorkGroup": "primary"
                      },
                      "End": true
                    },
                    "Fail": {
                      "Type": "Fail"
                    }
                  },
                  "QueryLanguage": "JSONata"
                }
                )
                .to_string(),
            )
            .send()
            .await?;

        Ok(())
    }
}

struct Schedule {
    name: String,
    interval_minutes: usize,
    target_state_machine_arn: String,
    target_role_arn: String,
    dead_letter_queue_arn: Option<String>,
}

impl Schedule {
    async fn create(&self, sdk_config: &aws_config::SdkConfig) -> anyhow::Result<()> {
        let sched_client = aws_sdk_scheduler::Client::new(sdk_config);

        sched_client
            .create_schedule()
            .name(&self.name)
            .schedule_expression(format!("rate({} minute)", self.interval_minutes))
            .flexible_time_window(
                FlexibleTimeWindow::builder()
                    .mode(FlexibleTimeWindowMode::Off)
                    .build()?,
            )
            .target(
                Target::builder()
                    .arn(&self.target_state_machine_arn)
                    .role_arn(&self.target_role_arn)
                    .input(
                        json!({
                            "detail-type": "Scheduled Event",
                            "source": "aws.events",
                            "detail": {}
                        })
                        .to_string(),
                    )
                    .retry_policy(
                        RetryPolicy::builder()
                            .maximum_retry_attempts(0)
                            .maximum_event_age_in_seconds(60)
                            .build(),
                    )
                    .set_dead_letter_config(
                        self.dead_letter_queue_arn
                            .as_ref()
                            .map(|arn| DeadLetterConfig::builder().arn(arn).build()),
                    )
                    .build()?,
            )
            .send()
            .await?;

        Ok(())
    }
}

struct KubernetesRoles {
    region: String,
    cluster: String,
    k8s_role_prefix: String,
    lambda_role_arn: String,
}

impl KubernetesRoles {
    fn print(&self) -> anyhow::Result<()> {
        let Self {
            region,
            cluster,
            k8s_role_prefix,
            lambda_role_arn,
        } = self;

        let yaml = format!(
            r#"
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {k8s_role_prefix}-clusterrole
rules:
- apiGroups:
  - ""
  resources: ["nodes", "namespaces", "pods"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {k8s_role_prefix}-binding
subjects:
- kind: Group
  name: {k8s_role_prefix}-group
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: {k8s_role_prefix}-clusterrole
  apiGroup: rbac.authorization.k8s.io
"#
        );

        let eksctl = format!(
            r#"eksctl create iamidentitymapping \
  --region "{region}"
  --cluster "{cluster}" \
  --arn "{lambda_role_arn}" \
  --username "{k8s_role_prefix}-binding" \
  --group "{k8s_role_prefix}-group"
"#
        );

        Ok(())
    }
}
