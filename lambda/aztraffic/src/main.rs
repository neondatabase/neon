use std::fs;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_sdk_ec2::types::{
    DestinationFileFormat, DestinationOptionsRequest, FlowLogsResourceType, LogDestinationType,
    TrafficType,
};
use aws_sdk_glue::primitives::Blob;
use aws_sdk_glue::types::{Column, DatabaseInput, SerDeInfo, StorageDescriptor, TableInput};
use aws_sdk_lambda::types::{Environment, FunctionCode, Runtime};
use clap::Parser;
use serde_json::json;

#[derive(Parser, Clone, Debug)]
struct Args {
    #[arg(long, value_name = "id")]
    account_id: String,
    #[arg(long, value_name = "region")]
    region: String,
    #[arg(long, value_name = "id")]
    vpc_id: Vec<String>,
    #[arg(long, value_name = "arn")]
    flow_logs_s3_bucket: String,

    #[command(flatten)]
    lambda: LambdaConfig,
}

#[derive(Parser, Clone, Debug)]
struct LambdaConfig {
    #[arg(long = "lambda-name", value_name = "name")]
    name: String,
    #[arg(long = "lambda-role", value_name = "iam")]
    role: String,

    #[arg(long = "lambda-target-eks-cluster-region", value_name = "region")]
    target_eks_cluster_region: String,
    #[arg(long = "lambda-target-eks-cluster-name", value_name = "cluster")]
    target_eks_cluster_name: String,

    #[arg(long = "lambda-target-s3-bucket-region", value_name = "region")]
    target_s3_bucket_region: String,
    #[arg(long = "lambda-target-s3-bucket-name", value_name = "bucket")]
    target_s3_bucket_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("{args:#?}");

    let sdk_config = create_sdk_config(&args).await?;

    // create_lambda_function(&sdk_config, &args).await?;

    // create_glue_tables(&sdk_config, &args).await?;
    // let named_query_id = create_athena_query(&sdk_config, &args).await?;
    // create_state_machine(&sdk_config, &args, &role_arn, &named_query_id, &glue_database).await?;

    let flow_log_ids = start_vpc_flow_logs(&sdk_config, &args).await?;
    println!("VPC flow log IDs: {flow_log_ids:?}");

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

async fn create_lambda_function(
    sdk_config: &aws_config::SdkConfig,
    args: &Args,
) -> anyhow::Result<()> {
    let code = fs::read("./target/lambda/pod_info_dumper/bootstrap.zip")?;

    let role = format!(
        "arn:aws:iam::{account_id}:role/{lambda_role}",
        account_id = args.account_id,
        lambda_role = args.lambda.role,
    );

    let client = aws_sdk_lambda::Client::new(sdk_config);
    client
        .delete_function()
        .function_name(args.lambda.name.clone())
        .send()
        .await
        .ok();

    client
        .create_function()
        .function_name(args.lambda.name.clone())
        .runtime(Runtime::Providedal2023)
        .handler("bootstrap")
        .role(role.clone())
        .code(FunctionCode::builder().zip_file(Blob::new(code)).build())
        .timeout(60)
        .environment(
            Environment::builder()
                .set_variables(Some(
                    [
                        ("NEON_LAMBDA_AWS_ACCOUNT_ID", args.account_id.as_str()),
                        (
                            "NEON_LAMBDA_TARGET_S3_BUCKET_REGION",
                            args.lambda.target_s3_bucket_region.as_str(),
                        ),
                        (
                            "NEON_LAMBDA_TARGET_S3_BUCKET_NAME",
                            args.lambda.target_s3_bucket_name.as_str(),
                        ),
                        (
                            "NEON_LAMBDA_TARGET_EKS_CLUSTER_REGION",
                            args.lambda.target_eks_cluster_region.as_str(),
                        ),
                        (
                            "NEON_LAMBDA_TARGET_EKS_CLUSTER_NAME",
                            args.lambda.target_eks_cluster_name.as_str(),
                        ),
                        ("AWS_LAMBDA_LOG_FORMAT", "JSON"),
                        ("AWS_LAMBDA_LOG_LEVEL", "DEBUG"),
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

async fn start_vpc_flow_logs(
    sdk_config: &aws_config::SdkConfig,
    args: &Args,
) -> anyhow::Result<Vec<String>> {
    let ec2_client = aws_sdk_ec2::Client::new(sdk_config);

    let flow_logs = ec2_client
        .create_flow_logs()
        .dry_run(false)
        .set_resource_ids(Some(args.vpc_id.clone()))
        .resource_type(FlowLogsResourceType::Vpc)
        .traffic_type(TrafficType::All)
        .log_destination_type(LogDestinationType::S3)
        .log_destination(&args.flow_logs_s3_bucket)
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

async fn create_glue_tables(
    sdk_config: &aws_config::SdkConfig,
    args: &Args,
    glue_database: &str,
    pod_info_s3_bucket_uri: &str,
    vpc_flog_logs_bucket_uri: &str,
    output_s3_bucket_uri: &str,
) -> anyhow::Result<()> {
    let glue_client = aws_sdk_glue::Client::new(sdk_config);

    let db = DatabaseInput::builder().name(glue_database).build()?;
    glue_client
        .delete_database()
        .name(db.name())
        .send()
        .await
        .ok();

    glue_client
        .create_database()
        .database_input(db.clone())
        .send()
        .await?;

    let pods_info_columns = &[
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
                .name("pods_info")
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location(pod_info_s3_bucket_uri)
                        .compressed(false)
                        .set_columns(Some(pods_info_columns.into_iter().cloned().collect()))
                        .input_format("org.apache.hadoop.mapred.TextInputFormat")
                        .output_format("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                        .serde_info(
                            SerDeInfo::builder()
                                .serialization_library("org.apache.hadoop.hive.serde2.OpenCSVSerde")
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
                .name("vpc_flow_logs")
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location(vpc_flog_logs_bucket_uri)
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
                .name("athena_results")
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location(output_s3_bucket_uri)
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

async fn create_athena_query(
    sdk_config: &aws_config::SdkConfig,
    args: &Args,
    glue_database: &str,
) -> anyhow::Result<String> {
    let query = format!(
        r#"
INSERT INTO "{athena_results_table_name}"
WITH
  ip_addresses_and_az_mapping AS (
    SELECT DISTINCT pkt_srcaddr as ipaddress, az_id
    FROM "{vpc_flow_logs_table_name}"
    WHERE flow_direction = 'egress'
    and from_unixtime("{vpc_flow_logs_table_name}".start) > (CURRENT_TIMESTAMP - ({invocation_frequency} * interval '1' minute))
  ),
  egress_flows_of_pods_with_status AS (
    SELECT
      "{pods_table_name}".name as srcpodname,
      pkt_srcaddr as srcaddr,
      pkt_dstaddr as dstaddr,
      "{vpc_flow_logs_table_name}".az_id as srcazid,
      bytes,
      start
    FROM "{vpc_flow_logs_table_name}"
    INNER JOIN "{pods_table_name}" ON "{vpc_flow_logs_table_name}".pkt_srcaddr = "{pods_table_name}".ip
    WHERE flow_direction = 'egress'
    and from_unixtime("{vpc_flow_logs_table_name}".start) > (CURRENT_TIMESTAMP - ({invocation_frequency} * interval '1' minute))
  ),
  cross_az_traffic_by_pod as (
    SELECT
      srcaddr,
      srcpodname,
      dstaddr,
      "{pods_table_name}".name as dstpodname,
      srcazid,
      ip_addresses_and_az_mapping.az_id as dstazid,
      bytes,
      start
    FROM egress_flows_of_pods_with_status
    INNER JOIN "{pods_table_name}" ON dstaddr = "{pods_table_name}".ip
    LEFT JOIN ip_addresses_and_az_mapping ON dstaddr = ipaddress
    WHERE ip_addresses_and_az_mapping.az_id != srcazid
  )
SELECT date_trunc('MINUTE', from_unixtime(start)) AS time, CONCAT(srcpodname, ' -> ', dstpodname) as traffic, sum(bytes) as total_bytes
FROM cross_az_traffic_by_pod
GROUP BY date_trunc('MINUTE', from_unixtime(start)), CONCAT(srcpodname, ' -> ', dstpodname)
ORDER BY time, total_bytes DESC
"#,
        athena_results_table_name = "athena_results",
        vpc_flow_logs_table_name = "vpc_flow_logs",
        pods_table_name = "pods_info",
        invocation_frequency = "60",
    );

    let athena_client = aws_sdk_athena::Client::new(sdk_config);
    let res = athena_client
        .create_named_query()
        .name("vpc_flow_logs_combine")
        .database(glue_database)
        .query_string(query)
        .send()
        .await?;

    Ok(res.named_query_id.unwrap())
}

async fn create_state_machine(
    sdk_config: &aws_config::SdkConfig,
    args: &Args,
    role_arn: &str,
    named_query_id: &str,
    glue_database: &str,
    lambda_function_name: &str,
    output_s3_bucket_uri: &str,
) -> anyhow::Result<()> {
    let sfn_client = aws_sdk_sfn::Client::new(sdk_config);
    sfn_client
        .create_state_machine()
        .name("vpc_flow_logs")
        .role_arn(role_arn)
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
                    "FunctionName": lambda_function_name,
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
                    "NamedQueryId": named_query_id
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
                      "Database": glue_database
                    },
                    "ResultConfiguration": {
                      "OutputLocation": output_s3_bucket_uri
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
