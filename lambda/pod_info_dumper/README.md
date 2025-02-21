# pod_info_dumper

An event-triggered AWS lambda function that writes the list of all pods with
node information to a CSV file in S3.

```shell
cargo lambda build -p pod_info_dumper --output-format Zip --x86-64 --profile release-lambda-function
```
