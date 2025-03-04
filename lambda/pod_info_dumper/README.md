# pod_info_dumper

An event-triggered AWS lambda function that writes the list of all pods with
node information to a CSV file in S3.

```json
{
  "detail-type": "Scheduled Event",
  "source": "aws.events",
  "detail": {}
}
```
