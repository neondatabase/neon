# Page server architecture

The Page Server has a few different duties:

- Respond to GetPage@LSN requests from the Compute Nodes
- Receive WAL from WAL safekeeper, and store it
- Upload data to S3 to make it durable, download files from S3 as needed

S3 is the main fault-tolerant storage of all data, as there are no Page Server
replicas. We use a separate fault-tolerant WAL service to reduce latency. It
keeps track of WAL records which are not synced to S3 yet.
