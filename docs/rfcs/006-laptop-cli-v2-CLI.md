Neon CLI allows you to operate database clusters (catalog clusters) and their commit history locally and in the cloud. Since ANSI calls them catalog clusters and cluster is a loaded term in the modern infrastructure we will call it "catalog".

# CLI v2 (after chatting with Carl)

Neon introduces the notion of a repository.

```bash
neon init
neon clone neon://neon.tech/piedpiper/northwind -- clones a repo to the northwind directory
```

Once you have a cluster catalog you can explore it

```bash
neon log -- returns a list of commits
neon status -- returns if there are changes in the catalog that can be committed
neon commit -- commits the changes and generates a new commit hash
neon branch experimental <hash> -- creates a branch called testdb based on a given commit hash
```

To make changes in the catalog you need to run compute nodes

```bash
-- here is how you a compute node
neon start /home/pipedpiper/northwind:main -- starts a compute instance
neon start neon://neon.tech/northwind:main -- starts a compute instance in the cloud
-- you can start a compute node against any hash or branch
neon start /home/pipedpiper/northwind:experimental --port 8008 -- start another compute instance (on different port)
-- you can start a compute node against any hash or branch
neon start /home/pipedpiper/northwind:<hash> --port 8009 -- start another compute instance (on different port)

-- After running some DML you can run 
-- neon status and see how there are two WAL streams one on top of 
-- the main branch
neon status 
-- and another on top of the experimental branch
neon status -b experimental

-- you can commit each branch separately
neon commit main
-- or
neon commit -c /home/pipedpiper/northwind:experimental
```

Starting compute instances against cloud environments

```bash
-- you can start a compute instance against the cloud environment
-- in this case all of the changes will be streamed into the cloud
neon start https://neon:tecj/pipedpiper/northwind:main
neon start https://neon:tecj/pipedpiper/northwind:main
neon status -c https://neon:tecj/pipedpiper/northwind:main
neon commit -c https://neon:tecj/pipedpiper/northwind:main
neon branch -c https://neon:tecj/pipedpiper/northwind:<hash> experimental
```

Pushing data into the cloud

```bash
-- pull all the commits from the cloud
neon pull
-- push all the commits to the cloud
neon push
```
