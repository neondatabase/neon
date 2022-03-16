Zenith CLI allows you to operate database clusters (catalog clusters) and their commit history locally and in the cloud. Since ANSI calls them catalog clusters and cluster is a loaded term in the modern infrastructure we will call it "catalog".

# CLI v2 (after chatting with Carl)

Zenith introduces the notion of a repository.

```bash
zenith init
zenith clone zenith://zenith.tech/piedpiper/northwind -- clones a repo to the northwind directory
```

Once you have a cluster catalog you can explore it

```bash
zenith log -- returns a list of commits
zenith status -- returns if there are changes in the catalog that can be committed
zenith commit -- commits the changes and generates a new commit hash
zenith branch experimental <hash> -- creates a branch called testdb based on a given commit hash
```

To make changes in the catalog you need to run compute nodes

```bash
-- here is how you a compute node
zenith start /home/pipedpiper/northwind:main -- starts a compute instance
zenith start zenith://zenith.tech/northwind:main -- starts a compute instance in the cloud
-- you can start a compute node against any hash or branch
zenith start /home/pipedpiper/northwind:experimental --port 8008 -- start anothe compute instance (on different port)
-- you can start a compute node against any hash or branch
zenith start /home/pipedpiper/northwind:<hash> --port 8009 -- start anothe compute instance (on different port)

-- After running some DML you can run 
-- zenith status and see how there are two WAL streams one on top of 
-- the main branch
zenith status 
-- and another on top of the experimental branch
zenith status -b experimental

-- you can commit each branch separately
zenith commit main
-- or
zenith commit -c /home/pipedpiper/northwind:experimental
```

Starting compute instances against cloud environments

```bash
-- you can start a compute instance against the cloud environment
-- in this case all of the changes will be streamed into the cloud
zenith start https://zenith:tech/pipedpiper/northwind:main
zenith start https://zenith:tech/pipedpiper/northwind:main
zenith status -c https://zenith:tech/pipedpiper/northwind:main
zenith commit -c https://zenith:tech/pipedpiper/northwind:main
zenith branch -c https://zenith:tech/pipedpiper/northwind:<hash> experimental
```

Pushing data into the cloud

```bash
-- pull all the commits from the cloud
zenith pull
-- push all the commits to the cloud
zenith push
```
