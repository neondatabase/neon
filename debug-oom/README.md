To build a compute image:
```
docker build --build-arg GIT_VERSION=custombuild --build-arg PG_VERSION=v16 -t neon-local-v16 -f ../compute/compute-node.Dockerfile .. && \
../../autoscaling/bin/vm-builder \
            -spec=../compute/vm-image-spec-bullseye.yaml \
            -src=neon-local-v16:latest \
            -dst=vm-neon-local-v16:latest \
            -target-arch=linux/amd64 \
            -size 2G && \
../../autoscaling/bin/kind load docker-image vm-neon-local-v16:latest --name neonvm-arthur
```

To start a compute node:
```
kubectl apply -f ./spec.yml
```

How to destroy:
```
kubectl delete -f ./spec.yml
```