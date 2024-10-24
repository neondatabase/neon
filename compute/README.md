This directory contains files that are needed to build the compute
images, or included in the compute images.

compute-node.Dockerfile
	To build the compute image

vm-image-spec.yaml
	Instructions for vm-builder, to turn the compute-node image into
	corresponding vm-compute-node image.

etc/
	Configuration files included in /etc in the compute image

patches/
	Some extensions need to be patched to work with Neon. This
	directory contains such patches. They are applied to the extension
	sources in compute-node.Dockerfile

In addition to these, postgres itself, the neon postgres extension,
and compute_ctl are built and copied into the compute image by
compute-node.Dockerfile.
