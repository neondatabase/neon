#ifndef CONTROL_PLANE_CONNECTOR_H
#define CONTROL_PLANE_CONNECTOR_H

void		InitControlPlaneConnector();
void        RequestShardMapFromControlPlane(ShardMap* shard_map);

#endif
