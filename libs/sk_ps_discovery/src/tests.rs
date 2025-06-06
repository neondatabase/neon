use utils::{id::TenantId, logging};

use super::*;
use crate::World;

#[track_caller]
fn validate_advertisements(
    actual: HashMap<NodeId, HashMap<TenantTimelineId, Lsn>>,
    expect: Vec<(NodeId, Vec<(TenantTimelineId, Lsn)>)>,
) {
    let expect: HashMap<_, _> = expect
        .into_iter()
        .map(|(node_id, innermap)| (node_id, innermap.into_iter().collect()))
        .collect();
    assert_eq!(actual, expect);
}

#[test]
fn basic() {
    let mut world = World::default();

    let tenant_id = TenantId::from_array([0xff; 16]);
    let timeline_id = TimelineId::from_array([1; 16]);
    let timeline2 = TimelineId::from_array([2; 16]);

    let attachment1 = TenantShardAttachmentId {
        tenant_id,
        shard_id: ShardIndex::unsharded(),
        generation: Generation::Valid(2),
    };
    let attachment2 = TenantShardAttachmentId {
        tenant_id,
        shard_id: ShardIndex::unsharded(),
        generation: Generation::Valid(3),
    };

    let ps1 = NodeId(0x100);

    // Out of order; in happy path, commit_lsn advances first, but let's test the
    // case where safekeeper doesn't know about the attachments yet first, before
    // we extend the case to the happy path.

    world.handle_remote_consistent_lsn_advertisement(RemoteConsistentLsnAdv {
        attachment: attachment1.timeline_attachment_id(timeline_id),
        remote_consistent_lsn: Lsn(0x23),
    });
    world.handle_remote_consistent_lsn_advertisement(RemoteConsistentLsnAdv {
        attachment: attachment2.timeline_attachment_id(timeline_id),
        remote_consistent_lsn: Lsn(0x42),
    });
    // SK authoritative info on which advertisements ought exist is still empty
    assert_eq!(world.get_commit_lsn_advertisements(), HashMap::default());
    world.update_attachment(AttachmentUpdate {
        tenant_shard_attachment_id: attachment1,
        action: AttachmentUpdateAction::Attach { ps_id: ps1 },
    });
    // We have not inserted any commit_lsn info yet, so, still no advs expected
    assert_eq!(world.get_commit_lsn_advertisements(), HashMap::default());
    // insert commit_lsn info for different timeline
    world.handle_commit_lsn_advancement(
        TenantTimelineId {
            tenant_id,
            timeline_id: timeline2,
        },
        Lsn(0x66),
    );
    // Advs should still be empty
    validate_advertisements(
        world.get_commit_lsn_advertisements(),
        vec![(
            ps1,
            vec![(
                TenantTimelineId {
                    tenant_id,
                    timeline_id: timeline2,
                },
                Lsn(0x66),
            )],
        )],
    );

    // Ok, out of order part tested. Now Safekeeper learns about the attachments.

    // insert commit_lsn info for the timeline we have remote_consistent_lsn info for
    world.handle_commit_lsn_advancement(
        TenantTimelineId {
            tenant_id,
            timeline_id,
        },
        Lsn(0x55),
    );
    dbg!(&world);
    // Now advertisements to attachment1 will be sent out, but attachment2  is still not known, so, no advertisements to it.
    validate_advertisements(
        world.get_commit_lsn_advertisements(),
        vec![(
            ps1,
            vec![(
                TenantTimelineId {
                    tenant_id,
                    timeline_id,
                },
                Lsn(0x55),
            )],
        )],
    );
}

#[test]
fn advertisement_for_new_timeline() {
    let mut world = World::default();

    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();
    let ttid = TenantTimelineId {
        tenant_id,
        timeline_id,
    };

    let tenant_shard_attachment_id = TenantShardAttachmentId {
        tenant_id,
        shard_id: ShardIndex::unsharded(),
        generation: Generation::Valid(2),
    };

    let ps_id = NodeId(0x100);

    world.update_attachment(AttachmentUpdate {
        tenant_shard_attachment_id,
        action: AttachmentUpdateAction::Attach { ps_id },
    });
    world.handle_commit_lsn_advancement(ttid, Lsn(23));

    let advs = world.get_commit_lsn_advertisements();
    validate_advertisements(advs, vec![(ps_id, vec![(ttid, Lsn(23))])]);
}

#[test]
fn quiescing_timeline_catchup() {
    let _guard = logging::init(
        logging::LogFormat::Test,
        logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
        logging::Output::Stdout,
    )
    .unwrap();

    let mut world = World::default();

    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();
    let ttid = TenantTimelineId {
        tenant_id,
        timeline_id,
    };

    let tenant_shard_attachment_id = TenantShardAttachmentId {
        tenant_id,
        shard_id: ShardIndex::unsharded(),
        generation: Generation::Valid(2),
    };

    let ps_id = NodeId(0x100);

    world.update_attachment(AttachmentUpdate {
        tenant_shard_attachment_id,
        action: AttachmentUpdateAction::Attach { ps_id },
    });
    world.handle_commit_lsn_advancement(ttid, Lsn(23));

    assert!(world.quiesced_timelines.is_empty());

    world.handle_remote_consistent_lsn_advertisement(RemoteConsistentLsnAdv {
        attachment: tenant_shard_attachment_id.timeline_attachment_id(timeline_id),
        remote_consistent_lsn: Lsn(23),
    });

    assert!(world.quiesced_timelines.contains_key(&ttid));
}

#[test]
fn nodes_timelines() {
    let mut world = World::default();

    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::from_array([0x1; 16]);
    let ttid = TenantTimelineId {
        tenant_id,
        timeline_id,
    };

    let tenant_shard_attachment_id = TenantShardAttachmentId {
        tenant_id,
        shard_id: ShardIndex::unsharded(),
        generation: Generation::Valid(2),
    };

    let ps_id = NodeId(0x100);

    world.update_attachment(AttachmentUpdate {
        tenant_shard_attachment_id,
        action: AttachmentUpdateAction::Attach { ps_id },
    });

    assert!(world.nodes_timelines.get(&ps_id).is_none());

    world.handle_commit_lsn_advancement(ttid, Lsn(0x23));

    assert_eq!(world.nodes_timelines[&ps_id].len(), 1);

    let timeline2 = TimelineId::from_array([0x2; 16]);
    world.handle_remote_consistent_lsn_advertisement(RemoteConsistentLsnAdv {
        attachment: TimelineAttachmentId {
            tenant_timeline_id: TenantTimelineId {
                tenant_id,
                timeline_id: timeline2,
            },
            shard_id: ShardIndex::unsharded(),
            generation: Generation::Valid(2),
        },
        remote_consistent_lsn: Lsn(0x42),
    });
}

// TODO: need more tests, esp for the removal path
