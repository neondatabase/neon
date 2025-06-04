use utils::id::TenantId;

use super::*;
use crate::World;

#[test]
fn basic() {
    let mut world = World::default();

    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();
    let timeline2 = TimelineId::generate();

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
    assert_eq!(world.get_commit_lsn_advertisements(), HashMap::default());

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
    {
        let mut advs = world.get_commit_lsn_advertisements();
        assert_eq!(advs.len(), 1);
        let advs = advs.remove(&ps1).unwrap();
        assert_eq!(advs.len(), 1);
        let (tenant_timeline_id, lsn) = advs.into_iter().next().unwrap();
        assert_eq!(
            TenantTimelineId {
                tenant_id,
                timeline_id
            },
            tenant_timeline_id
        );
        assert_eq!(lsn, Lsn(0x55));
    }
}
