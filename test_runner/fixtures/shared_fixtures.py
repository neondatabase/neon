from typing import Optional, List, Dict, Any

from fixtures.common_types import TimelineId, Lsn
from fixtures.neon_fixtures import PgProtocol, Safekeeper


class TestEndpoint(PgProtocol):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def start(self):
        pass

class TestTimeline:
    primary: Optional[TestEndpoint]
    secondaries: List[TestEndpoint]

    def __init__(self):
        self.primary = None

    def start_primary(self) -> Optional[TestEndpoint]:
        if not self.primary:
            return None
        self.primary.start()

        return self.primary

class TestPageserver:
    pass

class TestSafekeeper:
    pass

class TestTenant:
    """
    An object representing a single test case on a shared pageserver.
    All operations here are safe practically safe.
    """

    def __init__(
        self,
        pageserver: TestPageserver,
        safekeeper: Safekeeper,
        main_timeline_name: Optional[str] = "main",
    ):
        self.pageserver = pageserver
        self.safekeeper = safekeeper
        self.timeline_map: Dict[str, TestTimeline] = dict()

        self.timeline_map[main_timeline_name] = self.primary_timeline

    def create_timeline(
        self,
        name: str,
        parent_name: Optional[str],
        parent_id: Optional[TimelineId],
        branch_at: Optional[Lsn],
    ) -> TestTimeline:
        if (tlid := self.timeline_map.get(name)) is not None:
            return tlid

        return self._new_timeline()

    def _new_timeline(self) -> TestTimeline:
        pass

