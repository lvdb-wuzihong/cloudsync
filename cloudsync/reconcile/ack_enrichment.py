"""ACK cluster metadata enrichment (design doc section 7, decision D5).

Placeholder for P2: merge ACK API metadata (API endpoint, Pod/Service CIDR,
node count, VPC, K8s version, region) into informer-created k8s_cluster rows.

Prerequisites on the bingops side (design doc section 8): C1 (fields merged
by key in cloud_consumer) and C4 (v8 migration registering cloud_cluster_id).

Mapping strategy (D5):
1. Prefer fields->>'cloud_cluster_id' = ACK native_id;
2. Fallback to exact name match with WARNING (ops should backfill cloud_cluster_id);
3. Producer never writes business tables; the fallback hit does NOT persist
   cloud_cluster_id (ops backfills via UI in v1).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("cloudsync.reconcile.ack_enrichment")


async def enrich_ack_clusters(session: AsyncSession, account_id: str) -> int:
    """Enrich k8s_cluster rows with ACK metadata for one account.

    Args:
        session: Async session on the bingops shared database.
        account_id: Aliyun account ID owning the ACK clusters.

    Returns:
        Number of enrichment messages emitted.

    Raises:
        NotImplementedError: Scheduled for P2 once ACK adapter and bingops
            prerequisites C1/C4 are in place.
    """
    # TODO(P2):
    # 1. Call ACK DescribeClusters via the aliyun adapter (native_id, name, meta);
    # 2. Match cmdb_resources k8s_cluster rows by cloud_cluster_id, fallback name;
    # 3. Emit CloudResourceMessage(resource_type="k8s_cluster", attributes={...});
    #    consumer merges attributes by key without overwriting informer fields.
    raise NotImplementedError("ACK enrichment lands in P2 (requires bingops C1/C4)")
