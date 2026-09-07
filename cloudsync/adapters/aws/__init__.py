"""AWS adapter package (one module per resource type, dispatched below)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cloudsync.adapters.base import register_adapter
from cloudsync.adapters.aws.account import list_account
from cloudsync.adapters.aws.alb import list_alb
from cloudsync.adapters.aws.cloudfront import list_cloudfront
from cloudsync.adapters.aws.ec2 import list_ec2
from cloudsync.adapters.aws.eip import list_eip
from cloudsync.adapters.aws.elb import list_elb
from cloudsync.adapters.aws.s3 import list_s3
from cloudsync.adapters.aws.security_group import list_security_group
from cloudsync.adapters.aws.vpc import list_vpc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from cloudsync.core.accounts import AccountConfig
    from cloudsync.schemas.normalized import NormalizedResource

type Fetcher = Callable[[AccountConfig], AsyncIterator[NormalizedResource]]

PROVIDER = "aws"

# resource_type (model code) -> fetcher coroutine; grows per resource module.
# dns_zone / dns_record stay aliyun/gcp-only (AWS Route53 not in this batch).
_FETCHERS: dict[str, Fetcher] = {
    "aws_account": list_account,
    "aws_vpc": list_vpc,
    "aws_eip": list_eip,
    "aws_ec2": list_ec2,
    "aws_security_group": list_security_group,
    "aws_s3": list_s3,
    "aws_cloudfront": list_cloudfront,
    "aws_elb": list_elb,
    "aws_alb": list_alb,
}


class AwsAdapter:
    """Dispatches per resource type; unfetched types raise NotImplementedError."""

    provider: str = PROVIDER

    def default_resource_types(self) -> list[str]:
        """Default set when cmdb_sync_tasks.resource_types is empty.

        Derived from the registered fetchers so the default set can never
        contain an unimplemented type (empty whitelist = all implemented).
        """
        return sorted(_FETCHERS)

    async def list_resources(
        self, account: AccountConfig, resource_type: str
    ) -> AsyncIterator[NormalizedResource]:
        """Yield normalized resources via the per-type fetcher module."""
        fetcher = _FETCHERS.get(resource_type)
        if fetcher is None:
            raise NotImplementedError(
                f"aws adapter not implemented yet for {resource_type}"
            )
        async for resource in fetcher(account):
            yield resource


register_adapter(AwsAdapter())
