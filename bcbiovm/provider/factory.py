"""Cloud provider factory."""
import collections

from bcbiovm.common import constant
from bcbiovm.common import exception
from bcbiovm.provider import ship as shared_ship
from bcbiovm.provider.aws import aws_provider
from bcbiovm.provider.aws import ship as aws_ship
from bcbiovm.provider.azure import azure_provider
from bcbiovm.provider.azure import ship as azure_ship

_Ship = collections.namedtuple("Ship", ["pack", "reconstitute"])

CLOUD_PROVIDER = {
    constant.PROVIDER.AWS: aws_provider.AWSProvider,
    constant.PROVIDER.AZURE: azure_provider.AzureProvider,
}

SHIP = {
    "blob": (azure_ship.BlobPack, azure_ship.ReconstituteBlob),
    "shared": (None, shared_ship.ReconstituteShared),
    "S3": (aws_ship.S3Pack, aws_ship.ReconstituteS3),
}

SHIP_CONFIG = {
    "blob": (azure_ship.shiping_config, azure_ship.get_shiping_config),
    "shared": (shared_ship.shiping_config, shared_ship.get_shiping_config),
    "S3": (aws_ship.shiping_config, aws_ship.get_shiping_config)
}


def get(cloud_provider=constant.DEFAULT_PROVIDER):
    """Return the required cloud provider."""
    provider = CLOUD_PROVIDER.get(cloud_provider)
    if not provider:
        raise exception.NotFound(object=provider,
                                 container=CLOUD_PROVIDER.keys())
    return provider


def get_ship(provider):
    """Return the ship required for the received provider."""
    ship = SHIP.get(provider)
    if not ship:
        raise exception.NotFound(object=provider,
                                 container=SHIP.keys())

    return _Ship(pack=ship[0]() if ship[0] else None,
                 reconstitute=ship[1]() if ship[1] else None)


def get_ship_config(provider, raw=True):
    """Return the shiping configuration for the received provider."""
    ship_config = SHIP_CONFIG.get(provider)
    if not ship_config:
        raise exception.NotFound(object=provider,
                                 container=SHIP_CONFIG.keys())
    return ship_config[raw]
