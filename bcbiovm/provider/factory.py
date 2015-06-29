"""Cloud provider factory."""
import collections

from bcbiovm.common import constant
from bcbiovm.provider import ship
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
    "shared": (None, ship.ReconstituteShared),
    "S3": (aws_ship.S3Pack, aws_ship.ReconstituteS3),
}

SHIP_CONFIG = {
    "blob": azure_ship.shiping_config,
    "shared": ship.shared_shiping_config,
    "S3": aws_ship.shiping_config,
}


def get(cloud_provider=constant.DEFAULT_PROVIDER):
    """Return the required cloud provider."""
    # TODO(alexandrucoman): Check if received name is valid
    return CLOUD_PROVIDER.get(cloud_provider)


def get_ship(provider):
    """Return the ship required for the received provider."""
    # TODO(alexandrucoman): Check the received information
    return _Ship(*SHIP.get(provider))


def get_ship_config(provider):
    """Return the shiping configuration for the received provider."""
    # TODO(alexandrucoman): Check the received information
    return SHIP_CONFIG.get(provider)
