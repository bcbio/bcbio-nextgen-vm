"""Manage installation and updates of bcbio_vm on AWS systems.
"""

# Core and memory usage for AWS instances
AWS_INFO = {
    "m3.large": [2, 3500],
    "m3.xlarge": [4, 3500],
    "m3.2xlarge": [8, 3500],
    "c3.large": [2, 1750],
    "c3.xlarge": [4, 1750],
    "c3.2xlarge": [8, 1750],
    "c3.4xlarge": [16, 1750],
    "c3.8xlarge": [32, 1750],
    "c4.xlarge": [4, 1750],
    "c4.2xlarge": [8, 1750],
    "c4.4xlarge": [16, 1750],
    "c4.8xlarge": [36, 1600],
    "r3.large": [2, 7000],
    "r3.xlarge": [4, 7000],
    "r3.2xlarge": [8, 7000],
    "r3.4xlarge": [16, 7000],
    "r3.8xlarge": [32, 7000],
}
