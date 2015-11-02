"""The commands used by the command line parser."""
from bcbiovm import log as logging
from bcbiovm.client import base
from bcbiovm.client import commands as client_commands
from bcbiovm.client.commands import provider
from bcbiovm.client.commands import container
from bcbiovm.client.commands.provider import aws as aws_provider
from bcbiovm.client.commands.provider import azure as azure_provider

LOG = logging.get_logger(__name__)


class Config(base.Group):

    """Manipulate elasticluster configuration files, providing easy
    ways to edit in place.
    """

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "config",
            help="Define configuration details for running a cluster")
        actions = parser.add_subparsers(
            title="[configuration specific actions]")

        self._register_parser("actions", actions)


class Docker(base.Group):

    """Utilities to help with develping using bcbion inside of docker."""

    commands = [
        (container.docker.SetupInstall, "actions"),
        (container.docker.SystemUpdate, "actions")
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "devel",
            help=("Utilities to help with develping using bcbion"
                  "inside of docker."))
        actions = parser.add_subparsers(title="[devel commands]")
        self._register_parser("actions", actions)


class DockerAWS(Docker):

    """Utilities to help with develping using bcbion inside of docker."""

    commands = [
        (aws_provider.docker.Build, "actions"),
        (aws_provider.docker.BiodataUpload, "actions"),
    ]
    commands.extend(Docker.commands)


class DockerAzure(Docker):

    """Utilities to help with develping using bcbion inside of docker."""

    commands = [
        (azure_provider.docker.Build, "actions"),
        (azure_provider.docker.BiodataUpload, "actions"),
    ]
    commands.extend(Docker.commands)


class ElastiCluster(base.Group):

    """Run and manage a cluster using elasticluster."""

    commands = [
        (provider.cluster.Bootstrap, "actions"),
        (provider.cluster.Start, "actions"),
        (provider.cluster.Stop, "actions"),
        (provider.cluster.Setup, "actions"),
        (provider.cluster.SSHConnection, "actions"),
        (provider.cluster.Command, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "cluster", help="Run and manage AWS clusters")
        actions = parser.add_subparsers(title="[cluster specific actions]")

        self._register_parser("actions", actions)


class ICELCommand(base.Group):

    """Create scratch filesystem using Intel Cloud Edition for Lustre."""

    commands = [
        (aws_provider.icel.Create, "actions"),
        (aws_provider.icel.Specification, "actions"),
        (aws_provider.icel.Mount, "actions"),
        (aws_provider.icel.Unmount, "actions"),
        (aws_provider.icel.Stop, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "icel",
            help=("Create scratch filesystem using Intel Cloud Edition"
                  "for Lustre"))
        actions = parser.add_subparsers(title="[icel create]")
        self._register_parser("actions", actions)


class PrepareEnvironment(base.Group):

    commands = [
        (azure_provider.prepare.ManagementCertificate, "actions"),
        (azure_provider.prepare.PrivateKey, "actions"),
        (azure_provider.prepare.ECConfig, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "prepare",
            help=("Utilities to help with environment configuration."))
        actions = parser.add_subparsers(title="[devel commands]")

        self._register_parser("actions", actions)


class Tools(base.Group):

    """Tools and utilities."""

    commands = [
        (client_commands.tools.S3Upload, "storage_manager"),
        (client_commands.tools.BlobUpload, "storage_manager"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "tools", help="Tools and utilities.")
        tools = parser.add_subparsers(title="[available tools]")
        upload = tools.add_parser(
            "upload", help="Upload file to a storage manager.")
        storage_manager = upload.add_subparsers(title="[storage manager]")

        self._register_parser("tools", tools)
        self._register_parser("storage_manager", storage_manager)


class AWSProvider(base.Group):

    """Automate resources for running bcbio on AWS."""

    commands = [
        (aws_provider.bootstrap.IdentityAccessManagement, "actions"),
        (aws_provider.bootstrap.VirtualPrivateCloud, "actions"),
        (aws_provider.clusterk.ClusterK, "actions"),
        (client_commands.common.Graph, "actions"),
        (client_commands.common.Info, "actions"),
        (provider.cluster.EditConfig, "actions"),
        (DockerAWS, "actions"),
        (ElastiCluster, "actions"),
        (ICELCommand, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "aws",
            help="Automate resources for running bcbio on AWS")
        actions = parser.add_subparsers(title="[aws commands]")
        self._register_parser("actions", actions)


class AzureProvider(base.Group):

    """Automate resources for running bcbio on Azure."""
    commands = [
        (client_commands.common.Info, "actions"),
        (client_commands.common.Graph, "actions"),
        (provider.cluster.EditConfig, "actions"),
        (DockerAzure, "actions"),
        (ElastiCluster, "actions"),
        (PrepareEnvironment, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "azure",
            help="Automate resources for running bcbio on Azure")
        actions = parser.add_subparsers(title="[azure commands]")
        self._register_parser("actions", actions)
