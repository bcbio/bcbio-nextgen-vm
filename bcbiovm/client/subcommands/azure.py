"""Subcommands available for Azure provider."""
import os

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.common import utils


class ECConfig(base.Command):

    """Write Elasticluster configuration file with user information."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "ec-config",
            help="Write Elasticluster configuration file.")
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=constant.PATH.EC_CONFIG.format(
                provider=constant.PROVIDER.AZURE))

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        return utils.write_elasticluster_config(
            config={}, output=self.args.econfig,
            provider=constant.PROVIDER.AZURE)


class ManagementCertificate(base.Command):

    """Generate a management certificate."""

    def __init__(self, parent, parser):
        super(ManagementCertificate, self).__init__(parent, parser)
        self._ssh_path = os.path.join(os.path.expanduser("~"), ".ssh")

    @property
    def ssh_path(self):
        """Return the SSH keys path."""
        return self._ssh_path

    def _get_subject(self):
        """Return the information regarding client in subject format."""
        subject = []
        if self.args.country:
            subject.append("/C={}".format(self.args.country))
        if self.args.state:
            subject.append("/ST={}".format(self.args.state))
        if self.args.organization:
            subject.append("/O={}".format(self.args.organization))
        if self.args.cname:
            subject.append("/CN={}".format(self.args.cname))
        if self.args.email:
            subject.append("/emailAddress={}".format(self.args.email))
        return "".join(subject)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "management-cert",
            help="Generate a management certificate.")
        parser.add_argument(
            "-c", "--country", default=None,
            help="Country Name (2 letter code)")
        parser.add_argument(
            "-st", "--state", default=None,
            help="State or Province Name (full name)")
        parser.add_argument(
            "-o", "--organization", default="bcbio-nexgen",
            help="Organization Name (eg, company)")
        parser.add_argument(
            "-cn", "--cname", default=None,
            help="Common Name (e.g. server FQDN or YOUR name)")
        parser.add_argument(
            "-e", "--email", default=None,
            help="Email Address")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        if not os.path.exists(self._ssh_path):
            os.makedirs(self._ssh_path)
            utils.execute(["chmod", 700, self._ssh_path],
                          cwd=os.path.dirname(self._ssh_path))

        utils.execute(["openssl", "req", "-x509", "-nodes",
                       "-days", "365",
                       "-newkey", "rsa:2048",
                       "-keyout", "managementCert.pem",
                       "-out", "managementCert.pem",
                       "-subj", self._get_subject()],
                      cwd=self._ssh_path)
        utils.execute(["chmod", 600, "managementCert.pem"],
                      cwd=self._ssh_path)

        utils.execute(["openssl", "x509", "-outform", "der",
                       "-in", "managementCert.pem",
                       "-out", "managementCert.cer"],
                      cwd=self._ssh_path)
        utils.execute(["chmod", 600, "managementCert.cer"],
                      cwd=self._ssh_path)


class PrivateKey(base.Command):

    """Create a private key file that matches your management
    certificate.
    """

    def __init__(self, parent, parser):
        super(PrivateKey, self).__init__(parent, parser)
        self._ssh_path = os.path.join(os.path.expanduser("~"), ".ssh")

    @property
    def ssh_path(self):
        """Return the SSH keys path."""
        return self._ssh_path

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "pkey",
            help="Create a private key file that matches the management cert.")
        parser.add_argument(
            "--cert", default="managementCert.pem",
            help=("The management certificate name. "
                  "[default: managementCert.pem]"))

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        utils.execute(["openssl", "rsa",
                       "-in", self.args.cert,
                       "-out", "managementCert.key"],
                      cwd=self._ssh_path)
        utils.execute(["chmod", 600, "managementCert.key"],
                      cwd=self._ssh_path)


class PrepareEnvironment(base.Container):

    sub_commands = [
        (ManagementCertificate, "actions"),
        (PrivateKey, "actions"),
        (ECConfig, "actions"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "prepare",
            help=("Utilities to help with environment configuration."))
        actions = parser.add_subparsers(title="[devel commands]")
        self._register_parser("actions", actions)
