"""Create IAM users and instance profiles for running bcbio on AWS.

More information regarding IAM can be found on the following link:
    http://goo.gl/L4Iie0
"""
import os

import boto
import toolz

from bcbiovm.common import utils

LOG = utils.get_logger(__name__)
IAM_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "*",
      "Resource": "*"
    }
  ]
}
"""
S3_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }
      ]
}
"""


class IAMOps(object):

    """Create IAM users and instance profiles for running bcbio on AWS."""

    def __init__(self):
        self._connection = boto.connect_iam()

    def _create_user(self, create, recreate, credentials, **kwargs):
        """Create or recreate an IAM user."""
        name = kwargs.get("name", "bcbio")
        access_key_name = kwargs.get("access_key_name", "full_admin_access")
        if not create:
            return
        try:
            self._connection.get_user(name)
            if recreate:
                keys = self._connection.get_all_access_keys(name)
                access_key = toolz.get_in(["list_access_keys_response",
                                           "list_access_keys_result",
                                           "access_key_metadata"], keys, [])
                for key in access_key:
                    self._connection.delete_access_key(key["access_key_id"],
                                                       name)
                credentials.update(self._connection.create_access_key(name))
        except boto.exception.BotoServerError:
            self._connection.create_user(name)
            self._connection.put_user_policy(name, access_key_name,
                                             IAM_POLICY)
            credentials.update(self._connection.create_access_key(name))

    @staticmethod
    def create_keypair(config, keyname="bcbio"):
        """Create a bcbio keypair and import to ec2.

        Gives us access to keypair locally and at AWS.
        """

        keypair_dir = (os.path.dirname(config)
                       .replace("elasticluster", "aws_keypairs"))
        private_key = os.path.join(os.path.join(keypair_dir, "bcbio"))
        public_key = private_key + ".pub"
        new_key = not os.path.exists(private_key)

        if not os.path.exists(keypair_dir):
            os.makedirs(keypair_dir)

        if new_key:
            utils.execute(
                ["ssh-keygen", "-t", "rsa", "-N", "",
                 "-f", private_key, "-C", "bcbio_aws_keypair"],
                check_exit_code=0)

        ec2 = boto.connect_ec2()
        key = ec2.get_key_pair(keyname)
        if key and new_key:
            ec2.delete_key_pair(keyname)
            key = None

        if not key:
            with open(public_key) as in_handle:
                ec2.import_key_pair(keyname, in_handle.read())

        return {"user_key_name": keyname,
                "user_key_private": private_key,
                "user_key_public": public_key}

    def bcbio_iam_user(self, create, recreate):
        """Create a bcbio IAM user account with full access permissions."""
        name = "bcbio"
        credentials = {}
        self._create_user(create, recreate, credentials, name=name)
        if credentials:
            credentials = toolz.get_in(["create_access_key_response",
                                        "create_access_key_result",
                                        "access_key"], credentials)
            return {
                "ec2_access_key": credentials.get("access_key_id"),
                "ec2_secret_key": credentials.get("secret_access_key")
            }
        else:
            LOG.info("User %(user)s already exists, no new credentials",
                     {"user": name})
            LOG.info("Edit the configuration file to add existing user's "
                     "access and secret keys")
            return {}

    def bcbio_s3_instance_profile(self, create, name="bcbio_full_s3_access"):
        """Create an IAM instance profile with temporary S3 access to be
        applied to launched machines.
        """
        if not create:
            return {"instance_profile": ""}

        try:
            ip_address = self._connection.get_instance_profile(name)
        except boto.exception.BotoServerError:
            ip_address = self._connection.create_instance_profile(name)

        try:
            self._connection.get_role(name)
        except boto.exception.BotoServerError:
            self._connection.create_role(name)
            self._connection.put_role_policy(name, name, S3_POLICY)

        roles = toolz.get_in(["get_instance_profile_response",
                              "get_instance_profile_result",
                              "instance_profile", "roles"], ip_address)
        if not roles:
            self._connection.add_role_to_instance_profile(name, name)
        return {"instance_profile": name}
