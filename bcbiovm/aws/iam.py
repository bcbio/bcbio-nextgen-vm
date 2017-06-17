"""Create IAM users and instance profiles for running bcbio on AWS.

http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
"""
from __future__ import print_function

import datetime
import os
import shutil
import subprocess
import sys

import boto
import toolz as tz

NOIAM_MSG = """
IAM users and instance profiles not created.
Manually add the following items to your configuration file:
  ec2_access_key    AWS Access Key ID, ideally generated for an IAM user with full AWS permissions
                    http://docs.aws.amazon.com/AWSSecurityCredentials/1.0/AboutAWSCredentials.html#AccessKeys
                    http://docs.aws.amazon.com/IAM/latest/UserGuide/ManagingCredentials.html
  ec2_secret_key    AWS Secret Key ID matching the ec2_access_key
  instance_profile  Create an IAM Instance profile allowing access to S3 buckets for pushing/pulling data.
                    Use 'InstanceProfileName' from aws iam list-instance-profiles after setting up.
                    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
                    http://docs.aws.amazon.com/IAM/latest/UserGuide/role-usecase-ec2app.html
                    http://j.mp/iams3ip

The IAM user you create will need to have access permissions for:
  - EC2 and VPC -- ec2:*
  - IAM instance profiles -- iam:PassRole, iam:ListInstanceProfiles
  - CloudFormation for launching a Lutre ICEL instance -- cloudformation:*
"""
def bootstrap(args):
    conn = boto.iam.connect_to_region(args.region)
    config = create_keypair(args.econfig, region=args.region)
    config.update(_bcbio_iam_user(conn, args))
    config.update(bcbio_s3_instance_profile(conn, args))
    ec2_conn = boto.ec2.connect_to_region(args.region)
    config["ec2_region"] = ec2_conn.region.name
    config["ec2_url"] = "https://" + ec2_conn.region.endpoint
    econfig = _write_elasticluster_config(config, args.econfig)
    print("\nWrote elasticluster config file at: %s" % econfig)
    if args.nocreate:
        print(NOIAM_MSG)

def _write_elasticluster_config(config, out_file):
    """Write Elasticluster configuration file with user and security information.
    """
    orig_file = os.path.join(sys.prefix, "share", "bcbio-vm", "elasticluster", "config")
    if not os.path.exists(os.path.dirname(out_file)):
        os.makedirs(os.path.dirname(out_file))
    if os.path.exists(out_file):
        bak_file = out_file + ".bak%s" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        shutil.move(out_file, bak_file)
    with open(orig_file) as in_handle:
        with open(out_file, "w") as out_handle:
            for line in in_handle:
                if line.startswith(tuple(config.keys())):
                    name, val = line.strip().split("=")
                    out_handle.write("%s=%s\n" % (name, config[name]))
                else:
                    out_handle.write(line)
    return out_file

def create_keypair(econfig_file=None, region=None, keyname="bcbio"):
    """Create a bcbio keypair and import to ec2. Gives us access to keypair locally and at AWS.
    """
    if econfig_file:
        keypair_dir = os.path.dirname(econfig_file).replace("elasticluster", "aws_keypairs")
    else:
        keypair_dir = os.path.join(os.getcwd(), "aws_keypairs")
    if not os.path.exists(keypair_dir):
        os.makedirs(keypair_dir)
    private_key = os.path.join(os.path.join(keypair_dir, keyname))
    new_key = not os.path.exists(private_key)
    if new_key:
        cmd = ["ssh-keygen", "-t", "rsa", "-N", "", "-f", private_key, "-C", "bcbio_aws_keypair"]
        subprocess.check_call(cmd)
    public_key = private_key + ".pub"
    if region:
        ec2 = boto.ec2.connect_to_region(region)
    else:
        ec2 = boto.connect_ec2()
    key = ec2.get_key_pair(keyname)
    if key and new_key:
        print("Non matching key %s found in AWS, removing." % keyname)
        ec2.delete_key_pair(keyname)
        key = None
    if not key:
        print("Key %s not found in AWS, importing created key" % keyname)
        with open(public_key) as in_handle:
            ec2.import_key_pair(keyname, in_handle.read())
    return {"user_key_name": keyname, "user_key_private": private_key,
            "user_key_public": public_key}

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

def _bcbio_iam_user(conn, args):
    """Create a bcbio IAM user account with full access permissions.
    """
    name = "bcbio"
    access_key_name = "full_admin_access"
    if args.nocreate:
        need_creds = False
    else:
        try:
            conn.get_user(name)
            if args.recreate:
                keys = conn.get_all_access_keys(name)
                for access_key in tz.get_in(["list_access_keys_response", "list_access_keys_result",
                                             "access_key_metadata"], keys, []):
                    conn.delete_access_key(access_key["access_key_id"], name)
                need_creds = True
            else:
                need_creds = False
        except boto.exception.BotoServerError:
            conn.create_user(name)
            conn.put_user_policy(name, access_key_name, IAM_POLICY)
            need_creds = True
    if need_creds:
        creds = conn.create_access_key(name)
    else:
        creds = {}
    if creds:
        creds = tz.get_in(["create_access_key_response", "create_access_key_result", "access_key"], creds)
        print("User credentials for %s:" % name)
        for awsid in ["access_key_id", "secret_access_key"]:
            print(" %s: %s" % (awsid, creds.get(awsid)))
        return {"ec2_access_key": creds.get("access_key_id"),
                "ec2_secret_key": creds.get("secret_access_key")}
    else:
        print("User %s already exists, no new credentials" % name)
        print("Edit the configuration file to add existing user's access and secret keys")
        return {}

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

def bcbio_s3_instance_profile(conn, args):
    """Create an IAM instance profile with temporary S3 access to be applied to launched machines.
    """
    if hasattr(args, "nocreate") and args.nocreate:
        return {"instance_profile": ""}
    base_name = args.cluster if hasattr(args, "cluster") and args.cluster else "bcbio"
    name = "%s_full_s3_access" % (base_name)
    try:
        ip = conn.get_instance_profile(name)
    except boto.exception.BotoServerError:
        print("Instance profile %s doesn't exist, creating" % name)
        ip = conn.create_instance_profile(name)
    try:
        conn.get_role(name)
    except boto.exception.BotoServerError:
        print("Role %s doesn't exist, creating" % name)
        conn.create_role(name)
        conn.put_role_policy(name, name, S3_POLICY)
    if not tz.get_in(["get_instance_profile_response", "get_instance_profile_result", "instance_profile", "roles"],
                     ip):
        conn.add_role_to_instance_profile(name, name)
    print("Instance profile: %s" % name)
    return {"instance_profile": name}
