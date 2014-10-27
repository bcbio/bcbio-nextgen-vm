"""Create IAM users and instance profiles for running bcbio on AWS.
"""

import boto
import toolz as tz

def bootstrap(args):
    conn = boto.connect_iam()
    _bcbio_iam_user(conn, args)
    _bcbio_s3_instance_profile(conn)

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
    else:
        print("User %s already exists, no new credentials" % name)

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

def _bcbio_s3_instance_profile(conn):
    """Create an IAM instance profile with temporary S3 access to be applied to launched machines.
    """
    name = "bcbio_full_s3_access"
    try:
        ip = conn.get_instance_profile(name)
    except boto.exception.BotoServerError:
        ip = conn.create_instance_profile(name)
    try:
        conn.get_role(name)
    except boto.exception.BotoServerError:
        conn.create_role(name)
        conn.put_role_policy(name, name, S3_POLICY)
    if not tz.get_in(["get_instance_profile_response", "get_instance_profile_result", "instance_profile", "roles"],
                     ip):
        conn.add_role_to_instance_profile(name, name)
    print("Instance profile: %s" % name)
