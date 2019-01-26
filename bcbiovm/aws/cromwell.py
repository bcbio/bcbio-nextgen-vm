"""Automate creation of AWS resources for Cromwell.

Sets up the AWS batch environment needed to run Cromwell using CloudFormation
templates:

https://docs.opendata.aws/genomics-workflows

CloudFormation suggestions:
 - Avoid nodejs4 vs 8 errors when creating AMI
 - Check for S3 bucket when creating AWS batch environment
 - Delete temporary VPCs for genomics-api when finishing stack, other resources?
"""
import boto3
import requests

TEMPLATES = {
    "AMI": "https://s3.amazonaws.com/aws-genomics-workflows/templates/create-genomics-ami/create-custom-ami-new-vpc.yaml",
    "batch": "https://s3.amazonaws.com/aws-genomics-workflows/templates/aws-genomics-root.template.yaml"}

def setup_cmd(awsparser):
    """Command line interface for setting up Cromwell on AWS.
    """

    parser = awsparser.add_parser("cromwell",
                                  help="Setup AWS batch environment for running Cromwell")
    parser.add_argument("--keypair", help="Existing keypair to use for accessing AWS instances.", default="")
    parser.add_argument("--bucket", help="S3 bucket to store Cromwell logs and execution files", required=True)
    parser.add_argument("--zone", help=("AWS availability zones to create resources in "
                                        "(default: a, b in current region)"),
                        nargs="+", default=["%s%s" % (boto3.Session().region_name, z) for z in ["a", "b"]])
    parser.set_defaults(func=create_resources)

def create_resources(args):
    """Create Cromwell AWS Batch input resources.
    """
    ami_out = _create_ami(args)
    print("AMI: %s" % ami_out["AMI"])
    queue_out = _create_batch_queue(ami_out["AMI"], args)
    print("Region: %s" % boto3.Session().region_name)
    print("S3 bucket: s3://%s" % queue_out["GenomicsEnvS3Bucket"])
    print("Job Queue (Spot instances): %s" % queue_out["GenomicsEnvDefaultJobQueueArn"])
    print("High priority Job Queue: %s" % queue_out["GenomicsEnvHighPriorityJobQueueArn"])

def _create_batch_queue(ami_id, args):
    """Create a AWS Batch queue ready to run Cromwell jobs.
    """
    stack_name = "GenomicsEnv-Full"
    cf = boto3.client("cloudformation")
    stacks = [x for x in cf.list_stacks()["StackSummaries"]
              if x["StackName"] == stack_name and x["StackStatus"] == "CREATE_COMPLETE"]
    if stacks:
        return _get_stack_outputs(stack_name)
    else:
        _remove_stack(stack_name)
        _check_for_batch_resource_conflicts(args)
        params = {"S3BucketName": args.bucket,
                  "KeyPairName": args.keypair,
                  "AvailabilityZone1": args.zone[0],
                  "AvailabilityZone2": args.zone[1],
                  "CustomAmiId": ami_id}
        stack_input = {"StackName": stack_name,
                       "TemplateBody": _get_batch_template(),
                       "Parameters": [{"ParameterKey": k, "ParameterValue": v} for k, v in params.items()],
                       "Capabilities": ['CAPABILITY_IAM']}
        print("Creating AWS batch queue: %s" % stack_input["StackName"])
        cf.create_stack(**stack_input)
        waiter = cf.get_waiter("stack_create_complete")
        waiter.wait(StackName=stack_input["StackName"])
        return _get_stack_outputs(stack_name)

def _check_for_batch_resource_conflicts(args):
    """Avoids issues with CloudFormation template not checking existing resources.

    GenomicsEnvS3Bucket	CREATE_FAILED	bcbio-batch-cromwell-test already exists
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(args.bucket)
    if bucket.creation_date:
        bucket.delete()

def _remove_stack(stack_name):
    """Remove a stack if it exists and was not created correctly.
    """
    cf = boto3.client("cloudformation")
    stacks = [x for x in cf.list_stacks()["StackSummaries"] if x["StackName"] == stack_name]
    if stacks:
        assert stacks[0]["StackStatus"] != "CREATE_COMPLETE", (stack_name, stacks[0])
        cf.delete_stack(StackName=stack_name)
        cf.get_waiter("stack_delete_complete").wait(StackName=stack_name)

def _create_ami(args):
    """Create a custom AMI for running Cromwell tasks.
    """
    stack_name = "GenomicsWorkflow-AMI"
    cf = boto3.client("cloudformation")
    stacks = [x for x in cf.list_stacks()["StackSummaries"]
              if x["StackName"] == stack_name and x["StackStatus"] == "CREATE_COMPLETE"]
    if stacks:
        return _get_stack_outputs(stack_name)
    else:
        _remove_stack(stack_name)
        params = {"AMIType": "cromwell",
                  "ScratchMountPoint": "/cromwell_root"}
        if args.keypair:
            params["KeyName"] = args.keypair
        stack_input = {"StackName": stack_name,
                       "TemplateBody": _get_ami_template(),
                       "Parameters": [{"ParameterKey": k, "ParameterValue": v} for k, v in params.items()],
                       "Capabilities": ['CAPABILITY_IAM']}
        print("Creating custom genomics-AMI: %s" % stack_input["StackName"])
        cf.create_stack(**stack_input)
        waiter = cf.get_waiter("stack_create_complete")
        waiter.wait(StackName=stack_input["StackName"])
        return _get_stack_outputs(stack_name)

def _get_stack_outputs(stack_name):
    cfr = boto3.resource("cloudformation")
    stack = cfr.Stack(stack_name)
    assert stack.stack_status == "CREATE_COMPLETE"
    return {x["OutputKey"]: x["OutputValue"] for x in stack.outputs}

def _get_ami_template():
    template = requests.get(TEMPLATES["AMI"]).text
    # Fix Lambda Creation issue: The runtime parameter of nodejs4.3 is no
    # 	longer supported for creating or updating AWS Lambda functions. We
    # 	recommend you use the new runtime (nodejs8.10) while creating or
    # 	updating functions.
    template = template.replace("nodejs4.3", "nodejs8.10")
    return template

def _get_batch_template():
    template = requests.get(TEMPLATES["batch"]).text
    return template
