"""Retrieval of resources from AWS S3 buckets.
"""
import functools
import os

import toolz as tz
import yaml

from bcbio.distributed import objectstore

# ## S3 specific support

KEY = "s3"

def _config_folders(config):
    for folder in config["folders"]:
        if "/" in folder:
            bucket, rest = folder.split("/", 1)
        else:
            bucket = folder
            rest = ""
        yield "%s://%s@%s/%s" % (KEY, bucket, config["region"], rest)

def _find_file(config, target_file):
    for folder in _config_folders(config):
        cur = os.path.join(folder, target_file)
        remote = objectstore.list(cur)
        if remote:
            return cur

def _is_remote(path):
    return path.startswith("%s:/" % KEY)

# ## API: General functionality

def file_size(file_ref, config=None):
    """Retrieve file size in Mb.
    """
    conn = objectstore.connect(file_ref)
    remote = objectstore.parse_remote(file_ref)
    bucket = conn.get_bucket(remote.bucket)
    key = bucket.lookup(remote.key)
    return key.size / (1024.0 * 1024.0)

def clean_file(f):
    """Remove AWS @-based region specification from file.

    Tools such as Toil use us-east-1 bucket lookup, then pick region
    from boto.
    """
    approach, rest = f.split("://")
    bucket_region, key = rest.split("/", 1)
    bucket, region = bucket_region.split("@")
    return "%s://%s/%s" % (approach, bucket, key)

# ## API: Fill in files from S3 buckets

def get_files(target_files, config):
    """Retrieve files associated with the template inputs.
    """
    out = []
    for fname in target_files.keys():
        remote_fname = _find_file(config, fname)
        if remote_fname:
            out.append(remote_fname)
    return out

def add_remotes(items, config):
    """Add remote files to data, retrieving any files not present locally.
    """
    return _fill_remote(items, functools.partial(_find_file, config), _is_remote)

# ## API: Retrieve files from reference collections

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    ref_prefix = _find_ref_prefix(genome_build, functools.partial(_find_file, config[KEY]))
    return _standard_genome_refs(genome_build, aligner, ref_prefix, objectstore.list)

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    return _get_resources(genome_build, fasta_ref, tz.get_in(["config", KEY], data),
                          data, objectstore.open, objectstore.list)

# ## General supporting functions, non-S3 specific

def _get_resources(genome_build, fasta_ref, config, data, open_fn, list_fn):
    """Add genome resources defined in configuration file to data object.
    """
    resources_file = "%s-resources.yaml" % (os.path.splitext(fasta_ref)[0])
    base_dir = os.path.dirname(resources_file)
    with open_fn(resources_file) as in_handle:
        resources = yaml.safe_load(in_handle)
    cfiles = list_fn(os.path.dirname(base_dir))
    for k1, v1 in resources.items():
        if isinstance(v1, dict):
            for k2, v2 in v1.items():
                if isinstance(v2, basestring) and v2.startswith("../"):
                    test_v2 = _normpath_remote(os.path.join(base_dir, v2))
                    if test_v2 in cfiles:
                        resources[k1][k2] = test_v2
                    else:
                        del resources[k1][k2]
    data["genome_resources"] = resources
    data = _add_configured_indices(base_dir, cfiles, data)
    return _add_genome_context(base_dir, cfiles, data)

def _add_configured_indices(base_dir, cfiles, data):
    """Add additional resource indices defined in genome_resources: snpeff
    """
    snpeff_db = tz.get_in(["genome_resources", "aliases", "snpeff"], data)
    if snpeff_db:
        index_dir = _normpath_remote(os.path.join(os.path.dirname(base_dir), "snpeff", snpeff_db))
        snpeff_files = [x for x in cfiles if x.startswith(index_dir)]
        if len(snpeff_files) > 0:
            base_files = [x for x in snpeff_files if x.endswith("/snpEffectPredictor.bin")]
            assert len(base_files) == 1, base_files
            del snpeff_files[snpeff_files.index(base_files[0])]
            data["reference"]["snpeff"] = {"base": base_files[0], "indexes": snpeff_files}
    return data

def _add_genome_context(base_dir, cfiles, data):
    """Add associated genome context files, if present.
    """
    index_dir = _normpath_remote(os.path.join(os.path.dirname(base_dir), "coverage", "problem_regions"))
    context_files = [x for x in cfiles if x.startswith(index_dir) and x.endswith(".gz")]
    if len(context_files) > 0:
        data["reference"]["genome_context"] = context_files
    return data

def _normpath_remote(orig):
    """Normalize a path, avoiding removing initial s3:// style keys
    """
    if orig.find("://") > 0:
        key, curpath = orig.split(":/")
        return key + ":/" + os.path.normpath(curpath)
    else:
        return os.path.normpath(orig)

def _standard_genome_refs(genome_build, aligner, ref_prefix, list_fn):
    """Retrieve standard genome references: sequence, rtg and aligner.
    """
    out = {}
    base_targets = ("/%s.fa" % genome_build, "/mainIndex")
    for dirname in ["seq", "rtg", aligner]:
        key = {"seq": "fasta"}.get(dirname, dirname)
        cur_files = list_fn(os.path.join(ref_prefix, dirname))
        base_files = [x for x in cur_files if x.endswith(base_targets)]
        if len(base_files) > 0:
            assert len(base_files) == 1, base_files
            base_file = base_files[0]
            del cur_files[cur_files.index(base_file)]
            out[key] = {"base": base_file, "indexes": cur_files}
        else:
            out[key] = {"indexes": cur_files}
    return out

def _find_ref_prefix(genome_build, find_fn):
    """Identify reference prefix in folders for genome build.
    """
    for prefix in ["%s", "genomes/%s"]:
        cur_prefix = prefix % genome_build
        remote_dir = find_fn(cur_prefix)
        if remote_dir:
            return remote_dir
    raise ValueError("Did not find genome files for %s" % (genome_build))

def _fill_remote(cur, find_fn, is_remote_fn):
    """Add references in data dictionary to remote files if present and not local.
    """
    if isinstance(cur, (list, tuple)):
        return [_fill_remote(x, find_fn, is_remote_fn) for x in cur]
    elif isinstance(cur, dict):
        out = {}
        for k, v in cur.items():
            out[k] = _fill_remote(v, find_fn, is_remote_fn)
        return out
    elif (isinstance(cur, basestring) and os.path.splitext(cur)[-1] and not os.path.exists(cur)
          and not is_remote_fn(cur)):
        remote_cur = find_fn(cur)
        if remote_cur:
            return remote_cur
        else:
            return cur
    else:
        return cur
