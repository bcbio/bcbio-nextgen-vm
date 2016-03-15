"""Integration with Arvados Keep for file assessment. using the API.

Requires arvados-python-sdk.
"""
import os
import pprint

import toolz as tz
import yaml

# ## General functionality

def _get_api_client(config=None):
    if not config: config = {}
    if "token" in config and "host" in config:
        os.environ["ARVADOS_API_HOST"] = config["host"]
        os.environ["ARVADOS_API_TOKEN"] = config["token"]
    assert os.environ.get("ARVADOS_API_HOST") and os.environ.get("ARVADOS_API_TOKEN"), \
        "Need to set ARVADOS_API_HOST and ARVADOS_API_TOKEN to retrieve files from Keep"
    import arvados
    return arvados.api("v1")

def collection_files(uuid, config=None):
    """Retrieve files in the input collection.
    """
    import arvados
    api_client = _get_api_client(config)
    cr = arvados.CollectionReader(uuid, api_client=api_client)
    cr.normalize()
    return ["%s/%s" % (x.stream_name(), x.name) for x in cr.all_files()]

def open_remote(file_ref, config=None):
    """Retrieve an open handle to a file in an Arvados Keep collection.
    """
    import arvados
    api_client = _get_api_client(config)
    coll_uuid, coll_ref = file_ref.replace("keep:", "").split("/", 1)
    cr = arvados.CollectionReader(coll_uuid, api_client=api_client)
    return cr.open(coll_ref)

# ## Fill in files from input collections

def get_files(target_files, config):
    """Retrieve files associated with the potential inputs.
    """
    out = []
    if "input" in config:
        for keep_file in collection_files(config["input"], config):
            if os.path.basename(keep_file) in target_files:
                out.append("keep:" + os.path.normpath(os.path.join(config["input"], keep_file)))
    return out

def add_remotes(items, config):
    """Add remote Keep files to data objects, finding files not present locally.
    """
    if "input" in config:
        keep_files = collection_files(config["input"], config)
        return _fill_remote(items, keep_files, config)
    else:
        return items

def _fill_remote(cur, keep_files, config):
    """Add references to remote Keep files if present and not local.
    """
    if isinstance(cur, (list, tuple)):
        return [_fill_remote(x, keep_files, config) for x in cur]
    elif isinstance(cur, dict):
        out = {}
        for k, v in cur.items():
            out[k] = _fill_remote(v, keep_files, config)
        return out
    elif isinstance(cur, basestring) and os.path.splitext(cur)[-1] and not os.path.exists(cur):
        for test_keep in keep_files:
            if test_keep.endswith(cur):
                return "keep:" + os.path.normpath(os.path.join(config["input"], test_keep))
        return cur
    else:
        return cur

# ## Retrieve files from reference collections

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    ref_collection = tz.get_in(["arvados", "reference"], config)
    if not ref_collection:
        raise ValueError("Could not find reference collection in bcbio_system YAML for arvados.")
    cfiles = collection_files(ref_collection, config["arvados"])
    ref_prefix = None
    for prefix in ["./%s", "./genomes/%s"]:
        cur_prefix = prefix % genome_build
        if any(x.startswith(cur_prefix) for x in cfiles):
            ref_prefix = cur_prefix
            break
    assert ref_prefix, "Did not find genome files for %s:\n%s" % (genome_build, pprint.pformat(cfiles))
    out = {}
    base_targets = ("/%s.fa" % genome_build, "/mainIndex")
    for dirname in ["seq", "rtg", aligner]:
        key = {"seq": "fasta"}.get(dirname, dirname)
        cur_files = [x for x in cfiles if x.startswith("%s/%s/" % (ref_prefix, dirname))]
        cur_files = ["keep:%s" % os.path.normpath(os.path.join(ref_collection, x)) for x in cur_files]
        base_files = [x for x in cur_files if x.endswith(base_targets)]
        if len(base_files) > 0:
            assert len(base_files) == 1, base_files
            base_file = base_files[0]
            del cur_files[cur_files.index(base_file)]
            out[key] = {"base": base_file, "indexes": cur_files}
        else:
            out[key] = {"indexes": cur_files}
    return out

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    aconfig = tz.get_in(["config", "arvados"], data)
    resources_file = "%s-resources.yaml" % (os.path.splitext(fasta_ref)[0])
    base_dir = os.path.dirname(resources_file)
    with open_remote(resources_file, aconfig) as in_handle:
        resources = yaml.safe_load(in_handle)
    cfiles = [os.path.normpath(os.path.join("keep:%s" % aconfig["reference"], x))
              for x in collection_files(aconfig["reference"], aconfig)]
    for k1, v1 in resources.items():
        if isinstance(v1, dict):
            for k2, v2 in v1.items():
                if isinstance(v2, basestring) and v2.startswith("../") and os.path.splitext(v2)[-1]:
                    test_v2 = os.path.normpath(os.path.join(base_dir, v2))
                    if test_v2 in cfiles:
                        resources[k1][k2] = test_v2
                    else:
                        del resources[k1][k2]
    data["genome_resources"] = resources
    return _add_configured_indices(base_dir, cfiles, data)

def _add_configured_indices(base_dir, cfiles, data):
    """Add additional resource indices defined in genome_resources: snpeff
    """
    snpeff_db = tz.get_in(["genome_resources", "aliases", "snpeff"], data)
    if snpeff_db:
        index_dir = os.path.normpath(os.path.join(os.path.dirname(base_dir), "snpeff", snpeff_db))
        snpeff_files = [x for x in cfiles if x.startswith(index_dir)]
        if len(snpeff_files) > 0:
            base_files = [x for x in snpeff_files if x.endswith("/snpEffectPredictor.bin")]
            assert len(base_files) == 1, base_files
            del snpeff_files[snpeff_files.index(base_files[0])]
            data["reference"]["snpeff"] = {"base": base_files[0], "indexes": snpeff_files}
    return data
