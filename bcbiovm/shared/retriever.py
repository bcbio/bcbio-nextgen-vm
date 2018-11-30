"""Shared code for retrieving resources from external integrations.
"""
import os
import yaml

import toolz as tz

from bcbio import utils

def get_resources(genome_build, fasta_ref, config, data, open_fn, list_fn, find_fn=None,
                  normalize_fn=None):
    """Add genome resources defined in configuration file to data object.
    """
    resources_file = "%s-resources.yaml" % (os.path.splitext(fasta_ref)[0])
    if find_fn:
        resources_file = find_fn(resources_file)
    base_dir = os.path.dirname(resources_file)
    with open_fn(resources_file) as in_handle:
        resources = yaml.safe_load(in_handle)
    cfiles = list_fn(os.path.dirname(base_dir))
    for k1, v1 in resources.items():
        if isinstance(v1, dict):
            for k2, v2 in v1.items():
                if isinstance(v2, basestring) and v2.startswith("../"):
                    test_v2 = _normpath_remote(os.path.join(base_dir, v2), normalize_fn=normalize_fn)
                    if find_fn and find_fn(test_v2) is not None:
                        resources[k1][k2] = find_fn(test_v2)
                    elif test_v2 in cfiles:
                        resources[k1][k2] = test_v2
                    else:
                        del resources[k1][k2]
    data["genome_resources"] = _ensure_annotations(resources, cfiles, data, normalize_fn)
    data = _add_configured_indices(base_dir, cfiles, data, normalize_fn)
    data = _add_data_versions(base_dir, cfiles, data, normalize_fn)
    data = _add_viral(base_dir, cfiles, data, normalize_fn)
    return _add_genome_context(base_dir, cfiles, data, normalize_fn)

def _add_data_versions(base_dir, cfiles, data, norm_fn=None):
    """Add versions file with data names mapped to current version.
    """
    search_name = _normpath_remote(os.path.join(os.path.dirname(base_dir), "versions.csv"),
                                   normalize_fn=norm_fn)
    version_files = [x for x in cfiles if search_name == (norm_fn(x) if norm_fn else x)]
    version_file = version_files[0] if version_files else None
    data["reference"]["versions"] = version_file
    return data

def _add_viral(base_dir, cfiles, data, norm_fn=None):
    """Add fasta and indices for viral QC.
    """
    viral_dir = _normpath_remote(os.path.join(os.path.dirname(base_dir), "viral"),
                                 normalize_fn=norm_fn)
    viral_files = [x for x in cfiles if x.startswith(viral_dir)]
    if viral_files:
        data["reference"]["viral"] = {"base": [x for x in viral_files if x.endswith(".fa")][0],
                                      "indexes": [x for x in viral_files if not x.endswith(".fa")]}
    else:
        data["reference"]["viral"] = None
    return data

def _ensure_annotations(resources, cfiles, data, normalize_fn):
    """Retrieve additional annotations for downstream processing.

    Mirrors functionality in bcbio.pipeline.run_info.ensure_annotations
    """
    transcript_gff = tz.get_in(["rnaseq", "transcripts"], resources)
    if transcript_gff:
        gene_bed = utils.splitext_plus(transcript_gff)[0] + ".bed"
        test_gene_bed = normalize_fn(gene_bed) if normalize_fn else gene_bed
        for fname in cfiles:
            test_fname = normalize_fn(fname) if normalize_fn else fname
            if test_fname == test_gene_bed:
                resources["rnaseq"]["gene_bed"] = fname
                break
    return resources

def _add_configured_indices(base_dir, cfiles, data, norm_fn=None):
    """Add additional resource indices defined in genome_resources: snpeff
    """
    snpeff_db = tz.get_in(["genome_resources", "aliases", "snpeff"], data)
    if snpeff_db:
        tarball = _normpath_remote(os.path.join(os.path.dirname(base_dir), "snpeff--%s-wf.tar.gz" % snpeff_db),
                                   normalize_fn=norm_fn)
        snpeff_files = [x for x in cfiles if tarball == (norm_fn(x) if norm_fn else x)]
        if len(snpeff_files) == 1:
            data["reference"]["snpeff"] = {snpeff_db: snpeff_files[0]}
        else:
            index_dir = _normpath_remote(os.path.join(os.path.dirname(base_dir), "snpeff", snpeff_db),
                                         normalize_fn=norm_fn)
            if not index_dir.endswith("/"):
                index_dir += "/"
            snpeff_files = [x for x in cfiles if x.startswith(index_dir)]
            if len(snpeff_files) > 0:
                base_files = [x for x in snpeff_files if x.endswith("/snpEffectPredictor.bin")]
                assert len(base_files) == 1, base_files
                del snpeff_files[snpeff_files.index(base_files[0])]
                data["reference"]["snpeff"] = {"base": base_files[0], "indexes": snpeff_files}
    return data

def _add_genome_context(base_dir, cfiles, data, norm_fn=None):
    """Add associated genome context files, if present.
    """
    index_dir = _normpath_remote(os.path.join(os.path.dirname(base_dir), "coverage", "problem_regions"),
                                 normalize_fn=norm_fn)
    context_files = [x for x in cfiles if x.startswith(index_dir) and x.endswith(".gz")]
    if len(context_files) > 0:
        data["reference"]["genome_context"] = sorted(context_files, key=os.path.basename)
    return data

def _normpath_remote(orig, normalize_fn=None):
    """Normalize a path, avoiding removing initial s3:// style keys
    """
    if normalize_fn:
        return os.path.normpath(normalize_fn(orig))
    elif orig.find("://") > 0:
        key, curpath = orig.split(":/")
        return key + ":/" + os.path.normpath(curpath)
    else:
        return os.path.normpath(orig)

def standard_genome_refs(genome_build, aligner, ref_prefix, list_fn):
    """Retrieve standard genome references: sequence, rtg and aligner.
    """
    out = {}
    base_targets = ("/%s.fa" % genome_build, "/mainIndex")
    for dirname in [x for x in ["seq", "rtg", aligner] if x]:
        key = {"seq": "fasta", "ucsc": "twobit"}.get(dirname, dirname)
        tarball_files = [x for x in list_fn(ref_prefix)
                         if os.path.basename(x).startswith(dirname) and x.endswith("-wf.tar.gz")]
        if len(tarball_files) > 0:
            assert len(tarball_files) == 1, tarball_files
            if dirname == aligner:
                out[key] = {"base": tarball_files[0], "indexes": tarball_files}
            else:
                out[key] = tarball_files[0]
        else:
            cur_files = list_fn(os.path.join(ref_prefix, dirname))
            base_files = [x for x in cur_files if x.endswith(base_targets)]
            if len(base_files) > 0:
                assert len(base_files) == 1, base_files
                base_file = base_files[0]
                del cur_files[cur_files.index(base_file)]
                out[key] = {"base": base_file, "indexes": cur_files}
            elif len(cur_files) == 1:
                out[key] = cur_files[0]
            else:
                out[key] = {"indexes": cur_files}
    return out

def find_ref_prefix(genome_build, find_fn):
    """Identify reference prefix in folders for genome build.
    """
    for prefix in ["%s", "genomes/%s"]:
        cur_prefix = prefix % genome_build
        remote_dir = find_fn(cur_prefix)
        if remote_dir:
            return remote_dir
    raise ValueError("Did not find genome files for %s" % (genome_build))

def fill_remote(cur, find_fn, is_remote_fn):
    """Add references in data dictionary to remote files if present and not local.
    """
    if isinstance(cur, (list, tuple)):
        return [fill_remote(x, find_fn, is_remote_fn) for x in cur]
    elif isinstance(cur, dict):
        out = {}
        for k, v in cur.items():
            out[k] = fill_remote(v, find_fn, is_remote_fn)
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
