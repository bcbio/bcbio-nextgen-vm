"""Retrieve resources from local directories.

Avoids need for having Galaxy location files, reading reference files directly
from standard bcbio directory structures.
"""
import functools
import glob
import os

import toolz as tz

from bcbio import bam
from bcbio.bam import fastq
from bcbiovm.shared import retriever as sret

KEY = "local"

def _find_ref_file(config, target_file):
    f = os.path.abspath(os.path.join(config["ref"], target_file))
    if os.path.exists(f):
        return f

def _find_any_file(config, target_file):
    """Find file in ref or inputs.
    """
    for d in [config["ref"]] + config.get("inputs", []):
        f = os.path.abspath(os.path.join(d, target_file))
        if os.path.exists(f):
            return f

def _list(dname):
    out = []
    for cur_dname, _, files in os.walk(os.path.abspath(dname)):
        for f in files:
            out.append(os.path.join(cur_dname, f))
    return out

## API

def get_files(target_files, config):
    out = []
    for fname_in in target_files.keys():
        if isinstance(fname_in, (list, tuple)):
            fnames = fname_in
        else:
            fnames = fname_in.split(";")
        for fname in fnames:
            if os.path.exists(fname):
                out.append(fname)
            else:
                added = False
                for dirname in config["inputs"]:
                    for f in glob.glob(os.path.join(dirname, fname) + "*"):
                        if bam.is_bam(f) or fastq.is_fastq(f):
                            if os.path.exists(f):
                                out.append(f)
                                added = True
                assert added, "Did not find files %s in directories %s" % (fname, config["inputs"])
    return out

def add_remotes(items, config):
    return sret.fill_remote(items, functools.partial(_find_any_file, config), lambda x: False)

def get_refs(genome_build, aligner, config):
    ref_prefix = sret.find_ref_prefix(genome_build, functools.partial(_find_ref_file, config[KEY]))
    return sret.standard_genome_refs(genome_build, aligner, ref_prefix, _list)

def get_resources(genome_build, fasta_ref, data):
    return sret.get_resources(genome_build, fasta_ref, tz.get_in(["config", KEY], data),
                              data, open, _list)
