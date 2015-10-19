"""Storage manager contract class.

(Beginning of) the contract that every storage manager must follow,
and shared types that support that contract.
"""

import abc
import os

import yaml
import six


@six.add_metaclass(abc.ABCMeta)
class StorageManager(object):

    """The contract class for all the storage managers."""

    _ACCESS_ERROR = (
        "Cannot write to the parent directory of work directory %(cur_dir)s\n"
        "bcbio wants to store prepared uploaded files to %(final_dir)s\n"
        "We recommend structuring your project in a project specific "
        "directory structure\n"
        "with a specific work directory (mkdir -p your-project/work "
        "&& cd your-project/work)."
    )
    _JAR_RESOURCES = {
        "genomeanalysistk": "gatk",
        "mutect": "mutect"
    }

    @classmethod
    def _jar_resources(cls, list_function, sample_config):
        """Find uploaded jars for GATK and MuTect relative to input file.

        Automatically puts these into the configuration file to make them
        available for downstream processing. Searches for them in the specific
        project folder and also a global jar directory for a container.
        """
        configuration = {}
        jar_directory = os.path.join(os.path.dirname(sample_config), "jars")

        for filename in list_function(jar_directory):
            program = None
            for marker in cls._JAR_RESOURCES:
                if marker in filename.lower():
                    program = cls._JAR_RESOURCES[marker]
                    break
            else:
                continue

            resources = configuration.setdefault("resources", {})
            program = resources.setdefault(program, {})
            program["jar"] = filename

        return configuration

    @classmethod
    def _export_config(cls, list_function, config, sample_config, out_file):
        """Move a sample configuration locally."""
        if not os.access(os.pardir, os.W_OK | os.X_OK):
            raise IOError(cls._ACCESS_ERROR % {
                "final_dir": os.path.join(os.pardir, "final"),
                "cur_dir": os.getcwd()})

        config.update(cls._jar_resources(list_function, sample_config))
        with open(out_file, "w") as out_handle:
            yaml.dump(config, out_handle, default_flow_style=False,
                      allow_unicode=False)

    @abc.abstractmethod
    def exists(self, container, filename, context=None):
        """Check if the received key name exists in the bucket.

        :container: The name of the container.
        :filename:  The name of the item from the container.
        :context:   More information required by the storage manager.
        """
        pass

    @abc.abstractmethod
    def upload(self, path, filename, container, context=None):
        """Upload the received file.

        :path:      The path of the file that should be uploaded.
        :container: The name of the container.
        :filename:  The name of the item from the container.
        :context:   More information required by the storage manager.
        """
        pass

    @abc.abstractmethod
    def load_config(self, sample_config):
        """Move a sample configuration locally, providing remote upload."""
        pass
