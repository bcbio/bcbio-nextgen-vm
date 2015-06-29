"""Common objects used across the bcbio-vm project."""

import json
import collections

Flavor = collections.namedtuple("Flavor", ["cpus", "memory"])


class Report(object):

    """Simple information container."""

    def __init__(self):
        self._data = {}

    def add_section(self, name, title=None, description=None, fields=None):
        """Add a new section to the current report."""
        if name in self._data:
            # TODO(alexandrucoman): Raise custom exception
            #                       The section already exists
            return
        self._data[name] = Container(name, title, description, fields)
        return self._data[name]


class Container(object):

    """Simple container."""

    def __init__(self, name, title=None, description=None, fields=None):
        """
        :param name:            the name of the container
        :param title:           the title for this container
        :param description:     short description for this container
        :param fields:          a list of dictionaris which contains
                                information for fields setup.
        """
        self._data = {
            "meta": {"name": name, "title": title,
                     "description": description, "fields": []},
            "content": []}

        for field in fields or []:
            self.add_field(**field)

    def __str__(self):
        """String representation for current container."""
        value = "<Container: {}>".format(self._data["meta"]["name"])
        return value

    def __repr__(self):
        """Machine-readable container representation"""
        value = "<Container: Unknown format.>"
        try:
            value = json.dumps(self._data)
        except ValueError:
            pass
        return value

    def add_field(self, name, title=None, **kwargs):
        """Create or update an existing field.

        :param name:    the name of the field
        :param title:   the label used for this field
        """
        field = {"name": name, "title": title}
        field.update(kwargs)
        self._data["meta"]["fields"].append(field)

    def add_items(self, items):
        """Add multiple items into container."""
        for item in items:
            self.add_item(item)

    def add_item(self, item):
        """Add a new item into container."""
        if isinstance(item, dict):
            item = []
            for field in self._data["meta"]["fields"]:
                field_name = field.get("name")
                if field_name in item:
                    item.append(item[field_name])
                elif "default" in field:
                    item.append(field["default"])
                else:
                    # TODO(alexandrucoman): Raise custom exception
                    #                       Missing field
                    return
            self._data["content"].append(item)

        elif isinstance(item, (list, tuple)):
            if len(item) == len(self._data["meta"]["fields"]):
                self._data["content"].append(item)
            else:
                # TODO(alexandrucoman): Raise custom exception
                #                       Missing field
                return
        elif len(self._data["meta"]["fields"]) == 1:
            self._data["content"].append(item)

        else:
            # TODO(alexandrucoman): Raise custom exception
            #                       Unknown item type
            return

    def dump(self):
        """Text representation of the container."""
        return json.dumps(self._data)

    def to_dict(self):
        """Dictionary representation of the container."""
        return self._data


class ShipingConfig(object):

    """Store configuration for shipping to one storage service.

    Example:
    ::
        s3_config = ShipingConfig()
        s3_config.add_container(name="buckets", alias="containers")
        s3_config.add_container(name="folders")
        s3_config.add_item(name="type", value="S3")
        s3_config.add_item("run", "run_bucket", container="containers")
        s3_config.add_item("biodata", "biodata_bucket", container="containers")
        s3_config.add_item("output", "output_folder", container="folders")

    >>> s3_config.buckets
    {'run': 'run_bucket', 'biodata': 'biodata_bucket'}
    >>> s3_config.containers
    {'run': 'run_bucket', 'biodata': 'biodata_bucket'}
    >>> s3_config.buckets["run"]
    run_bucket
    >>> s3_config.data
    {'folders': {'output': 'output_folder'},
     'buckets': {'run': 'run_bucket', 'biodata': 'biodata_bucket'},
     'type': 'S3'
    }

    """

    def __init__(self):
        self._data = {}
        self._alias = {}

    def __getattr__(self, name):
        """Hook for getting attribute from local storage"""
        data = self.__dict__.get("_data")
        container = self._get_container(name)
        if container in data:
            return data[container]

        raise AttributeError("'ShipingConfig' object has no attribute '{}'"
                             .format(name))

    @property
    def data(self):
        """Return the local storage."""
        return self._data

    def _check_alias(self, alias):
        """Check if the received alias can be used."""
        if alias in self._data:
            return False
        else:
            return True

    def _get_container(self, alias):
        """Return the container name."""
        if alias in self._data:
            return alias

        if alias in self._alias:
            return self._alias[alias]

    def add_container(self, name, alias=None):
        """Create a new container for the shiping configuration."""
        if alias:
            if not self._check_alias(alias):
                # TODO(alexandrucoman): Raise custom exception
                #                       Invalid alias provided
                return
            self._alias[alias] = name

        self._data.setdefault(name, {})

    def add_item(self, name, value, container=None):
        """Add or update the received item."""
        if not container:
            container = self._data
        else:
            container = self._get_container(container) or container
            container = self._data.setdefault(container, {})

        container[name] = value

    def get_item(self, name, container=None):
        """Return the required item."""
        if not container:
            return self._data[name]
        else:
            container = self._get_container(container)
            return self._data[container][name]

    def to_dict(self):
        """Dictionary representation of the container."""
        return self._data
