"""Common objects used across the bcbio-vm project."""


import collections
import json
import prettytable

from bcbiovm.common import exception as exc


Flavor = collections.namedtuple("Flavor", ["cpus", "memory"])


class ReportMixin(object):

    def raw(self):
        """Dictionary representation of the container."""
        return self._data

    def _sanitize(self, data):
        if isinstance(data, dict):
            new_data = {}
            for key, value in data.items():
                new_data[key] = self._sanitize(value)
            return new_data
        if isinstance(data, SANITIZABLE):
            return data.raw()
        return data

    def json(self, indent=True):
        """Representation of the entity in JSON format."""
        # Sanitize the dictionary to make sure it contains only dicts.
        new_data = self._sanitize(self._data)
        # Prettify and return the whole dict as json text.
        indent = 4 if indent else None
        try:
            return json.dumps(new_data, indent=indent)
        except ValueError:
            return None


class Report(ReportMixin):

    """Simple information container."""

    def __init__(self):
        self._data = {}

    def add_section(self, name, title=None, description=None, fields=None):
        """Add a new section to the current report.

        :raises: bcbiovm.exception.BCBioException
        """
        if name in self._data:
            raise exc.BCBioException("The section %(section)r already "
                                     "exists", section=name)
        self._data[name] = Container(name, title, description, fields)
        return self._data[name]

    def __str__(self):
        """String representation for current report."""
        value = "<Report: {}>".format(self._data.keys())
        return value

    def __repr__(self):
        """Machine-readable report representation"""
        value = self.json(indent=False) or "Unknown format"
        return "<Report: {}>".format(value)

    def text(self):
        chunks = []
        for name, container in self._data.items():
            chunks.append("{}\n{}\n{}".format(
                name,
                "=" * len(name),
                container.text()
            ))
        return "\n\n".join(chunks)


class Container(ReportMixin):

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
        value = self.json(indent=False) or "Unknown format"
        return "<Container: {}>".format(value)

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
        """Add a new item into container.

        :raises: bcbiovm.exception.BCBioException
        """
        if isinstance(item, dict):
            row = []
            for field in self._data["meta"]["fields"]:
                field_name = field.get("name")
                if field_name in item:
                    row.append(item[field_name])
                elif "default" in field:
                    row.append(field["default"])
                else:
                    raise exc.BCBioException("The field %(field)r is mising.",
                                             field=field_name)
            self._data["content"].append(row)

        elif isinstance(item, (list, tuple)):
            if len(item) == len(self._data["meta"]["fields"]):
                self._data["content"].append(item)
            else:
                raise exc.BCBioException("Invalid number of fields.")

        elif len(self._data["meta"]["fields"]) == 1:
            self._data["content"].append([item])

        else:
            raise exc.BCBioException("Unknown item type %(item_type)r.",
                                     item_type=type(item))

    def text(self):
        """Return a pretty text table from the available data."""
        columns = [field["name"] for field in self._data["meta"]["fields"]]
        table = prettytable.PrettyTable(columns)
        for row in self._data["content"]:
            table.add_row(row)
        return str(table)


class ShippingConfig(object):

    """Store configuration for shipping to one storage service.

    Example:
    ::
        s3_config = ShippingConfig()
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

    def __init__(self, data=None):
        self._data = data or {}
        self._alias = {}

    def __getattr__(self, name):
        """Hook for getting attribute from local storage"""
        data = self.__dict__.get("_data")
        container = self._get_container(name)
        if container in data:
            return data[container]

        raise AttributeError("'ShippingConfig' object has no attribute '{}'"
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

    def add_alias(self, container, alias):
        """Add a new alias for the received container.

        :raises: bcbiovm.exception.BCBioException
        """
        if not alias:
            return

        if not self._check_alias(alias):
            raise exc.BCBioException("Invalid alias name provided: "
                                     "%(alias)r", alias=alias)
        self._alias[alias] = container

    def add_container(self, name, alias=None):
        """Create a new container for the shipping configuration.

        :raises: bcbiovm.exception.BCBioException
        """
        self.add_alias(name, alias)
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

    @classmethod
    def from_dict(cls, data, alias_list=None):
        """Create a new ShippingConfig from an existing dictionary."""
        shipping_config = cls(data)
        for container, alias in alias_list or ():
            shipping_config.add_alias(container, alias)
        return shipping_config


SANITIZABLE = (Report, Container)
