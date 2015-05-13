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

        elif type(item) in (list, tuple):
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
