# -*- Encoding: utf8 -*-

from .stores import DataStore
from .resource import is_local, read_json
from .objects import data_object
import os
import json
from collections import OrderedDict

from urllib.parse import urljoin, urlparse

class DataPackageResource(object):
    def __init__(self, package, resource):
        self.package = package

        path = resource.get("path")
        if path:
            self.url = urljoin(package.url, path)
        elif "url" in resource:
            self.url = resource["url"]
        elif "data" in resource:
            raise NotImplementedError("Embedded datapackage resource data "
                                      "are not supported")
        else:
            raise MetadataError("No path or url specified in a package "
                                "resource.")

        self.name = resource.get("name")
        self.title = resource.get("title")
        self.url = urljoin(package.url, path)
        self.fields = resource["schema"]["fields"]

        self.type = resource.get("type", os.path.splitext(self.url)[1][1:])

    def dataobject(self):
        return data_object("csv_source", self.url)

class DataPackage(object):
    def __init__(self, url):
        # TODO: currently only local paths are supported
        if is_local(url) and not url.endswith("/"):
            url = url + "/"

        self.url = url

        infopath = urljoin(url, "datapackage.json")
        metadata = read_json(infopath)
        with open(infopath) as f:
            try:
                metadata = json.load(f)
            except Exception as e:
                raise Exception("Unable to read %s: %s"
                                % (infopath, str(e)))

        self.name = metadata.get("name")
        self._resources = OrderedDict()
        for i, res in enumerate(metadata["resources"]):
            resource = DataPackageResource(self, res)
            if not resource.name:
                resource.name = "resource%d" % i

            if resource.name in self._resources:
                raise Exception("Duplicate resource '%s' in data package '%s'"
                                % (resource.name, self.name))
            self._resources[resource.name] = resource

    def __getitem__(self, item):
        return self._resources[item]

    def resource(self, name):
        return self._resources[name]

    @property
    def resources(self):
        return list(self._resources.values())

    @property
    def resource_count(self):
        return len(self._resources)

class DataPackageCollectionStore(DataStore):
    __extension_name__ = "datapackages"

    def __init__(self, url):
        """Creates a store that contains collection of data packages. The
        datasets are referred as `store_name.dataset_name`."""

        if not is_local(url):
            raise NotImplementedError("Remote package collections are "
                                      "not supported yet")

        self.packages = []

        paths = []
        for path in os.listdir(url):
            path = os.path.join(url, path)
            if not os.path.isdir(path):
                continue

            if not os.path.exists(os.path.join(path, "datapackage.json")):
                continue

            package = DataPackage(path)
            self.packages.append(package)

        self.datasets = OrderedDict()
        self._index_datasets()

    def _index_datasets(self):
        """Collect dataset names."""

        self.resources = OrderedDict()
        for package in self.packages:
            if package.resource_count == 1:
                name = package.name
                if name in self.resources:
                    raise MetadataError("Two single-resource packages with the"
                                        " same name '%s'" % name)
                self.resources[name] = package.resources[0]
            else:
                for resource in package.resources:
                    name = "%s.%s" % (package.name, resource.name)
                    if name in self.resources:
                        raise MetadataError("Duplicate datapackage resource %s in "
                                            "%s." % (resource.name, package.name) )
                    self.resources[name] = resource

    def object_names(self):
        return self.resources.keys()

    def get_object(self, name, **args):
        return self.resources[name].dataobject()

