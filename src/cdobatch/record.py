from __future__ import annotations
import json
import os
import cdo


from .log import log
from .node import Node


class Record:
    path: str
    index_name: str
    root_nodes: list

    def __init__(self, path=None, index_name="dataset.json"):
        self.path = path
        self.index_name = index_name
        self.root_nodes = list()

    def __enter__(self):
        # attempt to open index, if given directory, index it
        if os.path.isfile(self.path):
            self.load()
        else:
            self.index()
            self.path += self.index_name

        return self

    def __exit__(self, *args):
        self.dump()

    def get_node(self, name):
        for n in self.root_nodes:
            found_n = n.find_node(name)

            if found_n is not None:
                return found_n

        return None

    def add_node(self, node):
        self.root_nodes.append(node)

    def index(self):
        all_paths = []

        # get all files
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file.endswith(".nc"):
                    all_paths.append(
                        os.path.relpath(os.path.join(root, file), self.path)
                    )

        # create root node with all files
        self.root_nodes.append(Node("root", self.path, files=all_paths))

    def load(self):
        with open(self.path, "r") as record:
            raw = json.load(record)

            for n_raw in raw["nodes"]:
                self.root_nodes.append(Node.from_dict(n_raw))

    def dump(self):
        dump_contents = dict()
        dump_contents["nodes"] = [n.to_dict() for n in self.root_nodes]

        with open(self.path, "w+") as f:
            json.dump(dump_contents, f, indent=2)
