from __future__ import annotations
import json
import os
from .node import Node


class Record:
    path: str
    index_name: str
    root_nodes: list

    def __init__(self, path=None, index_name="dataset.json"):
        self.path = path
        self.index_name = index_name
        self.root_nodes = []

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
        # create root node with all files
        n = Node("root", self.path)
        n.find_files()
        self.root_nodes.append(n)

    def load(self):
        with open(self.path, "r") as record:
            raw = json.load(record)

            for n_raw in raw["nodes"]:
                self.root_nodes.append(Node.from_dict(n_raw))

    def dump(self):
        dump_contents = {}
        dump_contents["nodes"] = [n.to_dict() for n in self.root_nodes]

        with open(self.path, "w+") as f:
            json.dump(dump_contents, f, indent=2)
