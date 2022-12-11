from __future__ import annotations
import os


class Node:
    parent: Node
    children: list
    name: str
    path: str
    files: list

    def __init__(self, name, path, files=None):
        self.name = name
        self.path = path
        self.children = list()
        self.parent = None

        if files is None:
            self.files = list()
        else:
            self.files = files

    def find_node(self, name):
        if name == self.name:
            return self
        else:
            for c in self.children:
                n = c.find_node(name)
                if n is not None:
                    return n

    def get_root_path(self):

        # this is the root node
        if self.parent is None:
            return ""

        return os.path.join(self.parent.get_root_path(), self.path)

    def path_split(self, paths, node_names=None):

        # split off filesystem parts using paths into len(paths) nodes
        # one node, per path

        # node (a): "some/path"
        #   files:
        #       s/a/f0.nc
        #       s/b/f1.nc
        #       s/c/f2.nc
        #
        #
        # node (a): "some/path"
        #   files:
        #       s/c/f2.nc
        #   children:
        #       node (a): "s/a"
        #           files:
        #               f0.nc
        #       node (b): "s/b"
        #           files:
        #               f1.nc

        i = 0
        new_nodes = []
        for p in paths:
            matching_paths = list(filter(lambda f: f.startswith(p), self.files))
            self.files = list(filter(lambda f: f not in matching_paths, self.files))

            if node_names is None:
                name = self.name + p
            else:
                name = node_names[i]

            new_relative_paths = [
                os.path.relpath(mp, p) for mp in matching_paths
            ]
            n = Node(name, p, new_relative_paths)
            self.add_child(n)
            new_nodes.append(n)

            i += 1

        return new_nodes

    def add_child(self, node):
        node.parent = self
        self.children.append(node)

    @classmethod
    def from_dict(cls, d):
        n = cls(d["name"], d["path"], d["files"])

        for c_d in d["children"]:
            n.add_child(Node.from_dict(c_d))

        return n

    def to_dict(self):
        return {
            "name": self.name,
            "path": self.path,
            "files": self.files,
            "children": [c.to_dict() for c in self.children],
        }
