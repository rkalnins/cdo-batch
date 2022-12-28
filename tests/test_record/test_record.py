from cdobatch.record import Record
from cdobatch.node import Node

import json


def test_record_add():
    r = Record("tests/data/dataset.json")
    r.load()

    assert len(r.root_nodes) == 1
    r.add_node(Node("tmp", "tmp/out"))
    assert len(r.root_nodes) == 2


def test_record_load():
    with Record("tests/data/dataset.json") as r:
        n = r.get_node("root")
        b = r.get_node("node_b")

        x = r.get_node("doesnotexist")
        assert x is None

        assert len(n.children) == 3
        assert b.name == "node_b"
        assert b.path == "b/samples"
        assert b.get_root_path() == "b/samples"


def test_record_index():
    with Record("tests/data", index_name="tmp.json") as r:
        assert len(r.root_nodes) == 1

        root = r.root_nodes[0]

        assert root.name == "root"
        assert root.children == []
        assert root.path == "tests/data"
        assert len(root.files) == 9
        assert root.files == [
            "a/a3.nc",
            "a/a2.nc",
            "a/a1.nc",
            "climate/RCM3/gfdl_RCM3_Future.nc",
            "climate/RCM3/gfdl_RCM3_Current.nc",
            "climate/IRI/GFDL_Future.nc",
            "climate/IRI/GFDL_Current.nc",
            "b/samples/s2.nc",
            "b/samples/s1.nc",
        ]
