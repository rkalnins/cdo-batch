from cdobatch.node import Node


def test_create_node():
    name = "n"
    path = "/path/to/root"

    n = Node(name, path)
    assert n.name == name
    assert n.path == path
    # ensure empty files are handled correctly
    assert len(n.files) == 0

    files = [
        "test/file1.nc",
        "test/file2.nc",
        "test/file3.nc",
    ]

    n = Node(name, path, files)
    assert n.files == files


def test_path_split():
    files = [
        "test/file1.nc",
        "test/file2.nc",
        "test/file3.nc",
        "test/a/a1.nc",
        "test/a/a2.nc",
        "test/b/b1.nc",
    ]
    root = Node("root", "", files)

    split_nodes = root.path_split(["test/a", "test/b"], ["in_a", "in_b"])
    split_a = split_nodes[0]
    split_b = split_nodes[1]

    assert len(root.children) == 2

    assert root.files == [
        "test/file1.nc",
        "test/file2.nc",
        "test/file3.nc",
    ]

    assert split_a.name == "in_a"
    assert split_a.parent == root
    assert split_a.path == "test/a"
    assert split_a.files == [
        "a1.nc",
        "a2.nc",
    ]

    assert split_b.name == "in_b"
    assert split_b.parent == root
    assert split_b.path == "test/b"
    assert split_b.files == [
        "b1.nc",
    ]


def test_node_tree_create():
    n = Node("output", "path/to/output")
    a = Node("out_a", "samples/a")
    b = Node("out_b", "samples/b")
    c = Node("sub_a_out_c", "c")

    n.add_child(a)
    n.add_child(b)
    a.add_child(c)

    assert c.get_root_path() == "path/to/output/samples/a/c"
    assert b.get_root_path() == "path/to/output/samples/b"
    assert a.get_root_path() == "path/to/output/samples/a"
    assert n.get_root_path() == "path/to/output"


def test_node_serialization_tree():
    n = Node("output", "/path/to/output")
    a = Node("out_a", "samples/a")
    b = Node("out_b", "samples/b")
    c = Node("sub_a_out_c", "c")

    n.add_child(a)
    n.add_child(b)
    a.add_child(c)

    d = n.to_dict()

    assert d["children"] == [
        {
            "name": "out_a",
            "path": "samples/a",
            "files": [],
            "children": [
                {"name": "sub_a_out_c", "path": "c", "files": [], "children": []}
            ],
        },
        {"name": "out_b", "path": "samples/b", "files": [], "children": []},
    ]

    deserialized_n = Node.from_dict(d)
    assert len(deserialized_n.children) == 2


def test_node_serialization_leaf():
    name = "n"
    path = "/path/to/root"
    files = [
        "test/file1.nc",
        "test/file2.nc",
        "test/file3.nc",
    ]

    n = Node(name, path)
    d = n.to_dict()

    assert d == {"name": name, "path": path, "files": [], "children": []}

    deserialized_n = Node.from_dict(d)
    assert deserialized_n.name == name
    assert deserialized_n.path == path
    # ensure empty files are handled correctly
    assert deserialized_n.files == []

    n = Node(name, path, files)
    d = n.to_dict()

    assert d == {"name": name, "path": path, "files": files, "children": []}

    deserialized_n = Node.from_dict(d)
    assert deserialized_n.files == files
