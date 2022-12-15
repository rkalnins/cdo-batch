from cdo import *

from cdobatch.operator import Operator
from cdobatch.node import Node


def test_operator_chain():
    op_3 = Operator("test_op_3rd", "z")
    op_2 = Operator("test_op_2nd", "a,b,c", chain=op_3)
    op_1 = Operator("test_op_1st", "1.0,0.0,1.0,0.0", chain=op_2)

    op_chain = op_1.get_chain()

    assert op_chain == "-test_op_2nd,a,b,c -test_op_1st,1.0,0.0,1.0,0.0"


def test_operator_output_naming():
    files = ["a.nc", "b.nc"]
    in_n = Node("input", "inputs", files=files)
    out_n = Node("output", "path/to/output")
    op = Operator(
        "test_op",
        "z",
        output_node=out_n,
        output_format="{input_basename}.{a}.nc",
        opvar={"a": 4},
    )

    # get output name for first file
    out_file_name = op.get_output_name(files[0])
    assert out_file_name == "path/to/output/a.4.nc"
    out_file_name = op.get_output_name(files[1])
    assert out_file_name == "path/to/output/b.4.nc"

    op = Operator("test_op")
    assert op.get_output_name(files[0]) == None


def test_operator_run_single_op_dry():
    cdo = Cdo()
    files = ["a1.nc", "a2.nc", "a3.nc"]

    in_n = Node("root", "")
    in_n.add_child(Node("a_files", "a/in", files=files))

    out_n = Node("output", "out")

    op = Operator(
        "sellonlat", "p1,p2", output_node=out_n, output_format="{input_basename}-out.nc"
    )

    op.setup(in_n.find_node("a_files"))
    cmds = op.run(cdo, dry_run=True)

    assert cmds == [
        "cdo -sellonlat,p1,p2 a/in/a1.nc out/a1-out.nc",
        "cdo -sellonlat,p1,p2 a/in/a2.nc out/a2-out.nc",
        "cdo -sellonlat,p1,p2 a/in/a3.nc out/a3-out.nc",
    ]
