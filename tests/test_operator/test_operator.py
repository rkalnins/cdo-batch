import os

from netCDF4 import Dataset
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
    assert op.get_output_name(files[0]) == ""


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


def test_operator_run_no_output_dry():
    cdo = Cdo()
    files = ["a1.nc", "a2.nc", "a3.nc"]

    in_n = Node("root", "")
    in_n.add_child(Node("a_files", "a/in", files=files))

    op = Operator("info")

    op.setup(in_n.find_node("a_files"))
    cmds = op.run(cdo, dry_run=True)
    assert cmds == [
        "cdo -info a/in/a1.nc",
        "cdo -info a/in/a2.nc",
        "cdo -info a/in/a3.nc",
    ]


def test_operator_info():
    cdo = Cdo()
    files = ["a1.nc", "a2.nc", "a3.nc"]

    in_n = Node("root", "tests/data")
    in_n.add_child(Node("a_files", "a", files=files))

    op = Operator("info")

    op.setup(in_n.find_node("a_files"))
    info = op.run(cdo)

    assert len(info) == len(files)


def test_operator_info():
    cdo = Cdo()
    files = ["a1.nc", "a2.nc", "a3.nc"]

    in_n = Node("root", "tests/data")
    in_n.add_child(Node("a_files", "a", files=files))

    op = Operator("info")

    op.setup(in_n.find_node("a_files"))
    info = op.run(cdo)

    assert len(info) == len(files)


def test_operator_showname():
    cdo = Cdo()

    files = ["a1.nc", "a2.nc", "a3.nc"]
    in_n = Node("root", "tests/data")
    in_n.add_child(Node("a_files", "a", files=files))

    op = Operator("showname")

    op.setup(in_n.find_node("a_files"))
    names = op.run(cdo)

    assert names[0] == [
        "lastRecord invTime prevRecord inventory isOverflow firstInBin lastInBin numericWMOid latitude longitude elevation riverStage riverFlow precip5min precip5minQCA precip5minQCR precip5minQCD precip5minICA precip5minICR precip1hr precip1hrQCA precip1hrQCR precip1hrQCD precip1hrICA precip1hrICR precip3hr precip3hrQCA precip3hrQCR precip3hrQCD precip3hrICA precip3hrICR precip6hr precip6hrQCA precip6hrQCR precip6hrQCD precip6hrICA precip6hrICR precip12hr precip12hrQCA precip12hrQCR precip12hrQCD precip12hrICA precip12hrICR precip24hr precip24hrQCA precip24hrQCR precip24hrQCD precip24hrICA precip24hrICR precipAccum precipAccumQCA precipAccumQCR precipAccumQCD precipAccumICA precipAccumICR"
    ]

    assert names[2] == ["tos"]


def test_operator_selname_output_to_temp():
    cdo = Cdo()

    files = ["a1.nc", "a2.nc"]
    in_n = Node("root", "tests/data")
    in_n.add_child(Node("a_files", "a", files=files))

    op = Operator("selname", "invTime")

    op.setup(in_n.find_node("a_files"))
    output_files = op.run(cdo)

    assert len(output_files) == 2

    for o in output_files:
        d = Dataset(o, "r", format="NETCDF4")
        # TODO: test something here, if cdo finishes, odds are good it succeeded
        d.close()
