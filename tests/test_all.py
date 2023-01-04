from cdo import *

from cdobatch.node import Node
from cdobatch.operator import Operator


def test_climate_ops():
    dataset = Node("root", "tests/data/climate")
    iri = Node("iri", "IRI")
    rcm = Node("rcm3", "RCM3")
    dataset.add_child(iri)
    dataset.add_child(rcm)

    dataset.find_files()

    assert dataset.files == []
    assert rcm.files == ["gfdl_RCM3_Future.nc", "gfdl_RCM3_Current.nc"]
    assert iri.files == ["GFDL_Future.nc", "GFDL_Current.nc"]

    cdo = Cdo()

    sy_op = Operator("showyear")

    sy_op.configure(rcm)
    r = sy_op.run(cdo)

    future_years = r[0].result[0].split()
    current_years = r[1].result[0].split()

    assert current_years == [str(y) for y in range(1967, 2001)]
    assert future_years == [str(y) for y in range(2037, 2071)]

    out_merge_tmp_n = Node("merge_tmp", path="tests/output/tmp")
    merge_op = Operator("mergetime", out_node=out_merge_tmp_n, options="-O")

    eca_cfd_op = Operator("eca_cfd")
    selyear_op = Operator("selyear")
    selname_op = Operator("selname", "tmin")

    # vectorize to expand operation pattern using custom parameters
    # vectorizing on multiple dim0 creates a fork at the root operator
    merge_op.vectorize_on(
        [eca_cfd_op, selyear_op, selname_op],
        dimensions=[2, len(future_years)],
        op_idx=1,
        type="params",
        vars=[future_years, current_years],
    )

    # fork apply to change a parameter for all nodes matching filter
    merge_op.fork_apply("selname", var_name="op_input_file", vars=rcm)

    #   cfd->sy[f0]->sn[0]->....->cfd->sy[fn]->sn[0] = output[0]
    #   cfd->sy[c0]->sn[1]->....->cfd->sy[cn]->sn[1] = output[1]
    merge_op.configure(rcm, route_mode="file_fork_mapped", use_input_file=False)

    # output means conain output files
    results = merge_op.run(cdo)

    # this node contains the tmp files created
    timmean_output = Node("output", "tests/output/timmean_gfdl")

    timmean_op = Operator(
        "timmean",
        out_node=timmean_output,
        out_name_format="{input_basename}_mean.nc",
        options="-O",
    )
    timmean_op.configure(out_merge_tmp_n)
    timmean_op.run(cdo)
