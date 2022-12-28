from cdo import *

from cdobatch.node import Node
from cdobatch.operator import Operator, OperatorRunConfiguration, CdoResult


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

    sy_op.setup(rcm)
    r = sy_op.run(cdo)

    future_years = r[0].result[0].split()
    current_years = r[1].result[0].split()

    assert current_years == [str(y) for y in range(1967, 2001)]
    assert future_years == [str(y) for y in range(2037, 2071)]

    output = Node("min_temps", "tests/output")
    merge = Operator(
        "mergetime", output_node=output, output_format="{input_basename}_CFD.nc"
    )

    for y in current_years:
        sel_tmin = Operator("selname", "tmin")
        sel_tmin.setup(rcm)
        sel_y = Operator("selyear", y, use_chained_input=True)
        eca_cfd = Operator("eca_cfd", use_chained_input=True)

        # eca_cfd.append_chain([sel_y, sel_tmin])
        merge.append_chain([eca_cfd, sel_y, sel_tmin])

    merge.setup(rcm)

    merge.run(cdo)
