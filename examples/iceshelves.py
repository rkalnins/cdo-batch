from cdo import Cdo
import os
from cdobatch.node import Node
from cdobatch.operator import Operator

def iceshelf_iterate(shelves, time_period):
    root = Node("root",
                os.path.join("CMIP6_data/tas/MODELS_filtered/ssp585/month_avg",
                                     time_period))
    root.find_files()
    root.files = ["M1.nc", "M2.nc"]

    out_root = Node("out_root", os.path.join("iceshelves", time_period))
    out_seasonal = Node("seasonal", "seasonal")
    out_yearly = Node("yearly", "yearly")

    out_root.add_child(out_seasonal)
    out_root.add_child(out_yearly)

    cdo = Cdo()

    yearmean = Operator("yearmean")
    seasmean = Operator("seasmean")
    select = Operator("select", "season=DJF")
    sellonlat = Operator("sellonlatbox")

    year_root_op = Operator(out_node=out_yearly)
    seas_root_op = Operator(out_node=out_seasonal)

    output_formats = []
    for n in shelves["names"]:
        output_formats.append(n + "_{input_basename}.nc")


    year_root_op.vectorize_on(
        [yearmean, sellonlat],
        dimensions=[len(shelves["names"]), 1],
        op_idx=[0, 1],
        type=["out_format", "params"],
        vars=[output_formats, shelves["coords"]],
    )
    seas_root_op.vectorize_on(
        [seasmean, select, sellonlat],
        dimensions=[len(shelves["names"]), 1],
        op_idx=[0, 2],
        type=["out_format", "params"],
        vars=[output_formats, shelves["coords"]],
    )

    year_root_op.configure(root)
    seas_root_op.configure(root)
    res_yearly = year_root_op.run(cdo, dry_run=True)
    res_seas = seas_root_op.run(cdo, dry_run=True)

    for r in res_yearly:
        print(r)

    for r in res_seas:
        print(r)
    
    # either should be good
    # shouldn't need both yearly and monthly, just looking for correctly
    # sized ice shelves
    return res_yearly

def get_shelf_data(lines):
    shelves = {
        "names": [],
        "coords": []
    }

    for l in lines:
        parts = l.split(',')
        shelves["names"].append(parts[0].replace(" ", "-"))
        shelves["coords"].append(",".join(parts[1:]))

    return shelves

with open("examples/shelves.csv", "r") as in_file:
    shelves = get_shelf_data(in_file.readlines())
    successes_hist = iceshelf_iterate(shelves, "Historical")

    for s in successes_hist:
        for s in shelves["names"]:
            print(s.stdout)



    successes_proj = iceshelf_iterate(shelves, "Projections")
    
