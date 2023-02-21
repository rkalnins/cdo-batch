from cdo import Cdo
import os
from cdobatch.node import Node
from cdobatch.operator import Operator


def log_errors(results):

    failed_shelves = set()

    for s in results:
        if isinstance(s, str):
            print(s)

        elif s.error is not None:
            print(s.error, s.stdout)

            # collect failed names
            for name in shelves["names"]:
                if name in s.stdout or "(returncode:1)" in s.stdout:
                    failed_shelves.add(name)

    if len(failed_shelves) > 0:
        print("failed shelves")
        print(list(failed_shelves))


def iceshelf_iterate(shelves):
    root = Node("root", "tas/MODELS_filtered/ssp585/month_avg")
    root.find_files()

    out_root = Node("means", "means")
    out_shelves = Node("shelves", "shelves")
    out_seasonal = Node("mean_seasonal", "seasonal")
    out_yearly = Node("mean_yearly", "yearly")

    out_root.add_child(out_seasonal)
    out_root.add_child(out_yearly)

    cdo = Cdo()

    sel_root = Operator(out_node=out_shelves)
    sellonlat = Operator("sellonlatbox")

    sellonlat.vectorize(shelves["coords"], type="params", dir="vertical", root=sel_root)

    shelf_out_names = []
    for n in shelves["names"]:
        shelf_out_names.append(n + "_{input_basename}.nc")

    sel_root.fork_apply("sellonlatbox", "out_name_format", shelf_out_names)

    sel_root.configure(root)
    out = sel_root.run(cdo)

    log_errors(out)

    yearmean = Operator("yearmean", out_node=out_root)
    seasmean = Operator("seasmean", out_node=out_root)
    select = Operator("select", "season=DJF")
    seasmean.extend([select])

    yearmean.configure(out_shelves)
    seasmean.configure(out_shelves)
    res_yearly = yearmean.run(cdo, dry_run=True)
    res_seas = seasmean.run(cdo, dry_run=True)

    log_errors(res_yearly)
    log_errors(res_seas)


def get_shelf_data(lines):
    shelves = {"names": [], "coords": []}

    for l in lines:
        parts = l.split(",")
        shelves["names"].append(parts[0].replace(" ", "-"))
        shelves["coords"].append(",".join(parts[1:]).strip())

    return shelves


with open("examples/shelves.csv", "r") as in_file:
    shelves = get_shelf_data(in_file.readlines())
    iceshelf_iterate(shelves)
