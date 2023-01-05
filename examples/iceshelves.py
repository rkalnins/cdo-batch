from cdo import Cdo
from cdobatch.node import Node
from cdobatch.operator import Operator

files = ["M1.nc", "M2.nc"]
root = Node("root", "CMIP6_data/tas/MODELS_filtered/ssp585", files=files)

out_root = Node("out_root", "iceshelves")
out_seasonal = Node("seasonal", "seasonal")
out_yearly = Node("yearly", "yearly")

out_root.add_child(out_seasonal)
out_root.add_child(out_yearly)


cdo = Cdo()

names = ["ross", "ronne"]
coords = ["1.0,1.0,1.0,1.0", "4.0,12,1,0"]

yearmean = Operator("yearmean")
seasmean = Operator("seasmean")
select = Operator("select", "season=DJF")
sellonlat = Operator("sellonlatbox")

year_root_op = Operator(out_node=out_yearly)
seas_root_op = Operator(out_node=out_seasonal)


output_formats = []

for n in names:
    output_formats.append(n + "_{input_basename}.nc")


print("-----yearly")
year_root_op.vectorize_on(
    [yearmean, sellonlat],
    dimensions=[len(names), 1],
    op_idx=[0, 1],
    type=["out_format", "params"],
    vars=[output_formats, coords],
)
print("----seasonal")
seas_root_op.vectorize_on(
    [seasmean, select, sellonlat],
    dimensions=[len(names), 1],
    op_idx=[0, 2],
    type=["out_format", "params"],
    vars=[output_formats, coords],
)


year_root_op.configure(root)
seas_root_op.configure(root)
res_yearly = year_root_op.run(cdo, dry_run=True)
res_seas = seas_root_op.run(cdo, dry_run=True)

for r in res_yearly:
    print(r)

for r in res_seas:
    print(r)
