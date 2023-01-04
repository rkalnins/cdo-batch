from cdo import Cdo
from cdobatch.node import Node
from cdobatch.operator import Operator

files = ["M1.nc", "M2.nc"]
root = Node("root", "CMIP6_data/tas/MODELS_filtered/ssp585", files=files)

out_root = Node("out_root", "output")
out_seasonal = Node("seasonal", "seasonal")
out_yearly = Node("yearly", "yearly")

out_root.add_child(out_seasonal)
out_root.add_child(out_yearly)


cdo = Cdo()

# yearly
yearmean = Operator("yearmean")

names = ["ross", "tmp"]
coords = ["1.0,1.0,1.0,1.0", "4.0,12,1,0"]

sellonlat_root = Operator()
sellonlat_seas_op = Operator("sellonlatbox", out_node=out_root)
sellonlat_seas_op.vectorize(coords, dir="vertical", type="params", root=sellonlat_root)

sellonlat_root.extend_leaves([Operator("seasmean"), Operator("select", "season=DJF")])

outut_formats = []

for n in names:
    outut_formats.append("{input_basename}_" + n + ".nc")

sellonlat_root.fork_apply(
    "sellonlatbox", var_name="out_name_format", vars=outut_formats
)

sellonlat_root.configure(root)
res = sellonlat_root.run(cdo, dry_run=True)

for r in res:
    print(r)
