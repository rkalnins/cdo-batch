from cdo import Cdo
from cdobatch.node import Node
from cdobatch.operator import Operator

root = Node("root", "CMIP6_data/tas/MODELS_filtered/ssp585")
root.find_files()

# split tree recursively twice using filesystem paths
input_nodes = root.path_split(
    [
        "seasonal_avg/Projections",
        "seasonal_avg/Historical",
        "year_avg/Projections",
        "year_avg/Historical",
    ]
)

output_node = Node("outputs", "iceshelves")
root.add_child(output_node)

shelves = []  # read shelf named

cdo = Cdo()

for n in input_nodes:
    for shelf in shelves:

        # create output path, skip file name
        path_parts = n.get_root_path().split("/")[-1:]

        # rearrange file path to match desired output
        # flip Historical and year_avg/seasonal_avg path parts
        path = f"iceshelves/{shelf['name']}/{path_parts[1]}/{path_parts[0]}"

        # create output node with desired output path
        name = f"{shelf['name']}_{path_parts[1]}_{path_parts[0]}"
        shelf_output_node = Node(name, path)
        output_node.add_child(shelf_output_node)

        # each command maps to an output node
        # built-in `input_basename` is name of input file
        op = Operator(
            "sellonlatbox",
            shelf["coords"],
            out_node=shelf_output_node,
            out_name_format="{input_basename}" + f".{shelf['name']}.nc",
        )

        op.configure(n)
        op.run(cdo)
