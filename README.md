# CDO batching tool

A tool for creating, manipulating, and reading NetCDF files with CDO.

Users can create, split, copy, groups (nodes) of files on which to apply
CDO operators. 

## Example Usage

Create a node and apply an operation.
```python
from cdo import *
from cdobatch.node import Node
from cdobatch.op import Operator

cdo = Cdo()

files = ["a.nc", "b.nc"]
input_node = Node("root", "path/to/data")

op = Operator("selname", "invTime")
op.configure(input_node)

# returns list of paths to temporary files generated
output_files = op.run(cdo)
```

Discover files and apply an operation, write output to output files.
```python
from cdo import *
from cdobatch.node import Node
from cdobatch.op import Operator

input_node = Node("root", "path/to/data")
output_node = Node("output", "path/to/output")

# find all data files
input_node.find_files()

op = Operator(
        "sellonlatbox",
        "0,1,2,3",
        out_node=output_node,
        out_name_format="{input_basename}.{customField}.nc)
        out_name_vars={"customField": "foo"},
)

op.configure(input_node)
results = op.run(cdo)
```


Apply the same operator to a collection of files that's already been indexed. Move output to different directory.

```python
from cdobatch.record import Record
from cdobatch.node import Node
from cdobatch.op import Operator

# ensure any changes get written to dataset.json
with Record(load_path="dataset.json") as r:

    output = Node("output", "path/to/output/relative/to/dataset/root")
    op = Operator("sellonlatbox" "100,280,-50,50", out_node=output)

    input_node = r.get_node("dataset_full")
    op.configure(input_node)
    op.run()
```

Apply an operator with variable parameters to a collection of files from a dataset and remap output to a different file structure and change the base file name.

```python
from cdobatch.record import Record
from cdobatch.node import Node
from cdobatch.op import Operator

with Record(load_path="CMIP6_data/tas/MODELS_filtered/ssp585") as r:
    # split tree recursively twice using filesystem paths
    input_nodes = r.get_node("root").path_split(["seasonal_avg/Projections",
                                                 "seasonal_avg/Historical"
                                                 "year_avg/Projections",
                                                 "year_avg/Historical"])

    ops = {}
    output_node = Node("outputs", "CMIP6_data/tas/MODELS_filtered/ssp585/iceshelves")
    r.add_node(output_node)

    for n in input_nodes:
        for shelf in shelves:

            # create output path
            path_parts = n.get_root_path().split("/")[-1:]
            path = f"iceshelves/{shelf["name"]}/{path_parts[1]}/{path_parts[0]}"
            
            # create output node
            name = f"{shelf["name"]}_{path_parts[1]}_{path_parts[0]}"
            shelf_output_node = Node(name, path)
            output_node.add_child(shelf_output_node)

            # each command maps to an output node
            # built-in `input_basename` is name of input file
            op = Operator(
                    "sellonlatbox",
                    shelf["coords"],
                    out_node=shelf_output_node,
                    out_name_format="{input_basename}" + f".{shelf["name"]}.nc"
            )

            op.configure(n)
            op.run()

```


