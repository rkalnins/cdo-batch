# CDO batching tool

A tool for creating, manipulating, and reading NetCDF files with CDO.

Users can create, split, copy, groups (nodes) of files on which to apply
CDO operators. 

## Batching Operations

Operators are created and added to a directed acyclic graph that connects
inputs and outputs via different operator paths.

The vectorizing, permuting, forking, applying, and configuring operations
are used to manipulate and prepare the network for execution.

### Vectorizing

Provided by the function `operator.vectorize_on`, vectorizing expands
a series of 1 or more operations in 2 dimensions on some variable in the
operator.

The following is based on the example at [adammwilson/SpatialAnalysisTutorials](https://github.com/adammwilson/SpatialAnalysisTutorials/blob/master/climate/code/ClimateMetrics.md#cdo-is-great-at-piping-operators).

We first set up the input nodes to select the current and future year ranges:
```python
dataset = Node("root", "tests/data/climate")
rcm = Node("rcm3", "RCM3")
dataset.add_child(rcm)
rcm.find_files() # finds gfdl_RCM3_Future.nc and gfdl_RCM3_Current.nc
```

We then get the the years of each input:
```python
cdo = Cdo()
showyr = Operator("showyear")
showyr.configure(rcm)
r = showyr.run(cdo)

future_years = r[0].result[0].split() # ['2037', '2039', ..., '2070']
current_years = r[1].result[0].split() # ['1067'. '1968', ..., '2000']
```


For example, the a root operator `mergetime` and the three operators `eca_cfd`, `selyear`, and `selname` are
created along with an output node `merge_output`:

```python
merge_output = Node("merge_out", path="output/merge")
mergetime = Operator("mergetime", out_node=merge_output, options="-O")
eca_cfd = Operator("eca_cfd")
selyear = Operator("selyear")
selname = Operator("selname", "tmin")
```

We need to apply `eca_cfd` to each year's `tmin` for both current and future
years.

1. Repeat the chain `-eca_cfd -selyear,y, -selname,tmin` for each year `y`.
2. Split the generated chain on current and future years

Ultimately, we have the chains where `fn` and `cn` are the nth future and current
years

```
merge┬->eca_cfd->selyear[f0]->selname,tmin[]->....->eac_cfd->selyear[fn]->selname,tmin[]
     └->eca_cfd->selyear[c0]->selname,tmin[]->....->eac_cfd->selyear[cn]->selname,tmin[]
```

is generated with

```python
merge.vectorize_on(
  [eca_cfd, selyear, selname],
  dimensions=[2, len(future_years)],
  op_idx=1,
  params=[future_years, current_years]
)
```

Since we're using a size greater than one in the first dimensions
on our vectorization, we end up with a forked
graph. In our particular case, each fork correspond to future and current years,
respectively.

`op_idx` specifies the index of which operator in the series provided (e.g. `[eca_cfd,
selyear, selname]` in our case) to apply the variables to.

The chain hasn't properly applied the input file to each `-selname,tmin`
operator so we need to modify each `selname` operation in each fork using a
different variable.

We need the future fork to use `gfdl_RCM3_Future.nc` and the current fork to use
`gfdl_RCM3_Current.nc`.

Thus, we can use `fork_apply` to apply a variable to each fork independently:
```python
merge.fork_apply("selname", var_name="op_input_file", vars=rcm)
```

In this case, since we're applying the input files, we can simply provide the 
node and `cdobatch` will use the files in that node as the variables.

We now have the graph
```
merge┬->eca_cfd->selyear[f0]->selname,tmin[gfdl_RCM3_Future.nc]->....->eac_cfd->selyear[fn]->selname,tmin[gfdl_RCM3_Future.nc]
     └->eca_cfd->selyear[c0]->selname,tmin[gfdl_RCM3_Current.nc]->....->eac_cfd->selyear[cn]->selname,tmin[gfdl_RCM3_Current.nc]
```

The default strategy when generating and executing cdo operations from the graph
is to apply each path (e.g. from `merge` the last `selname` via the top and
bottom fork) to all input files. In our case, we want the top and bottom forks
to apply to the first and second input files in the `rcm` node. Therefore, we
need to use the `file_fork_mapped` routing mode. Since the top level operator
(`merge`) is using the piped output as input, we need to configure merge to not
use the input files from `rcm`.

```python
merge.configure(rcm, route_mode="file_fork_mapped", use_input_file=False)
```

Finally, we can execute the cdo commands:
```python
merge.run(cdo)
```

If we only needed the futures data, our code would look something like this:
```python
# only use futures file
rcm.files = ["gfdl_RCM3_Future.nc"]

# only size 1 in 1st dimension
merge.vectorize_on(
  [eca_cfd, selyear, selname],
  dimensions=[1, len(future_years)],
  op_idx=1,
  params=future_years
)

# no fork created so don't need to use fork_apply
merge.vector_apply("selname", var_name="op_input_file", vars=rcm)

# no fork so routing mode doesn't matter
merge.configure(rcm, use_input_file=False)
merge.run(cdo)
```

### Permuting (TODO)

Note: not yet implemented

Permuting on an operation allows for running the identical chains of operators except
for one operator.

### Forking (TODO)

Note: not yet implemented.

Behavior is similar ot vectorizing on 2d so TBD on if this is necessary.

### Configure

Configuring an operation finds all paths through the operator graph and 
creates all required cdo commands. Nothing is executing until `operator.run` is
called.


## Example Usage

Create a node and apply an operation.
```python
from cdo import *
from cdobatch.node import Node
from cdobatch.operator import Operator

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
from cdobatch.operator import Operator

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
from cdobatch.operator import Operator

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
from cdobatch.operator import Operator

with Record(load_path="CMIP6_data/tas/MODELS_filtered/ssp585") as r:
    # split tree recursively twice using filesystem paths
    input_nodes = r.get_node("root").path_split(["seasonal_avg/Projections",
                                                 "seasonal_avg/Historical",
                                                 "year_avg/Projections",
                                                 "year_avg/Historical"])

    output_node = Node("outputs", "CMIP6_data/tas/MODELS_filtered/ssp585/iceshelves")
    r.add_node(output_node)

    for n in input_nodes:
        for shelf in shelves:

            # create output path
            path_parts = n.get_root_path().split("/")[-1:]
            path = f"iceshelves/{shelf['name']}/{path_parts[1]}/{path_parts[0]}"
            
            # create output node
            name = f"{shelf['name']}_{path_parts[1]}_{path_parts[0]}"
            shelf_output_node = Node(name, path)
            output_node.add_child(shelf_output_node)

            # each command maps to an output node
            # built-in `input_basename` is name of input file
            op = Operator(
                    "sellonlatbox",
                    shelf["coords"],
                    out_node=shelf_output_node,
                    out_name_format="{input_basename}" + f".{shelf['name']}.nc"
            )

            op.configure(n)
            op.run()

```


