from __future__ import annotations

from contextlib import redirect_stdout, redirect_stderr

import copy
import io
import os
from typing import Any

from .node import Node
from cdo import *


class CdoResult:
    result: Any
    error: Any
    errout: str
    stdout: str

    errmsg: str

    def __init__(self, result, error, errout, stdout):
        self.result = result
        self.error = error

        # warning: all cdo output goes to stdout, not to redirect_stderr
        self.errout = errout.getvalue()
        self.stdout = stdout.getvalue()

        if error is not None:
            self.errmsg = [l for l in self.stdout.splitlines() if l != ""][-1]
        else:
            self.errmsg = ""


class Operator:
    op_next: list[Operator]
    op_prev: list[Operator]

    op_name: str
    op_param: str
    op_out_node: Node
    op_out_name_vars: dict
    out_name_format: str
    op_options: str
    op_input_file: str

    visited: bool

    cdo_cmds: list[dict]

    def __init__(self):
        pass

    def __init__(
        self,
        name,
        param="",
        out_node=None,
        out_name_vars=None,
        out_name_format="",
        options="",
    ):
        self.op_name = name
        self.op_param = param
        self.op_out_node = out_node
        self.out_name_format = out_name_format
        self.op_options = options
        self.op_input_file = ""

        self.visited = False

        self.op_next = []

        self.cdo_cmds = []

        if out_name_vars is None:
            self.op_out_name_vars = {}
        else:
            self.op_out_name_vars = out_name_vars

        if out_name_format == "":
            self.out_name_format = "{input_basename}.nc"
        else:
            self.out_name_format = out_name_format

    def vectorize_on(self, series: list[Operatpr], **kwargs):
        # repeat the given series on each var modifying op
        # append the craeted series to this op
        dimensions = [1]
        if "dimensions" in kwargs:
            dimensions = kwargs["dimensions"]

        ops_to_modify = []

        for _ in range(dimensions[0]):
            row = []
            variable_ops = []
            for _ in range(dimensions[1]):
                l = [copy.deepcopy(o) for o in series]
                variable_ops.append(l[kwargs["op_idx"]])
                row.extend(l)

            self.extend(row)
            ops_to_modify.append(variable_ops)

        def modify(ops, prop_name, vars, dim0):
            if dim0 == 1:
                vars2d = [vars]
            else:
                vars2d = vars

            for i in range(len(vars2d)):
                for j in range(len(vars2d[0])):
                    setattr(ops[i][j], prop_name, vars2d[i][j])

        if "ops" in kwargs:
            modify(ops_to_modify, "op_name", kwargs["ops"], dimensions[0])
        elif "params" in kwargs:
            modify(ops_to_modify, "op_param", kwargs["params"], dimensions[0])
        elif "inputs" in kwargs:
            modify(ops_to_modify, "op_input_file", kwargs["inputs"], dimensions[0])
        else:
            print("Unknown var")

    def permute_on(self, op: Operator, **kwargs):
        # adds permutations to operator chain
        # var count should equal number of inputs
        # each permutation corresponds to 1 cdo command
        if "ops" in kwargs:
            # var is operator
            pass
        elif "params" in kwargs:
            # var is param
            pass
        elif "inputs" in kwargs:
            # var is input file name
            pass
        else:
            print("Unknown var")

    def fork_on(self, op: Operator, **kwargs):
        # splits inputs and applies different variables to
        # different paths
        # each fork is 1 additional CDO command
        if "ops" in kwargs:
            # var is operator
            pass
        elif "params" in kwargs:
            # var is param
            pass
        elif "inputs" in kwargs:
            # var is input file name
            pass
        else:
            print("Unknown var")

    def vector_apply(self, filter_op, var_name, var):

        # apply single variable to all
        for o in self.op_next:
            o.vector_apply(filter_op, var_name, var)

        if self.op_name == filter_op:
            setattr(self, var_name, var)

    def fork_apply(self, filter_op, var_name, vars):
        apply_inputs = vars
        if isinstance(vars, Node):
            apply_inputs = [os.path.join(vars.get_root_path(), f) for f in vars.files]

        if len(apply_inputs) != len(self.op_next):
            print("Fork apply failed: not enough vars")
            return


        for i in range(len(self.op_next)):
            self.op_next[i].vector_apply(filter_op, var_name, apply_inputs[i])

    def append(self, op: Operator):
        self.op_next.append(op)

    def extend(self, ops: list[Operator]):
        self.append(ops[0])
        for i in range(len(ops) - 1):
            ops[i].append(ops[i + 1])

    def get_output_name(self, input_path: str) -> str:
        if self.op_out_node is None:
            return ""

        output_path = self.op_out_node.get_root_path()
        input_name = os.path.splitext(os.path.basename(input_path))[0]
        return os.path.join(
            output_path,
            self.out_name_format.format(
                input_basename=input_name, **self.op_out_name_vars
            ),
        )

    def get_input_name(self, input_file):
        return input_file

    def get_commands(self, op_paths, working_path):
        self.visited = True
        working_path.append(self)

        if len(self.op_next) == 0:
            op_paths.append(working_path[:])
        else:
            for n in self.op_next:
                if not n.visited:
                    op_paths = n.get_commands(op_paths, working_path[:])

        working_path = working_path[:-1]
        self.visited = False

        return op_paths

    def print_graph(self):
        print(self, self.op_name, self.op_param, self.op_input_file, self.op_next)

        for o in self.op_next:
            o.print_graph()

    def create_command(self, input_path, p, use_input_file):
        cmd = {}
        cmd["func_name"] = self.op_name
        cmd["param"] = self.op_param
        cmd["output"] = self.get_output_name(input_path)
        cmd["options"] = self.op_options
        cmd["input"] = ""

        # skip first item in chain
        for o in p[1:]:
            needs_space = True
            # build piped input
            cmd["input"] += f"-{o.op_name}"

            if o.op_param != "":
                needs_space = False
                cmd["input"] += f",{o.op_param} "

            if o.op_input_file != "":
                needs_space = False
                cmd["input"] += f"{o.op_input_file} "

            if needs_space:
                cmd["input"] += " "

        if use_input_file:
            cmd["input"] += self.get_input_name(input_path)

        cmd["input"] = cmd["input"].strip()

        return cmd
        

    def configure(self, node: Node, route_mode="default", use_input_file=True):
        # depth first search to build all cdo commands
        self.cdo_cmds = []
        
        for i in range(len(node.files)):
            input_file = node.files[i]
            input_path = os.path.join(node.get_root_path(), input_file)
            op_path = []
            working_set = []
            op_path = self.get_commands(op_path, working_set)
            
            if route_mode == "default":
                # translate op path, node, input file into cdo arguments
                for p in op_path:
                    cmd = self.create_command(input_path, p, use_input_file)
                    self.cdo_cmds.append(cmd)
            elif route_mode == "file_fork_mapped":
                cmd = self.create_command(input_path, op_path[i], use_input_file)
                self.cdo_cmds.append(cmd)

    def make_cdo_cmd_str(self, c):
        cmd_str = "cdo"
        if c["options"] != "":
            cmd_str += f' {c["options"]}'

        cmd_str += f' -{c["func_name"]}'

        if c["param"] != "":
            cmd_str += f',{c["param"]}'

        cmd_str += f' {c["input"]} {c["output"]}'
        cmd_str = cmd_str.strip()

        return cmd_str

    def run_dry(self):
        results = []

        for c in self.cdo_cmds:
            cmd_str = self.make_cdo_cmd_str(c)
            results.append(cmd_str)

        return results

    def preprocess(self):
        # create all output files
        pass

    def run_real(self, cdo):
        results = []
        for c in self.cdo_cmds:
            cdo_func = getattr(cdo, c["func_name"])

            err = io.StringIO()
            out = io.StringIO()

            try:
                with redirect_stderr(err), redirect_stdout(out):
                    args = []
                    kwargs = {}

                    if c["param"] != "":
                        args.append(c["param"])

                    if c["input"] != "":
                        kwargs["input"] = c["input"]

                    if c["output"] != "":
                        kwargs["output"] = c["output"]

                    if c["options"] != "":
                        kwargs["options"] = c["options"]

                    r = cdo_func(*args, **kwargs)

            except CDOException as e:
                results.append(CdoResult(None, e, err, out))
            else:
                results.append(CdoResult(r, None, err, out))
            
            if self.op_out_node is not None:
                self.op_out_node.find_files()

        return results

    def run(self, cdo, create_outputs_only=False, dry_run=False):
        if dry_run:
            return self.run_dry()

        if create_outputs_only:
            self.preprocess()
            return

        return self.run_real(cdo)
