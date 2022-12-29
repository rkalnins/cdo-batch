from __future__ import annotations

from contextlib import redirect_stdout, redirect_stderr

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

    def serial_pipe_on(self, **kwargs):
        # repeat this command chain on each var
        # does not create a new cdo command
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
            op_paths.append(working_path)
        else:
            for n in self.op_next:
                if not n.visited:
                    op_paths = n.get_commands(op_paths, working_path)

        working_path = working_path[:-1]
        self.visited = False

        return op_paths

    def configure(self, node: Node):
        # depth first search to build all cdo commands
        self.cdo_cmds = []

        for input_file in node.files:
            input_path = os.path.join(node.get_root_path(), input_file)
            op_path = []
            working_set = []
            op_path = self.get_commands(op_path, working_set)

            # translate op path, node, input file into cdo arguments
            for p in op_path:
                cmd = {}
                cmd["func_name"] = self.op_name
                cmd["param"] = self.op_param
                cmd["output"] = self.get_output_name(input_path)
                cmd["options"] = self.op_options
                cmd["input"] = ""

                # skip first item in chain
                for o in p[1:]:
                    # build piped input
                    cmd["input"] += f"-{o.op_name},{o.op_param} {o.op_input_file} "

                cmd["input"] += self.get_input_name(input_path)
                cmd["input"] = cmd["input"].strip()

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

        return results


    def run(self, cdo, create_outputs_only=False, dry_run=False):
        if dry_run:
            return self.run_dry()

        if create_outputs_only:
            self.preprocess()
            return

        return self.run_real(cdo)
