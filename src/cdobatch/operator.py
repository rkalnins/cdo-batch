from __future__ import annotations

from contextlib import redirect_stdout, redirect_stderr

import copy
import io
import os
import numpy as np
from typing import Any

from .node import Node
from cdo import *


class CdoResult:
    result: Any
    error: Any
    errout: str
    stdout: str

    errmsg: str

    op: Any

    def __init__(self, op, result, error, errout, stdout):
        self.op = op
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

    def __init__(
        self,
        name="",
        param="",
        out_node=None,
        out_name_vars=None,
        out_name_format="",
        options="",
    ):
        """
        Create an operator

        :param name str: operator name
        :param out_node Node: node to where outputs will be written
        :param out_name_vars: variables to format out_name_format with
        :param out_name_format: format of output file names
        :param options str: CDO options to pass thhrough to all cdo commands run
        """

        self.op_name = name
        self.op_param = param
        self.op_out_node = out_node
        self.out_name_format = out_name_format
        self.op_options = options
        self.op_input_file = ""

        self.visited = False

        self.op_next = []
        self.op_prev = []
        self.cdo_cmds = []

        if out_name_vars is None:
            self.op_out_name_vars = {}
        else:
            self.op_out_name_vars = out_name_vars

        if out_name_format == "":
            self.out_name_format = "{input_basename}.nc"
        else:
            self.out_name_format = out_name_format

    def vectorize(self, vars, dir="horizontal", **kwargs):
        """
        Vectorize this node, requires that parent node exists
        """

        ops = [copy.deepcopy(self) for _ in range(len(vars) - 1)]
        ops.insert(0, self)

        if kwargs["type"] == "ops":
            self.modify_op(ops, "op_name", vars)
        elif kwargs["type"] == "params":
            self.modify_op(ops, "op_param", vars)
        elif kwargs["type"] == "inputs":
            self.modify_op(ops, "op_input_file", vars)
        else:
            print("Unknown var")

        if dir == "horizontal":
            self.extend(ops[1:])
        elif dir == "vertical":
            for o in ops:
                kwargs["root"].append(o)
        else:
            print("Unknown direction")

    def modify_op(self, ops, prop_name, vars):

        if not isinstance(ops[0], list):
            for i in range(len(ops)):
                setattr(ops[i], prop_name, vars[i])
        else:
            for i in range(len(ops)):
                for j in range(len(ops[0])):
                    setattr(ops[i][j], prop_name, vars[i][j])

    def vectorize_on(self, series: list[Operator], **kwargs):
        """
        Expand operators given in series up to 2 dimensions while modifying
        the operator in the series at op_idx using the variable provided in
        either ops, params, or inputs arguments

        Provide only one of ops, params, or inputs.

        :param series list[Operator]: the operator chain to replicate or extend
        :param dimensions list[int]: 2-d dimensions, 1st dimension describes
        number of forks, second dimension describes chain length
        :param op_idx int: the index (from 0) of the operator in the given series
        to modify with each varible in ops, params, or inputs
        :param ops list: list of operator names to replace the exisitng names
        :param params list: list of params to replace in chosen operators
        :param inputs list: list of input files to replace in chosen operators
        """
        op_idx = kwargs["op_idx"]
        if not isinstance(kwargs["op_idx"], list):
            op_idx = [op_idx]

        is_parameterized = len(op_idx) > 1
        param_count = len(op_idx)

        types = kwargs["type"]
        if not isinstance(kwargs["type"], list):
            types = [types]

        if is_parameterized:
            vars = kwargs["vars"]
        else:
            if isinstance(kwargs["vars"][0], list):
                vars = [np.array(kwargs["vars"]).flatten().tolist()]
            else:
                vars = [kwargs["vars"]]

        # repeat the given series on each var modifying op
        # append the craeted series to this op
        dimensions = [1]
        if "dimensions" in kwargs:
            dimensions = kwargs["dimensions"]

        ops_to_modify = [[] for _ in range(param_count)]

        for _ in range(dimensions[0]):
            row = []
            for _ in range(dimensions[1]):
                # need deepcopies so each operator in graph is unique
                l = [copy.deepcopy(o) for o in series]

                # choose each operator from the new set
                # keep the same operators together in the same sublists
                for i in range(param_count):
                    ops_to_modify[i].append(l[op_idx[i]])

                    # create the row
                row.extend(l)

            self.extend(row)

        # choose correct member variable to update
        for i in range(len(types)):
            t = types[i]
            if t == "ops":
                self.modify_op(ops_to_modify[i], "op_name", vars[i])
            elif t == "params":
                self.modify_op(ops_to_modify[i], "op_param", vars[i])
            elif t == "inputs":
                self.modify_op(ops_to_modify[i], "op_input_file", vars[i])
            elif t == "out_format":
                self.modify_op(ops_to_modify[i], "out_name_format", vars[i])
            else:
                print("Unknown var")

    def permute_on(self, op: Operator, **kwargs):
        # TODO
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
        # TODO
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

    def vector_apply(self, filter_op: str, var_name: str, var: Any, type="all"):
        """
        Change a variable of name var_name for each operator of type filter_op to var

        :param filter_op str: name of operator
        :param var_name str: name of variable to modify
        :param var Any: variable to insert
        """

        if type == "all":
            if self.op_name == filter_op:
                setattr(self, var_name, var)
                print(self, var_name, var)

            # apply single variable to all
            for o in self.op_next:
                o.vector_apply(filter_op, var_name, var, type)
        elif type == "horizontal":
            if self.op_name == filter_op:
                setattr(self, var_name, var)

            if len(self.op_next) == 0:
                return

            self.op_next[0].vector_apply(filter_op, var_name, var, type)

        elif type == "vertical":
            for o in self.op_next:
                if o.op_name == filter_op:
                    setattr(o, var_name, var)

        else:
            print("Unknown type")

    def fork_apply(self, filter_op: str, var_name: str, vars: list[Any], type="all"):
        """
        Apply each variable in vars to each fork, respectively using vector_apply

        :param filter_op str: name of operator
        :param var_name str: name of variable to modify
        :param var list[Any]: variables to use for each fork. Can also be a node
        if modifying the input files. In this case, the input files need to be
        modified and fork_apply will use the node's list of files as the
        variable.
        """

        # get file list if node is provided for vars
        apply_inputs = vars
        if isinstance(vars, Node):
            apply_inputs = [os.path.join(vars.get_root_path(), f) for f in vars.files]

        if len(apply_inputs) != len(self.op_next):
            print("Fork apply failed: not enough vars")
            return

        # apply different var for each fork
        for i in range(len(self.op_next)):
            self.op_next[i].vector_apply(filter_op, var_name, apply_inputs[i], type)

    def set_prev_op(self, p):
        self.op_prev.append(p)

    def get_leaves(self):
        def recurse_find(op, ends):
            if len(op.op_next) == 0:
                ends.add(op)
                return ends

            for o in op.op_next:
                ends.update(recurse_find(o, ends))

            return ends

        ends = set()
        return list(recurse_find(self, ends))

    def append(self, op: Operator):
        """
        Add operator to this node, creates a fork in the operator graph.

        :param op Operator: the operator to add
        """
        op.set_prev_op(self)
        self.op_next.append(op)

    def extend_leaves(self, ops: list[Operator]):
        leaves = self.get_leaves()

        for l in leaves:
            l.extend(copy.deepcopy(ops))

    def extend(self, ops: list[Operator]):
        """
        Add an operator chain to this node, does not create a fork in the operator graph.
        The last operator in ops will be the first operator applied to the input
        file.

        :param ops list[Operator]: the operators to chain to this node.
        """

        self.append(ops[0])
        ops[0].set_prev_op(self)
        for i in range(len(ops) - 1):
            # chain each operator to the next
            ops[i].append(ops[i + 1])
            ops[i + 1].set_prev_op(ops[i])

    def get_output_name(self, input_path: str) -> str:
        """
        Get the output name for the provided input file at input_path.
        Applies any custom variable fields

        :param input_path str: path of the file to generate an output for
        :return: the full path and name of the output file
        :rtype: str
        """
        if self.op_out_node is None:
            return ""

        # get full output path from the root node
        output_path = self.op_out_node.get_root_path()

        # file name of input
        input_name = os.path.splitext(os.path.basename(input_path))[0]
        return os.path.join(
            output_path,
            # apply custom input format
            # TODO: provide more customizability features
            self.out_name_format.format(
                input_basename=input_name, **self.op_out_name_vars
            ),
        )

    def get_commands(
        self, op_paths: list[list[Operator]], working_path: list[Operator]
    ) -> list[list[Operator]]:
        """
        Use depth first search to create all paths through the operator network

        :param op_paths list[Operator]: all paths created so far
        :param working_path list[Operator]: the current path being explored
        :return: A list of all operator paths
        :rtype: list[Operator]
        """

        self.visited = True

        # start with this operator
        working_path.append(self)

        if len(self.op_next) == 0:
            # base case, no operators are chained to this node
            op_paths.append(working_path[:])
        else:
            # DFS on each fork
            for n in self.op_next:
                if not n.visited:
                    op_paths = n.get_commands(op_paths, working_path[:])

        working_path = working_path[:-1]
        self.visited = False

        return op_paths

    def print_graph(self):
        print(
            self,
            self.op_name,
            self.op_param,
            self.op_input_file,
            self.op_next,
            self.out_name_format,
            self.op_out_node,
        )

        for o in self.op_next:
            o.print_graph()

    def create_command(
        self, input_path: str, p: list[Operator], use_input_file: bool
    ) -> dict:
        """
        Create a command from an operator chain

        :param input_path str: input file path
        :param p list[Operator]: operator path for this command
        :param use_input_file bool: the root operator (self) will add an input
        file if True, often False if the root operator is only taking the
        chain's output as input

        :return: a dictionary of all cdo command components
        :rtype: dict
        """
        cmd = {}

        cmd["func_name"] = self.op_name
        cmd["param"] = self.op_param

        cmd["output"] = self.get_output_name(input_path)
        cmd["options"] = self.op_options
        cmd["input"] = ""

        # skip first item in chain
        for o in p[1:]:
            if o.op_name == "":
                continue

            needs_space = True
            # build piped input
            cmd["input"] += f"-{o.op_name}"

            if o.op_param != "":
                needs_space = False
                cmd["input"] += f",{o.op_param} "

            if o.op_input_file != "":
                needs_space = False
                cmd["input"] += f"{o.op_input_file} "

            # fix missing space when no param and no input files are provided
            if needs_space:
                cmd["input"] += " "

        if use_input_file:
            cmd["input"] += input_path

        # ensure no whitespace around the command
        cmd["input"] = cmd["input"].strip()

        return cmd

    def configure(self, node: Node, route_mode="default", use_input_file=True):
        """
        Find all operator paths in the operator graph starting from this
        operator. Creates a set of cdo commands to run.

        :param node Node: the node of input files on which to operator on
        :param route_mode str: the routing mode ("default" or
        "file_fork_mapped") to use when applying input files to operator paths
        :param use_input_file bool: the root operator will add an input file if
        True, often False if the root operator is only taking the chain's output
        as input
        """
        # depth first search to build all cdo commands

        root_ops = []

        self.cdo_cmds = []

        if self.op_name == "":
            for o in self.op_next:
                o.op_out_node = self.op_out_node
                root_ops.append(o)
        else:
            root_ops.append(self)

        for o in root_ops:
            for i in range(len(node.files)):
                input_file = node.files[i]
                input_path = os.path.join(node.get_root_path(), input_file)
                op_path = []
                working_set = []

                # find all paths through operator graph
                op_path = o.get_commands(op_path, working_set)

                if route_mode == "default":
                    # translate op path, node, input file into cdo arguments
                    for p in op_path:
                        # create a command for each path for each input file
                        cmd = o.create_command(input_path, p, use_input_file)
                        self.cdo_cmds.append(cmd)
                elif route_mode == "file_fork_mapped":
                    # map each input file to a different path
                    cmd = o.create_command(input_path, op_path[i], use_input_file)
                    self.cdo_cmds.append(cmd)

    def make_cdo_cmd_str(self, c: dict) -> str:
        """
        Convert cdo operation dictionary to a debug string (or for dry run)

        :param c dict: cdo operation dictionary
        :return: the cdo command line
        :rtype: str
        """
        cmd_str = "cdo"
        if c["options"] != "":
            cmd_str += f' {c["options"]}'

        if c["func_name"] != "":
            cmd_str += f' -{c["func_name"]}'

        if c["param"] != "":
            cmd_str += f',{c["param"]}'

        cmd_str += f' {c["input"]} {c["output"]}'
        cmd_str = cmd_str.strip()

        return cmd_str

    def run_dry(self) -> list[str]:
        """
        Perform a dry run of all operations. Creates command lines.

        :return: cdo command lines to be executed
        :rtype: list[str]
        """
        results = []

        for c in self.cdo_cmds:
            cmd_str = self.make_cdo_cmd_str(c)
            results.append(cmd_str)

        return results

    def preprocess(self):
        """
        Creates all output directories necessary
        """
        for c in self.cdo_cmds:
            if c["output"] != "":
                os.makedirs(os.path.dirname(c["output"]), exist_ok=True)

    def run_real(self, cdo: Cdo) -> list[CdoResult]:
        """
        Apply cdo to input files and write output

        :param cdo Cdo: cdo instance to use
        :return: list of results from each run of cdo
        :rtype: list[CdoResult]
        """
        results = []
        for c in self.cdo_cmds:
            # get function corresponding to the operator
            cdo_func = getattr(cdo, c["func_name"])

            # capture stdout and stderr
            err = io.StringIO()
            out = io.StringIO()

            # catch all CDO related exceptions
            try:
                # capture stdout and stderr
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
                results.append(CdoResult(self, None, e, err, out))
            else:
                results.append(CdoResult(self, r, None, err, out))

            # update the output node with any new output files
            if self.op_out_node is not None:
                self.op_out_node.find_files()

        return results

    def run(
        self, cdo: Cdo, create_outputs_only=False, dry_run=False
    ) -> (list[CdoResult] | list[str] | None):
        """
        Run cdo, either dry run or actually operate. Can also only create output
        directories first.

        :param cdo Cdo: cdo instance to use
        :param create_outputs_only bool: only create output directories, do not
        run
        :param dry_run bool: perform dry run if True, doesn't write or create
        any output artifacts
        """
        if dry_run:
            return self.run_dry()

        self.preprocess()
        if create_outputs_only:
            # don't do anything else
            return

        return self.run_real(cdo)
