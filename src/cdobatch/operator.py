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


class OperatorRunConfiguration:
    cdo_operator: str
    cdo_parameters: str
    cdo_inputs: list
    cdo_outputs: list
    cdo_options: str

    def __init__(
        self,
        cdo_operator,
        cdo_parameters=None,
        cdo_inputs=None,
        cdo_outputs=None,
        cdo_options=None,
    ):
        self.cdo_operator = cdo_operator
        self.cdo_parameters = cdo_parameters
        self.cdo_inputs = cdo_inputs
        self.cdo_outputs = cdo_outputs
        self.cdo_options = cdo_options

        if self.cdo_inputs is None:
            self.cdo_inputs = []

        if self.cdo_outputs is None:
            self.cdo_outputs = []


class Operator:
    operator: str
    parameter: str
    output_node: Node
    opvar: dict
    output_format: str
    options: str

    is_setup: bool
    use_chained_input: bool

    chain: list

    run_config: OperatorRunConfiguration

    def __init__(self):
        pass

    def __init__(
        self,
        operator,
        parameter="",
        output_node=None,
        opvar=None,
        output_format="",
        chain=None,
        options="",
        use_chained_input=False,
    ):
        self.operator = operator
        self.parameter = parameter
        self.output_node = output_node

        self.is_setup = False

        self.use_chained_input = use_chained_input

        if self.parameter != "":
            self.parameter = "," + self.parameter

        if opvar is None:
            self.opvar = {}
        else:
            self.opvar = opvar

        if output_format is None:
            self.output_format = "{input_basename}.nc"
        else:
            self.output_format = output_format

        if chain is None:
            self.chain = []
        elif isinstance(chain, Operator):
            self.chain = [chain]
        else:
            self.chain = chain

        self.options = options

        self.run_config = None

    def get_chain(self, input_file="", is_root=True):
        if len(self.chain) == 0:
            return ""

        cmd = ""

        for o in self.chain:
            this_cmd = f"-{o.operator}{o.parameter} "

            if not o.use_chained_input:
                this_cmd += f"{input_file} "

            cmd = cmd + this_cmd + o.get_chain(is_root=False)

        return cmd.strip()

    def append_chain(self, chain: Operator | list):
        if isinstance(chain, Operator):
            self.chain.append(chain)
        else:
            self.chain.extend(chain)

    def get_output_name(self, file_path):
        if self.output_node is None:
            return ""

        # TODO: provide more options for renaming

        # get destination directory (no file name)
        path = self.output_node.get_root_path()

        # get file name of output
        input_name = os.path.splitext(os.path.basename(file_path))[0]

        return os.path.join(
            path, self.output_format.format(input_basename=input_name, **self.opvar)
        )

    def get_py_cdo_input(self, input_file):
        if len(self.chain) > 0:
            # operating chaining provided with inputs
            cdo_input = f"{self.get_chain(input_file=input_file)}".strip()
        else:
            # no chaining
            cdo_input = input_file.strip()

        return cdo_input

    def setup(self, node, chain_call=False):
        # don't setup again if already configured
        if chain_call and self.is_setup:
            return

        # setup all nodes in chain
        for c in self.chain:
            c.setup(node, chain_call=True)

        # convert operator vars to cdo command input strings
        self.run_config = OperatorRunConfiguration(
            f"-{self.operator}",
            cdo_parameters=self.parameter.strip(),
            cdo_options=self.options,
        )

        if self.output_node is not None:
            print(f"node {node.name} [{node.get_root_path()}] maps:")

        for f in node.files:
            input_file = os.path.join(node.get_root_path(), f)

            cdo_input = self.get_py_cdo_input(input_file)

            # prepare output file
            cdo_output = self.get_output_name(f)

            if cdo_output != "":
                print(f"\t{input_file.strip()} -> {cdo_output}")

            # prepare input file
            self.run_config.cdo_outputs.append(cdo_output)
            self.run_config.cdo_inputs.append(cdo_input)

        self.is_setup = True

    def preprocess(self):
        # TODO: does cdo create output directories?
        # TODO: create all directories required
        pass

    def run_dry(self):
        results = []

        for i in range(len(self.run_config.cdo_outputs)):
            op_name = self.run_config.cdo_operator
            op_params = self.run_config.cdo_parameters
            in_path_i = self.run_config.cdo_inputs[i]
            out_path_i = self.run_config.cdo_outputs[i]
            op_options = self.run_config.cdo_options

            results.append(
                f"cdo {op_name}{op_params} {in_path_i} {out_path_i} {op_options}".strip()
            )

        return results

    def run_real(self, cdo):
        cdo_op = getattr(cdo, self.operator)
        results = []

        count = len(self.run_config.cdo_inputs)
        assert count == len(self.run_config.cdo_outputs)

        params = [self.run_config.cdo_parameters] * count
        options = [self.run_config.cdo_options] * count

        ops = [cdo_op] * count
        cdo_args = []
        cdo_kwargs = []
        for i in range(count):
            cdo_args.append([])
            cdo_kwargs.append({})

            if self.run_config.cdo_parameters != "":
                cdo_args[i].append(self.run_config.cdo_parameters)

            if self.run_config.cdo_options != "":
                cdo_kwargs[i]["options"] = self.run_config.cdo_options

            if self.run_config.cdo_outputs[i] != "":
                cdo_kwargs[i]["output"] = self.run_config.cdo_outputs[i]

            if self.run_config.cdo_inputs[i] != "":
                cdo_kwargs[i]["input"] = self.run_config.cdo_inputs[i]

        for i in range(count):
            err = io.StringIO()
            out = io.StringIO()
            try:
                with redirect_stderr(err), redirect_stdout(out):
                    r = ops[i](*cdo_args[i], **cdo_kwargs[i])
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
