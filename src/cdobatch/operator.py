from __future__ import annotations

import os
from pathos.pools import ProcessPool

from .node import Node


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

    chain: Operator

    run_config: OperatorRunConfiguration

    def __init__(
        self,
        operator,
        parameter="",
        output_node=None,
        opvar=None,
        output_format="",
        chain=None,
        options="",
    ):
        self.operator = operator
        self.parameter = parameter
        self.output_node = output_node

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

        self.chain = chain
        self.options = options

        self.run_config = None

    def get_chain(self):
        if self.chain is None:
            return ""

        # last in chain is first operator applied
        cmd = self.chain.get_chain() + f" -{self.operator}{self.parameter}"
        return cmd.strip()

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

    def setup(self, node):
        if self.options is None:
            cdo_options = ""
        else:
            cdo_options = self.options

        self.run_config = OperatorRunConfiguration(
            f"-{self.operator}",
            cdo_parameters=self.parameter.strip(),
            cdo_options=cdo_options,
        )

        if self.output_node is not None:
            print(f"node {node.name} [{node.get_root_path()}] maps:")

        for f in node.files:
            input_file = os.path.join(node.get_root_path(), f)
            if self.chain is None:
                # operating chaining provided with inputs
                cdo_input = f"{self.get_chain()} {input_file}".strip()
            else:
                # no chaining
                cdo_input = input_file.strip()

            # prepare output file
            cdo_output = self.get_output_name(f)

            if cdo_output != "":
                print(f"\t{input_file.strip()} -> {cdo_output}")

            # prepare input file
            self.run_config.cdo_outputs.append(cdo_output)
            self.run_config.cdo_inputs.append(cdo_input)

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

    @staticmethod
    def parallel_op(cdo_func, cdo_args, cdo_kwargs):
        print(cdo_args, cdo_kwargs)
        # cdo_func(*cdo_args, **cdo_kwargs)

    def run_real(self, cdo):
        cdo_op = getattr(cdo, self.operator)
        pool = ProcessPool(nodes=10)
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
            results.append(ops[i](*cdo_args[i], **cdo_kwargs[i]))

        return results

    def run(self, cdo, dry_run=False):
        if dry_run:
            return self.run_dry()

        return self.run_real(cdo)
