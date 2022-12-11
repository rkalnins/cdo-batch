from __future__ import annotations

import os

from .node import Node


class Operator:
    operator: str
    parameter: str
    output_node: Node
    opvar: dict
    output_format: str
    options: str

    chain: Operator

    def __init__(
        self,
        operator,
        parameter=None,
        output_node=None,
        opvar=None,
        output_format=None,
        chain=None,
        options=None,
    ):
        self.operator = operator
        self.parameter = parameter
        self.output_node = output_node

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

    def get_chain(self):
        if self.chain is None:
            return ""

        # last in chain is first operator applied
        cmd = self.chain.get_chain() + f" -{self.operator},{self.parameter}"
        return cmd.strip()

    def get_output_name(self, node, file_path):
        # TODO: provide more options for renaming

        # get destination directory (no file name)
        path = self.output_node.get_root_path()

        # get file name of output
        input_name = os.path.splitext(os.path.basename(file_path))[0]

        return os.path.join(
            path, self.output_format.format(input_basename=input_name, **self.opvar)
        )

    def run(self, cdo, node, dry_run=False):
        cdo_op_name = f"-{self.operator}"
        cdo_param = self.parameter.strip()

        cdo_op = getattr(cdo, self.operator)

        if self.options is None:
            cdo_options = ""
        else:
            cdo_options = self.options

        results = []

        for f in node.files:
            # prepare input file
            input_file = os.path.join(node.get_root_path(), f)

            if self.chain is None:
                # operating chaining provided with inputs
                cdo_input = f"{self.get_chain()} {input_file}".strip()
            else:
                # no chaining
                cdo_input = input_file.strip()

            # prepare output file
            cdo_output = self.get_output_name(node, f)

            if dry_run:
                results.append(
                    f"cdo {cdo_op_name},{cdo_param} {cdo_input} {cdo_output} {cdo_options}".strip()
                )

            # TODO: figure out how cdo works better to clean this up
            elif cdo_param and cdo_output:
                results.append(
                    cdo.cdo_op(
                        cdo_param,
                        input=cdo_input,
                        output=cdo_output,
                        options=cdo_options,
                    )
                )

            elif cdo_param:
                results.append(
                    cdo.cdo_op(cdo_param, input=cdo_input, options=cdo_options)
                )
            elif cdo_output:
                results.append(
                    cdo.cdo_op(input=cdo_input, output=cdo_output, options=cdo_options)
                )
            else:
                results.append(cdo.cdo_op(input=cdo_input, options=cdo_options))

        return results
