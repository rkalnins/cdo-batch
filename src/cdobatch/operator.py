from __future__ import annotations
from node import Node


class Operator:
    operator: str
    parameter: str
    output_node: Node
    opvar: dict
    output_format: str
    options: str

    chain: Command

    def __init__(
        self,
        operator,
        parameter,
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
            self.opvar = dict()
        else:
            self.opvar = opvar

        if output_format is None:
            self.output_format = "{input_basename}.nc"
        else:
            self.output_format = output_format

        self.chain = chain
        self.options = options

    def get_chain(self):
        if output_node is not None:
            # at the head of the chain, do not repeat current op/param
            return self.chain.get_chain()
        else:
            # last in chain is first operator applied
            return self.chain.get_chain() + f" -{self.operator},{self.parameter}"

    def get_output_name(self, node):
        # TODO: provide more options for renaming

        # get destination directory (no file name)
        path = self.output_node.get_root_path()

        # get file name of output
        input_name = os.path.splitext(os.path.basename(node.get_root_path()))[0]

        return path + output_format.format(input_basename=input_name, **self.opvar)

    def run(self, cdo, node):
        cdo_op_name = f"-{self.operator}"
        cdo_param = self.parameter

        if self.chain is None:
            # operating chaining provided with inputs
            cdo_input = self.get_chain() + " " + node.get_root_path()
        else:
            # no chaining
            cdo_input = node.get_root_path()

        cdo_output = self.get_output_name()

        cdo_op = getattr(cdo, cdo_op_name)
        cdo_options = self.options

        # TODO: figure out how cdo works better to clean this up
        if cdo_param and cdo_output:
            return cdo.cdo_op(
                cdo_param, input=cdo_input, output=cdo_output, options=cdo_options
            )
        elif cdo_param:
            return cdo.cdo_op(cdo_param, input=cdo_input, options=cdo_options)
        elif cdo_output:
            return cdo.cdo_op(input=cdo_input, output=cdo_output, options=cdo_options)
        else:
            return cdo.cdo_op(input=cdo_input, options=cdo_options)
