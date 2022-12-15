from __future__ import annotations

import multiprocessing

from .operator import Operator
from .node import Node


def apply(nodes, ops, dry_run=False):
    for n in nodes:
        this_node_ops = ops[n.name]
    
        # TODO: aggregate and parallelize
        # TODO: capture output?
        # TODO: figure out if output directories need to be created, if so, need some sort of minimal solution
        # TODO: use multiprocessing
    pass
