from __future__ import annotations

from .operator import Operator
from .node import Node


def apply(nodes, ops, dry_run=False):
    # TODO: aggregate and parallelize
    # TODO: capture output?
    # TODO: figure out if output directories need to be created, if so, need some sort of minimal solution
    # TODO: use multiprocessing
    pass
