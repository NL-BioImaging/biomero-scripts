"""Shared pytest setup for the harness.

Many tests load single functions out of a script by parsing it with ``ast``
and executing the extracted nodes in a small, hand-built namespace. The type
annotations of those functions are evaluated when they are defined, which
would require every such namespace to provide each annotated name (``List``,
``UUID``, ``SlurmClient``, ...). The tests do not depend on annotations, so
they are dropped from function signatures when a script is parsed.

Only function signatures are touched; annotations in class bodies (dataclass
fields, ``NamedTuple``) and on variables are kept. The harness has about 45
``compile(...)`` sites and no shared loader, so a ``from __future__ import
annotations`` compiler flag cannot be applied in one place; patching
``ast.parse`` here is the one central point.
"""
import ast

_ORIGINAL_PARSE = ast.parse


def _drop_signature_annotations(tree: ast.AST) -> ast.AST:
    """Remove parameter and return annotations from every function."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.returns = None
            arguments = node.args
            for argument in (arguments.posonlyargs + arguments.args
                             + arguments.kwonlyargs
                             + [arguments.vararg, arguments.kwarg]):
                if argument is not None:
                    argument.annotation = None
    return tree


def _parse_without_annotations(*args, **kwargs) -> ast.AST:
    """``ast.parse`` that drops function signature annotations."""
    return _drop_signature_annotations(_ORIGINAL_PARSE(*args, **kwargs))


ast.parse = _parse_without_annotations
