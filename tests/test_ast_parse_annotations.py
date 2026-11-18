"""The harness drops function annotations, and only those (see conftest.py)."""
import ast

SOURCE = """\
class A:
    x: int

def f(a: int, *args: str, b: int = 1, **kw: int) -> int: ...
y: int = 1
"""


def test_parse_drops_function_annotations_and_keeps_class_annotations():
    cls, function, variable = ast.parse(SOURCE).body

    assert cls.body[0].annotation is not None
    assert variable.annotation is not None
    assert function.returns is None
    arguments = function.args
    assert all(argument.annotation is None for argument in (
        arguments.args + arguments.kwonlyargs
        + [arguments.vararg, arguments.kwarg]))
