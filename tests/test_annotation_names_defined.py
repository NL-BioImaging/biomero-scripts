"""Names in function signatures are defined when a script is imported.

Python evaluates parameter and return annotations (and defaults) when the
``def`` runs, which is at import. The harness runs single functions in small
namespaces and drops annotations when it parses a script (``conftest.py``), so
it would not notice a signature that names something only imported under an
``if`` (for example ``CanonicalInput`` for importer-only features): the script
then fails to load in the default configuration.

This test walks the module-level statements of every script in order, keeps
the names bound unconditionally (import, def, class or assignment outside any
``if`` / ``try`` / ``with``) and checks the annotations and defaults of every
function (and class-body annotations) against them. String annotations are not
evaluated and are skipped.
"""
import ast
import builtins
import os
from pathlib import Path

import pytest

SOURCE_ROOT = Path(os.environ.get(
    "BIOMERO_SCRIPTS_ROOT", Path(__file__).parents[1]
))
SCRIPTS = sorted(path for folder in ("__workflows", "_data", "admin")
                 for path in (SOURCE_ROOT / folder).glob("*.py"))
BUILTINS = set(dir(builtins))


def parse(source, filename="<script>"):
    """Parse without the harness patch that strips annotations."""
    return compile(source, filename, "exec", flags=ast.PyCF_ONLY_AST)


def names_in(node):
    """Names an annotation or default expression looks up when evaluated."""
    if node is None:
        return set()
    return {child.id for child in ast.walk(node)
            if isinstance(child, ast.Name)
            and isinstance(child.ctx, ast.Load)}


def signature_expressions(function):
    """Annotations and defaults of a function, as evaluated at definition."""
    arguments = function.args
    every_argument = (arguments.posonlyargs + arguments.args
                      + arguments.kwonlyargs
                      + [arguments.vararg, arguments.kwarg])
    expressions = [argument.annotation for argument in every_argument
                   if argument is not None]
    expressions += [function.returns, *arguments.defaults,
                    *arguments.kw_defaults]
    # A string is a forward reference that is not evaluated.
    return [expression for expression in expressions
            if expression is not None
            and not (isinstance(expression, ast.Constant)
                     and isinstance(expression.value, str))]


def bound_by(statement):
    """Names a top-level statement binds unconditionally."""
    if isinstance(statement, ast.Import):
        return {(alias.asname or alias.name.split(".")[0])
                for alias in statement.names}
    if isinstance(statement, ast.ImportFrom):
        return {(alias.asname or alias.name) for alias in statement.names}
    if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.ClassDef)):
        return {statement.name}
    if isinstance(statement, ast.Assign):
        return {target.id for target in statement.targets
                if isinstance(target, ast.Name)}
    if isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name):
        return {statement.target.id}
    if isinstance(statement, ast.If):
        # Bound whichever branch runs: only what both branches bind counts.
        in_body = set().union(*[bound_by(node) for node in statement.body])
        in_else = set().union(*[bound_by(node) for node in statement.orelse])
        return in_body & in_else
    return set()


def undefined_annotation_names(source):
    """Return (line, name) of signature names not bound before their ``def``."""
    problems = []
    bound = set()
    for statement in parse(source).body:
        functions = []
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(statement)
        elif isinstance(statement, ast.ClassDef):
            functions += [node for node in statement.body
                          if isinstance(node, (ast.FunctionDef,
                                               ast.AsyncFunctionDef))]
            class_names = {node.target.id for node in statement.body
                           if isinstance(node, ast.AnnAssign)
                           and isinstance(node.target, ast.Name)}
            class_names |= {name for node in statement.body
                            for name in bound_by(node)}
            for node in statement.body:
                if isinstance(node, ast.AnnAssign):
                    for name in names_in(node.annotation):
                        if name not in bound | class_names | BUILTINS:
                            problems.append((node.lineno, name))
        for function in functions:
            for expression in signature_expressions(function):
                for name in names_in(expression):
                    if name not in bound | BUILTINS:
                        problems.append((expression.lineno, name))
        bound |= bound_by(statement)
    return sorted(set(problems))


def test_the_check_finds_a_name_that_is_only_conditionally_imported():
    source = (
        "from typing import Tuple\n"
        "if FLAG:\n"
        "    from schema import CanonicalInput\n"
        "def parse() -> Tuple[CanonicalInput, ...]:\n"
        "    return ()\n")
    assert undefined_annotation_names(source) == [(4, "CanonicalInput")]


def test_the_check_finds_a_name_defined_after_its_use():
    source = (
        "def make(a: Later = None) -> int: ...\n"
        "class Later: ...\n")
    assert undefined_annotation_names(source) == [(1, "Later")]


def test_the_check_accepts_a_name_bound_by_both_branches_of_an_if():
    source = (
        "if FLAG:\n"
        "    SCRIPTS = [1]\n"
        "else:\n"
        "    SCRIPTS = [2]\n"
        "def run(scripts=SCRIPTS): ...\n")
    assert undefined_annotation_names(source) == []


def test_the_check_accepts_unconditional_names_builtins_and_strings():
    source = (
        "from typing import Dict\n"
        "import uuid\n"
        "class Run:\n"
        "    wf: uuid.UUID\n"
        "    def go(self, a: Dict[str, int], b: 'Later', c: int = len('x'))"
        " -> 'Later': ...\n")
    assert undefined_annotation_names(source) == []


def test_the_scripts_are_found():
    assert SCRIPTS, f"no scripts found below {SOURCE_ROOT}"


@pytest.mark.parametrize("path", SCRIPTS, ids=lambda path: path.name)
def test_signature_names_are_defined_unconditionally_at_import(path):
    problems = undefined_annotation_names(path.read_text(encoding="utf-8"))
    assert not problems, "\n".join(
        f"{path.name}:{line} annotation or default uses {name!r}, which is "
        f"not bound unconditionally before this def" for line, name in problems)
