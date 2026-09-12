#!/usr/bin/env python3
"""Small, dependency-free architecture and maintainability audit.

The checker is deliberately static.  It reads Python source with :mod:`ast`
and does not import the project, so it is safe to run while a transport, an
OAuth store, or a SQLite database is unavailable.  Its output is intended to
be compared before and after a refactor; it therefore avoids timestamps and
other ambient state.

The rules are intentionally narrow:

* ``mtf_lab.core`` is the pure domain layer and may not depend on adapters,
  persistence, presentation, the optional SDK, or I/O/clock modules.
* imports guarded only by ``TYPE_CHECKING`` are reported but do not become
  runtime graph edges;
* strongly connected components are reported as cycles;
* top-level calls which can perform I/O, launch a process, or sleep are
  reported as import-time side effects;
* private attributes are inventory, not an automatic failure.  Cross-object
  private access is kept visible so a caller cannot accidentally hide a
  coupling behind a seemingly harmless refactor;
* complexity is a comparable McCabe-style measure.  It is evidence for
  review, not a substitute for design review.

Only the Python standard library is used.  The public entry points
``analyze_repository`` and ``main`` are intentionally usable from a test or a
CI-like local command without installing a linter.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

PACKAGE = "mtf_lab"
DEFAULT_COMPLEXITY_THRESHOLD = 10

# Internal dependencies forbidden for the pure domain layer.  A TYPE_CHECKING
# edge is retained in ``typing_only_forbidden_core_imports`` but is not a
# runtime violation.
CORE_FORBIDDEN_INTERNAL = (
    "mtf_lab.configuration",
    "mtf_lab.data",
    "mtf_lab.ops",
    "mtf_lab.pipeline",
    "mtf_lab.runtime",
)
CORE_FORBIDDEN_EXTERNAL = (
    "argparse",
    "asyncio",
    "ctrader_open_api",
    "google",
    "http",
    "requests",
    "scipy",
    "shutil",
    "socket",
    "sqlite3",
    "ssl",
    "subprocess",
    "threading",
    "urllib",
    "webbrowser",
    "websockets",
    "twisted",
)

_SIDE_EFFECT_ROOTS = {
    "open",
    "os.chmod",
    "os.makedirs",
    "os.mkdir",
    "os.remove",
    "os.rename",
    "os.replace",
    "os.rmdir",
    "os.unlink",
    "pathlib.Path.cwd",
    "pathlib.Path.mkdir",
    "pathlib.Path.open",
    "pathlib.Path.unlink",
    "pathlib.Path.write_bytes",
    "pathlib.Path.write_text",
    "shutil.copy",
    "shutil.copy2",
    "shutil.move",
    "shutil.rmtree",
    "socket.create_connection",
    "socket.socket",
    "sqlite3.connect",
    "subprocess.call",
    "subprocess.check_call",
    "subprocess.check_output",
    "subprocess.Popen",
    "subprocess.run",
    "time.sleep",
    "urllib.request.urlopen",
    "webbrowser.open",
}

_CLOCK_ENV_CALLS = {
    "datetime.datetime.now",
    "datetime.datetime.utcnow",
    "datetime.now",
    "datetime.utcnow",
    "os.environ.get",
    "os.getenv",
    "os.getcwd",
    "pathlib.Path.cwd",
    "time.monotonic",
    "time.sleep",
}


def _call_name(node: ast.AST) -> str:
    """Return a stable dotted spelling for a call target when possible."""

    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _call_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    if isinstance(node, ast.Call):
        return f"{_call_name(node.func)}(...)"
    return "<dynamic>"


def _is_type_checking_test(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "TYPE_CHECKING"
    if isinstance(node, ast.Attribute):
        return node.attr == "TYPE_CHECKING"
    return False


def _is_main_guard(node: ast.If) -> bool:
    test = node.test
    if not isinstance(test, ast.Compare) or len(test.ops) != 1:
        return False
    if not isinstance(test.ops[0], ast.Eq) or len(test.comparators) != 1:
        return False
    left = test.left
    right = test.comparators[0]
    return (
        isinstance(left, ast.Name)
        and left.id == "__name__"
        and isinstance(right, ast.Constant)
        and right.value == "__main__"
    ) or (
        isinstance(right, ast.Name)
        and right.id == "__name__"
        and isinstance(left, ast.Constant)
        and left.value == "__main__"
    )


def _literal_string(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _is_forbidden_prefix(value: str, prefixes: Iterable[str]) -> bool:
    return any(value == prefix or value.startswith(prefix + ".") for prefix in prefixes)


def _relative_target(current: str, is_package: bool, level: int, imported: str | None) -> str:
    """Resolve an ImportFrom spelling relative to a known source module."""

    parts = current.split(".")
    package_parts = parts if is_package else parts[:-1]
    # level=1 means the current package; level=2 means its parent.
    keep = max(0, len(package_parts) - (level - 1))
    base = package_parts[:keep]
    if imported:
        base.append(imported)
    return ".".join(base)


def _module_info(root: Path, include_tests: bool) -> tuple[dict[str, Path], dict[str, bool]]:
    package_root = root / PACKAGE
    files: dict[str, Path] = {}
    package_flags: dict[str, bool] = {}
    if not package_root.is_dir():
        return files, package_flags
    for path in sorted(package_root.rglob("*.py")):
        if not path.is_file():
            continue
        if not include_tests and (path.name.startswith("test_") or path.name.endswith("_test.py")):
            continue
        relative = path.relative_to(root)
        parts = list(relative.parts)
        if parts[-1] == "__init__.py":
            parts.pop()
            module = ".".join(parts)
            is_package = True
        else:
            parts[-1] = parts[-1][:-3]
            module = ".".join(parts)
            is_package = False
        if not module:
            continue
        files[module] = path
        package_flags[module] = is_package
    return files, package_flags


class _ImportCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self._typing_only = False

    def _record(
        self,
        node: ast.AST,
        target: str,
        kind: str,
        names: list[str] | None = None,
    ) -> None:
        record = {
            "line": int(getattr(node, "lineno", 0)),
            "target": target,
            "kind": kind,
            "typing_only": self._typing_only,
        }
        if names is not None:
            # ImportFrom keeps the imported symbol separate from ``module``;
            # retaining it prevents ``from .. import __version__`` from being
            # mistaken for ``from .. import every child submodule``.
            record["names"] = names
        self.records.append(record)

    def visit_If(self, node: ast.If) -> None:  # noqa: N802 - AST protocol
        old = self._typing_only
        guarded = _is_type_checking_test(node.test)
        self._typing_only = old or guarded
        for statement in node.body:
            self.visit(statement)
        self._typing_only = old
        for statement in node.orelse:
            self.visit(statement)

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            self._record(node, alias.name, "import")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        target = ("." * node.level) + (node.module or "")
        self._record(node, target, "from", [alias.name for alias in node.names])


class _DynamicImportCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        name = _call_name(node.func)
        if name in {"__import__", "importlib.import_module", "import_module"}:
            value = _literal_string(node.args[0]) if node.args else None
            self.records.append(
                {
                    "line": int(getattr(node, "lineno", 0)),
                    "call": name,
                    "literal": value,
                }
            )
        self.generic_visit(node)


class _PrivateStateCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self._scope: list[str] = []

    @property
    def scope(self) -> str:
        return ".".join(self._scope) or "<module>"

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self._scope.append(node.name)
        for statement in node.body:
            self.visit(statement)
        self._scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._scope.append(node.name)
        for statement in node.body:
            self.visit(statement)
        self._scope.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._scope.append(node.name)
        for statement in node.body:
            self.visit(statement)
        self._scope.pop()

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        if node.attr.startswith("_") and not node.attr.startswith("__"):
            base = ast.unparse(node.value)
            root = base.split(".", 1)[0]
            external = root not in {"self", "cls", "super"} and not base.startswith("super(")
            self.records.append(
                {
                    "line": int(getattr(node, "lineno", 0)),
                    "scope": self.scope,
                    "attribute": node.attr,
                    "base": base,
                    "access": "write" if isinstance(node.ctx, ast.Store) else "read",
                    "cross_object": external,
                }
            )
        self.generic_visit(node)


class _ComplexityVisitor(ast.NodeVisitor):
    """McCabe-style score for one function body, excluding nested functions."""

    def __init__(self) -> None:
        self.score = 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
        return

    def visit_If(self, node: ast.If) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)

    def visit_IfExp(self, node: ast.IfExp) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)

    def visit_While(self, node: ast.While) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:  # noqa: N802
        self.score += len(node.handlers)
        self.generic_visit(node)

    def visit_TryStar(self, node: ast.TryStar) -> None:  # noqa: N802
        self.score += len(node.handlers)
        self.generic_visit(node)

    def visit_Match(self, node: ast.Match) -> None:  # noqa: N802
        self.score += len(node.cases)
        self.generic_visit(node)

    def visit_BoolOp(self, node: ast.BoolOp) -> None:  # noqa: N802
        self.score += max(0, len(node.values) - 1)
        self.generic_visit(node)

    def visit_comprehension(self, node: ast.comprehension) -> None:  # noqa: N802
        self.score += 1
        self.generic_visit(node)


def _collect_functions(tree: ast.AST) -> list[dict[str, Any]]:
    functions: list[dict[str, Any]] = []

    def walk(node: ast.AST, scope: list[str]) -> None:
        if isinstance(node, ast.ClassDef):
            new_scope = scope + [node.name]
            for child in node.body:
                walk(child, new_scope)
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            new_scope = scope + [node.name]
            visitor = _ComplexityVisitor()
            for statement in node.body:
                visitor.visit(statement)
            functions.append(
                {
                    "name": ".".join(new_scope),
                    "line": int(node.lineno),
                    "end_line": int(getattr(node, "end_lineno", node.lineno)),
                    "lines": int(getattr(node, "end_lineno", node.lineno) - node.lineno + 1),
                    "complexity": visitor.score,
                    "async": isinstance(node, ast.AsyncFunctionDef),
                }
            )
            # Discover nested functions/classes, but do not include them in the
            # enclosing function's score (the visitor above skips them).
            for child in node.body:
                walk(child, new_scope)
            return
        for nested_node in ast.iter_child_nodes(node):
            walk(nested_node, scope)

    walk(tree, [])
    return functions


class _TopLevelSideEffectCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
        return

    def visit_If(self, node: ast.If) -> None:  # noqa: N802
        if _is_main_guard(node) or _is_type_checking_test(node.test):
            # Neither branch executes during a normal import: the main guard
            # is deferred to script execution and TYPE_CHECKING is false at
            # runtime.
            for statement in node.orelse:
                self.visit(statement)
            return
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        name = _call_name(node.func)
        matched = name in _SIDE_EFFECT_ROOTS
        if name == "open":
            # Reading or writing a file at import time is an observable I/O
            # dependency.  Keep it visible even when the mode is dynamic.
            matched = True
        if matched:
            self.records.append(
                {
                    "line": int(getattr(node, "lineno", 0)),
                    "call": name,
                    "reason": "import_time_io_or_process",
                }
            )
        self.generic_visit(node)


def _module_layer(module: str) -> str:
    if module == "mtf_lab.core" or module.startswith("mtf_lab.core."):
        return "core"
    if module == "mtf_lab.data" or module.startswith("mtf_lab.data."):
        return "data"
    if module == "mtf_lab.runtime" or module.startswith("mtf_lab.runtime."):
        return "runtime"
    if module == "mtf_lab.ops" or module.startswith("mtf_lab.ops."):
        return "ops"
    return "package"


def _root_name(target: str) -> str:
    return target.lstrip(".").split(".", 1)[0]


def _tarjan(graph: dict[str, set[str]]) -> list[list[str]]:  # noqa: C901 - Tarjan DFS state machine
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    components: list[list[str]] = []

    def strongconnect(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for neighbor in sorted(graph.get(node, ())):
            if neighbor not in indices:
                strongconnect(neighbor)
                lowlinks[node] = min(lowlinks[node], lowlinks[neighbor])
            elif neighbor in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[neighbor])
        if lowlinks[node] == indices[node]:
            component: list[str] = []
            while True:
                current = stack.pop()
                on_stack.remove(current)
                component.append(current)
                if current == node:
                    break
            component.sort()
            if len(component) > 1 or node in graph.get(node, set()):
                components.append(component)

    for node in sorted(graph):
        if node not in indices:
            strongconnect(node)
    return sorted(components)


def _safe_read(path: Path) -> tuple[str | None, str | None]:
    try:
        return path.read_text(encoding="utf-8"), None
    except (OSError, UnicodeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _relative_import_edges(
    module: str,
    record: dict[str, Any],
    *,
    is_package: bool,
    files: dict[str, Path],
    level: int,
    imported: str | None,
) -> tuple[set[str], str]:
    resolved = _relative_target(module, is_package, level, imported)
    if record["typing_only"]:
        return set(), resolved
    if imported is not None:
        return ({resolved} if resolved in files else set()), resolved

    package = _relative_target(module, is_package, level, None)
    edges = {package} if package in files and package != module else set()
    edges.update(f"{package}.{name}" for name in record.get("names", []) if f"{package}.{name}" in files)
    return edges, resolved


def _resolve_import(
    module: str,
    record: dict[str, Any],
    *,
    is_package: bool,
    files: dict[str, Path],
) -> tuple[set[str], dict[str, Any] | None, str]:
    raw = str(record["target"])
    typing_only = bool(record["typing_only"])
    if raw.startswith("."):
        level = len(raw) - len(raw.lstrip("."))
        imported = raw[level:] or None
        edges, resolved = _relative_import_edges(
            module,
            record,
            is_package=is_package,
            files=files,
            level=level,
            imported=imported,
        )
        return edges, None, resolved
    if raw.startswith(PACKAGE + ".") or raw == PACKAGE:
        return ({raw} if raw in files and not typing_only else set()), None, raw
    return (
        set(),
        {
            "line": int(record["line"]),
            "target": raw,
            "root": _root_name(raw),
            "kind": record["kind"],
            "typing_only": typing_only,
        },
        raw,
    )


def _classify_import(
    module: str,
    record: dict[str, Any],
    *,
    is_package: bool,
    files: dict[str, Path],
) -> tuple[set[str], dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve one import and return edges, external record, and violations."""

    edges, external, resolved = _resolve_import(module, record, is_package=is_package, files=files)
    if _module_layer(module) != "core":
        return edges, external, None, None
    target_for_rule = resolved or str(record["target"])
    forbidden_target = (
        _is_forbidden_prefix(target_for_rule, CORE_FORBIDDEN_INTERNAL)
        or _root_name(target_for_rule) in CORE_FORBIDDEN_EXTERNAL
    )
    if not forbidden_target:
        return edges, external, None, None
    violation = {
        "module": module,
        "line": int(record["line"]),
        "target": target_for_rule,
        "typing_only": bool(record["typing_only"]),
    }
    if record["typing_only"]:
        return edges, external, None, violation
    return edges, external, violation, None


def _analyze_tree(module: str, tree: ast.AST, *, is_package: bool, files: dict[str, Path]) -> dict[str, Any]:
    graph: set[str] = set()
    external_imports: list[dict[str, Any]] = []
    forbidden: list[dict[str, Any]] = []
    typing_forbidden: list[dict[str, Any]] = []
    imports = _ImportCollector()
    imports.visit(tree)
    for record in imports.records:
        edges, external, violation, typing_violation = _classify_import(
            module, record, is_package=is_package, files=files
        )
        graph.update(edges)
        if external is not None:
            external_imports.append(external)
        if violation is not None:
            forbidden.append(violation)
        if typing_violation is not None:
            typing_forbidden.append(typing_violation)

    complexity = [dict(item, module=module) for item in _collect_functions(tree)]
    private = [dict(item, module=module) for item in _collect_private_state(tree)]
    side_effects = [dict(item, module=module) for item in _collect_side_effects(tree)]
    dynamic_imports = [dict(item, module=module) for item in _collect_dynamic_imports(tree)]
    clock_accesses: list[dict[str, Any]] = []
    if _module_layer(module) == "core":
        clock_accesses = [
            {
                "module": module,
                "line": int(getattr(node, "lineno", 0)),
                "call": _call_name(node.func),
            }
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and _call_name(node.func) in _CLOCK_ENV_CALLS
        ]
    return {
        "edges": graph,
        "external_imports": external_imports,
        "forbidden": forbidden,
        "typing_forbidden": typing_forbidden,
        "complexity": complexity,
        "private": private,
        "side_effects": side_effects,
        "dynamic_imports": dynamic_imports,
        "clock_accesses": clock_accesses,
    }


def _collect_private_state(tree: ast.AST) -> list[dict[str, Any]]:
    collector = _PrivateStateCollector()
    collector.visit(tree)
    return collector.records


def _collect_side_effects(tree: ast.AST) -> list[dict[str, Any]]:
    collector = _TopLevelSideEffectCollector()
    for statement in tree.body:  # type: ignore[attr-defined]
        collector.visit(statement)
    return collector.records


def _collect_dynamic_imports(tree: ast.AST) -> list[dict[str, Any]]:
    collector = _DynamicImportCollector()
    collector.visit(tree)
    return collector.records


def _analyze_module(module: str, path: Path, *, is_package: bool, files: dict[str, Path]) -> dict[str, Any]:
    source, read_error = _safe_read(path)
    if read_error:
        return {"module_record": None, "parse_errors": [{"module": module, "path": str(path), "error": read_error}]}
    assert source is not None
    try:
        tree = ast.parse(source, filename=str(path))
    except (SyntaxError, ValueError, TypeError) as exc:
        return {
            "module_record": {
                "bytes": len(source.encode("utf-8")),
                "lines": len(source.splitlines()),
            },
            "parse_errors": [{"module": module, "path": str(path), "error": f"{type(exc).__name__}: {exc}"}],
        }
    result = _analyze_tree(module, tree, is_package=is_package, files=files)
    result["module_record"] = {
        "bytes": len(source.encode("utf-8")),
        "lines": len(source.splitlines()),
    }
    result["parse_errors"] = []
    return result


def analyze_repository(
    root: str | os.PathLike[str] = ".",
    *,
    include_tests: bool = False,
    complexity_threshold: int = DEFAULT_COMPLEXITY_THRESHOLD,
) -> dict[str, Any]:
    """Return a deterministic static audit for ``root``.

    ``root`` is expected to contain the ``mtf_lab`` package.  Missing files
    and parse errors are represented in the result rather than raised, which
    keeps before/after reports useful during an in-progress refactor.
    """

    root_path = Path(root).resolve()
    files, package_flags = _module_info(root_path, include_tests)
    module_records: dict[str, dict[str, Any]] = {}
    graph: dict[str, set[str]] = {module: set() for module in files}
    external_imports: dict[str, list[dict[str, Any]]] = {}
    private_records: list[dict[str, Any]] = []
    complexity_records: list[dict[str, Any]] = []
    side_effects: list[dict[str, Any]] = []
    dynamic_imports: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    forbidden: list[dict[str, Any]] = []
    typing_forbidden: list[dict[str, Any]] = []
    clock_environment_accesses: list[dict[str, Any]] = []

    for module in sorted(files):
        outcome = _analyze_module(module, files[module], is_package=package_flags[module], files=files)
        if outcome.get("module_record") is not None:
            module_records[module] = {
                **outcome["module_record"],
                "path": str(files[module].relative_to(root_path)),
                "layer": _module_layer(module),
            }
        parse_errors.extend(outcome.get("parse_errors", []))
        graph[module].update(outcome.get("edges", set()))
        if outcome.get("external_imports"):
            external_imports[module] = outcome["external_imports"]
        private_records.extend(outcome.get("private", []))
        complexity_records.extend(outcome.get("complexity", []))
        side_effects.extend(outcome.get("side_effects", []))
        dynamic_imports.extend(outcome.get("dynamic_imports", []))
        forbidden.extend(outcome.get("forbidden", []))
        typing_forbidden.extend(outcome.get("typing_forbidden", []))
        clock_environment_accesses.extend(outcome.get("clock_accesses", []))

    for dynamic in dynamic_imports:
        literal = dynamic.get("literal")
        if _module_layer(str(dynamic.get("module", ""))) != "core" or not isinstance(literal, str):
            continue
        if _is_forbidden_prefix(literal, CORE_FORBIDDEN_INTERNAL) or _root_name(literal) in CORE_FORBIDDEN_EXTERNAL:
            forbidden.append(
                {
                    "module": dynamic["module"],
                    "line": int(dynamic["line"]),
                    "target": literal,
                    "typing_only": False,
                    "dynamic": True,
                }
            )

    for module in module_records:
        module_records[module]["runtime_dependencies"] = sorted(graph[module])
        module_records[module]["runtime_dependency_count"] = len(graph[module])

    cycles = _tarjan(graph)
    complexity_records.sort(
        key=lambda item: (-int(item["complexity"]), item["module"], int(item["line"]), item["name"])
    )
    private_records.sort(key=lambda item: (item["module"], int(item["line"]), item["scope"], item["attribute"]))
    side_effects.sort(key=lambda item: (item["module"], int(item["line"]), item["call"]))
    dynamic_imports.sort(key=lambda item: (item["module"], int(item["line"]), item["call"]))
    forbidden.sort(key=lambda item: (item["module"], int(item["line"]), item["target"]))
    typing_forbidden.sort(key=lambda item: (item["module"], int(item["line"]), item["target"]))
    clock_environment_accesses.sort(key=lambda item: (item["module"], int(item["line"]), item["call"]))

    high_complexity = [item for item in complexity_records if int(item["complexity"]) > complexity_threshold]
    cross_private = [item for item in private_records if item["cross_object"]]
    private_writes = [item for item in private_records if item["access"] == "write"]
    strict_violations: list[dict[str, Any]] = []
    strict_violations.extend({"kind": "parse_error", **item} for item in parse_errors)
    strict_violations.extend({"kind": "cycle", "modules": cycle} for cycle in cycles)
    strict_violations.extend({"kind": "forbidden_core_import", **item} for item in forbidden)
    strict_violations.extend({"kind": "import_time_side_effect", **item} for item in side_effects)
    strict_violations.extend(
        {"kind": "core_clock_or_environment_access", **item} for item in clock_environment_accesses
    )

    return {
        "schema_version": 1,
        "root": str(root_path),
        "package": PACKAGE,
        "include_tests": include_tests,
        "complexity_threshold": complexity_threshold,
        "summary": {
            "modules": len(module_records),
            "dependency_edges": sum(len(values) for values in graph.values()),
            "external_import_modules": len(external_imports),
            "cycles": len(cycles),
            "forbidden_core_imports": len(forbidden),
            "typing_only_forbidden_core_imports": len(typing_forbidden),
            "import_time_side_effects": len(side_effects),
            "dynamic_imports": len(dynamic_imports),
            "functions": len(complexity_records),
            "high_complexity_functions": len(high_complexity),
            "max_complexity": max((int(item["complexity"]) for item in complexity_records), default=0),
            "private_accesses": len(private_records),
            "cross_object_private_accesses": len(cross_private),
            "private_writes": len(private_writes),
            "parse_errors": len(parse_errors),
            "strict_violations": len(strict_violations),
        },
        "modules": module_records,
        "dependencies": {module: sorted(values) for module, values in sorted(graph.items())},
        "external_imports": external_imports,
        "cycles": cycles,
        "complexity": {
            "metric": "1 + if/loop/except/match/conditional-expression + boolean-operands-minus-one + comprehensions; nested functions excluded",
            "functions": complexity_records,
            "over_threshold": high_complexity,
        },
        "private_state": {
            "accesses": private_records,
            "cross_object": cross_private,
            "writes": private_writes,
        },
        "import_time_side_effects": side_effects,
        "dynamic_imports": dynamic_imports,
        "forbidden_core_imports": forbidden,
        "typing_only_forbidden_core_imports": typing_forbidden,
        "core_clock_or_environment_accesses": clock_environment_accesses,
        "parse_errors": parse_errors,
        "strict_violations": strict_violations,
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Auditoría AST offline de arquitectura, complejidad y estado privado")
    parser.add_argument("--root", default=".", help="raíz que contiene mtf_lab (por defecto: .)")
    parser.add_argument("--json", dest="json_path", default="-", help="salida JSON o '-' para stdout")
    parser.add_argument("--include-tests", action="store_true", help="incluye test_*.py dentro de mtf_lab")
    parser.add_argument("--complexity-threshold", type=int, default=DEFAULT_COMPLEXITY_THRESHOLD)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="termina con 1 ante ciclos, dependencias prohibidas o efectos de importación",
    )
    args = parser.parse_args(argv)
    if args.complexity_threshold < 1:
        parser.error("--complexity-threshold debe ser positivo")
    result = analyze_repository(
        args.root, include_tests=args.include_tests, complexity_threshold=args.complexity_threshold
    )
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.json_path == "-":
        sys.stdout.write(rendered)
    else:
        _write_json(Path(args.json_path), result)
    if args.strict and result["strict_violations"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
