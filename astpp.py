#!/usr/bin/env python3
"""Self-Inlining Python Macro Preprocessor & Runtime Polyfill Library.

Homepage:
---------
https://github.com/f-fix/astpp

Overview:
---------
`astpp` is a dual-use Python library and AST-level macro preprocessor designed
for cross-runtime codebases that target MicroPython, PyPy, and CPython.

1. Runtime Polyfill (Zero-Build Development):
   Importing `astpp` provides zero-overhead, runtime-compatible implementations of
   MicroPython compiler pragmas (`const`, `micropython.native`, `micropython.viper`)
   and conditional decorators (`keep`, `omit`, `keep_if`, `omit_if`). Scripts can be
   executed directly under CPython, PyPy, or MicroPython without requiring an explicit
   build or preprocessing step.

2. Compile-Time Preprocessor (Zero-Dependency Builds):
   When invoked as a script or via `astpp_main()`, `astpp` inspects the consumer's
   Abstract Syntax Tree (AST), removes the `from astpp import ...` statement entirely,
   and inlines minimal self-contained fallback definitions copied directly from its own
   source code. The emitted code has zero external dependencies on `astpp.py`.

Documentation & Tested Examples:
--------------------------------
The following verified examples illustrate source transformations across various CLI flags.
These exact inputs, flags, and outputs are parsed directly from this docstring and validated
by `astpp_main(["--test"])`:

### Example 1: Import Inlining
Input:
```python
from astpp import const
X = const(10)
```
Flags: -O3
Output:
```python
try:
    from micropython import const
except ImportError:

    def const(x):
        return x
X = const(10)
```

### Example 2: Constant Definition & In-Line Override
Input:
```python
_X = const(True)
BUFFER_SIZE = const(1024)
```
Flags: -DBUFFER_SIZE=512 -O
Output:
```python
BUFFER_SIZE = const(512)
```

### Example 3: Conditional Decorator Pruning
Input:
```python
@keep_if(_IS_DESKTOP_PY)
def desktop_helper():
    return 42
```
Flags: -D_IS_DESKTOP_PY=False
Output:
```python
```

### Example 4: Assertions and Debug Block Stripping
Input:
```python
assert 1 == 1, 'invariant'
if __debug__:
    def debug_only():
        pass
```
Flags: -O
Output:
```python
```

### Example 5: Native Decorator Stripping via Macro Mapping
Input:
```python
@micropython.native
def fast_calc(x):
    return x * 2
```
Flags: -Dmicropython.native=keep
Output:
```python
def fast_calc(x):
    return x * 2
```

CLI Usage:
----------
  python3 astpp.py [-O[n]] [-DNAME=VAL ...] [input] [-o output]
  python3 astpp.py --help
  python3 astpp.py --test

API Usage:
----------
  from astpp import astpp_main
  astpp_main(argv=['-O3', '-D_IS_DESKTOP_PY=False'], stdin=in_stream, stdout=out_stream)
"""

# ==============================================================================
# 1. RUNTIME EXPORTS
# ==============================================================================
try:
    from micropython import const
except ImportError:

    def const(x):
        """Identity fallback for compile-time constants on non-MicroPython runtimes.

        On MicroPython, `const()` is intercepted natively at parse time to inline values.
        On CPython and PyPy, this function acts as an identity pass-through returning `x`.
        """
        return x


def keep(obj):
    """Identity decorator: keeps the decorated function or class as-is.

    When processed by `astpp`, the `@keep` decorator is stripped from the AST so that the
    compiled output incurs zero runtime decorator invocation overhead.
    """
    return obj


def omit(obj):
    """Omission decorator: drops the decorated function or class completely.

    When processed by `astpp`, any node decorated with `@omit` is purged from the parse tree.
    """
    return None


def keep_if(cond):
    """Conditional decorator factory that preserves the target definition if cond is True.

    Parameters:
        cond: Expression evaluating to a truthy or falsy value.

    Returns:
        The identity decorator `keep` if cond is True, or `omit` if False.
        When processed by `astpp`, the decorator is stripped if kept, or the entire node is
        deleted from the AST if omitted.
    """
    return keep if cond else omit


def omit_if(cond):
    """Conditional decorator factory that drops the target definition if cond is True.

    Parameters:
        cond: Expression evaluating to a truthy or falsy value.

    Returns:
        The omission decorator `omit` if cond is True, or `keep` if False.
        When processed by `astpp`, the entire target node is pruned from the AST if omitted.
    """
    return omit if cond else keep


try:
    import micropython as _real_mpy
except ImportError:
    _real_mpy = None

if _real_mpy is not None:
    micropython = _real_mpy
else:

    class _MicroPythonShim:
        """Desktop compatibility shim providing no-op decorators for MicroPython pragmas."""

        native = staticmethod(keep)
        viper = staticmethod(keep)

    micropython = _MicroPythonShim()

_OMIT_MAIN = const(False)

__version__ = "0.1.3"


# ==============================================================================
# 2. PREPROCESSOR ENTRYPOINT & SELF-TEST SUITE
# ==============================================================================
@omit_if(_OMIT_MAIN)
def astpp_main(argv=None, stdin=None, stdout=None, stderr=None):
    """Programmatic entrypoint for AST-level preprocessing, help display, and self-testing.

    Parameters:
        argv:   List of command-line argument strings (e.g. ['-O3', '-DFOO=1']).
                Defaults to sys.argv[1:] if None.
        stdin:  Readable text stream containing Python source code. Defaults to sys.stdin.
        stdout: Writable text stream to receive transformed source code. Defaults to sys.stdout.
        stderr: Writable text stream for diagnostic logging and error messages. Defaults to sys.stderr.

    Returns:
        0 on successful transformation, help output, or passing test suite; non-zero on error.
    """
    import ast
    import copy
    import argparse

    if argv is None or stdin is None or stdout is None or stderr is None:
        import sys

        if argv is None:
            argv = sys.argv[1:]
        if stdin is None:
            stdin = sys.stdin
        if stdout is None:
            stdout = sys.stdout
        if stderr is None:
            stderr = sys.stderr

    # --- Argument Parser & Normal Filter Mode ---
    def _build_parser():
        parser = argparse.ArgumentParser(
            prog="astpp",
            description="Self-Inlining Macro Preprocessor & Runtime Polyfill Library.",
            add_help=False,
        )
        parser.add_argument(
            "-h", "--help", action="store_true", help="Show this help message and exit"
        )
        parser.add_argument(
            "input",
            nargs="?",
            default="-",
            help="Input Python source file or '-' for stdin (default: '-')",
        )
        parser.add_argument(
            "-o",
            "--output",
            default="-",
            help="Output destination file or '-' for stdout (default: '-')",
        )
        parser.add_argument(
            "-D",
            dest="defines",
            action="append",
            default=[],
            metavar="NAME=VALUE",
            help="Define macro replacement or override constant (e.g. -DNAME=VAL or -DNAME)",
        )
        parser.add_argument(
            "-O",
            dest="opt",
            nargs="?",
            const="1",
            default=None,
            metavar="LEVEL",
            help="Optimization level: -O (strip asserts, force __debug__=False), -OO or -O2 (strip docstrings), -O3 (full)",
        )
        parser.add_argument(
            "-OO",
            dest="opt_oo",
            action="store_true",
            help="Optimization level 2: strip assertions, __debug__=False, and docstrings",
        )
        parser.add_argument(
            "--test",
            action="store_true",
            help="Run self-contained unit test suite",
        )
        return parser

    parser = _build_parser()

    if "-h" in argv or "--help" in argv:
        parser.print_help(file=stdout)
        if hasattr(stdout, "flush"):
            stdout.flush()
        return 0

    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        c = getattr(e, "code", None)
        if c is None:
            c = e.args[0] if e.args else 0
        return c

    # --- Self-Contained Test Mode ---
    if args.test:
        if not __debug__:
            stderr.write("--test is not available in optimized builds\n")
            if hasattr(stderr, "flush"):
                stderr.flush()
            return 1
        try:
            import unittest
        except ImportError:
            stderr.write(
                "--test disabled: unittest is not available in this environment\n"
            )
            return 1

        global TestAstpp

        class TestAstpp(unittest.TestCase):
            """Self-test suite for astpp runtime helpers, inlining, and docstring examples."""

            def test_runtime_helpers(self):
                """Validate behavior of const, keep, omit, keep_if, omit_if, and micropython shims."""
                self.assertEqual(const(123), 123)
                self.assertEqual(keep(42), 42)
                self.assertIsNone(omit(42))
                self.assertEqual(keep_if(True)(99), 99)
                self.assertIsNone(keep_if(False)(99))
                self.assertIsNone(omit_if(True)(99))
                self.assertEqual(omit_if(False)(99), 99)
                self.assertEqual(micropython.native(10), 10)
                self.assertEqual(micropython.viper(20), 20)

            def test_docstring_examples(self):
                """Parse all documented examples directly from __doc__ and verify against astpp_main."""
                import io, re, shlex, sys

                doc = sys.modules[__name__].__doc__ or __doc__
                pattern = re.compile(
                    r"Input:\s*```python\s*(.*?)\s*```\s*Flags:\s*(.*?)\s*Output:\s*```python\s*(.*?)\s*```",
                    re.DOTALL,
                )
                matches = pattern.findall(doc)
                self.assertGreaterEqual(
                    len(matches), 5, "Must find at least 5 docstring examples"
                )

                for idx, (inp, flags_str, expected) in enumerate(matches):
                    flags = shlex.split(flags_str.strip())
                    fake_in = io.StringIO(inp.strip() + "\n")
                    fake_out = io.StringIO()
                    fake_err = io.StringIO()
                    rc = astpp_main(flags, fake_in, fake_out, fake_err)
                    self.assertEqual(
                        rc, 0, f"Example {idx+1} failed with rc={rc}: {flags}"
                    )

                    actual_ast = ast.parse(fake_out.getvalue().strip())
                    exp_ast = ast.parse(expected.strip())
                    self.assertEqual(
                        ast.dump(actual_ast),
                        ast.dump(exp_ast),
                        f"Example {idx+1} AST mismatch!\nActual:\n{fake_out.getvalue()}\nExpected:\n{expected}",
                    )

            def test_constant_expressions_and_pruning(self):
                """Validate constant comparisons (==, !=, <, etc.), boolean logic, and 'if not' pruning."""
                import io

                src = """
_FLAG = const(True)
_VAL = const(42)
PUBLIC_CONST = const(100)

if not _FLAG:
    dead_code_1 = 1

if _FLAG:
    live_code_1 = 2

if _VAL == 42:
    live_code_2 = 3

if _VAL != 42:
    dead_code_2 = 4

if _VAL > 100:
    dead_code_3 = 5
elif _VAL < 50:
    live_code_3 = 6
"""
                fake_in = io.StringIO(src)
                fake_out = io.StringIO()
                rc = astpp_main([], fake_in, fake_out, io.StringIO())
                self.assertEqual(rc, 0)
                out = fake_out.getvalue()
                self.assertNotIn("dead_code", out)
                self.assertIn("live_code_1 = 2", out)
                self.assertIn("live_code_2 = 3", out)
                self.assertIn("live_code_3 = 6", out)
                self.assertNotIn("_FLAG = const", out)
                self.assertNotIn("_VAL = const", out)
                self.assertIn("PUBLIC_CONST = const(100)", out)

            def test_empty_nested_blocks_receive_pass(self):
                """Validate that nested blocks stripped of all statements receive 'pass'."""
                import io

                src = """
if runtime_check():
    _STRIPPED = const(1)

def empty_after_assert():
    assert False

class EmptyClass:
    _INNER = const(2)
"""
                fake_in = io.StringIO(src)
                fake_out = io.StringIO()
                rc = astpp_main(["-O"], fake_in, fake_out, io.StringIO())
                self.assertEqual(rc, 0)
                out = fake_out.getvalue()
                self.assertIn("if runtime_check():\n    pass", out)
                self.assertIn("def empty_after_assert():\n    pass", out)
                self.assertIn("class EmptyClass:\n    pass", out)

            def test_omit_main_pruning(self):
                """Validate that -D_OMIT_MAIN=True drops @omit_if(_OMIT_MAIN) functions and if not _OMIT_MAIN blocks."""
                import io

                src = """
from astpp import const, omit_if
_OMIT_MAIN = const(False)

@omit_if(_OMIT_MAIN)
def main():
    return "cli_active"

if not _OMIT_MAIN:
    if __name__ == "__main__":
        main()
"""
                fake_in = io.StringIO(src)
                fake_out = io.StringIO()
                rc = astpp_main(["-D_OMIT_MAIN=True"], fake_in, fake_out, io.StringIO())
                self.assertEqual(rc, 0)
                out = fake_out.getvalue()
                self.assertNotIn("def main", out)
                self.assertNotIn('__name__ == "__main__"', out)
                self.assertNotIn("_OMIT_MAIN", out)

            def test_cli_help_flag(self):
                """Validate that --help writes formatted usage to stdout without process exit."""
                import io, re

                help_out = io.StringIO()
                rc = astpp_main(["--help"], stdout=help_out, stderr=io.StringIO())
                self.assertEqual(rc, 0)
                val = help_out.getvalue()
                clean_val = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", val)
                self.assertIn("usage: astpp", clean_val)
                self.assertIn("-D NAME=VALUE", clean_val)

        loader = getattr(unittest, "TestLoader", None)
        if loader is not None:
            suite = loader().loadTestsFromTestCase(TestAstpp)
            runner = unittest.TextTestRunner(stream=stderr, verbosity=2)
        else:
            suite = unittest.TestSuite()
            suite.addTest(TestAstpp)
            runner = unittest.TextTestRunner()
        res = runner.run(suite)
        return (
            0
            if (
                res.wasSuccessful()
                if hasattr(res, "wasSuccessful")
                else len(getattr(res, "errors", [])) == 0
            )
            else 1
        )

    def _parse_val(v_str):
        if v_str == "True":
            return True
        if v_str == "False":
            return False
        if v_str == "None":
            return None
        try:
            return int(v_str)
        except ValueError:
            return v_str

    defines = {}
    for item in args.defines:
        if "=" in item:
            k, v = item.split("=", 1)
            defines[k] = _parse_val(v)
        else:
            defines[item] = True

    opt_level = None
    if args.opt_oo:
        opt_level = 2
    elif args.opt is not None:
        opt_level = int(args.opt)

    if opt_level and opt_level >= 1:
        defines["__debug__"] = False

    if args.input == "-":
        in_source = stdin.read()
    else:
        with open(args.input, "r", encoding="utf-8") as f:
            in_source = f.read()

    # Self-parsing: extract live AST nodes preceding astpp_main directly from this source file
    with open(__file__, "r", encoding="utf-8") as f:
        self_tree = ast.parse(f.read(), filename=__file__)

    self_helpers = []
    for n in self_tree.body:
        if isinstance(n, ast.FunctionDef) and n.name == "astpp_main":
            break
        self_helpers.append(n)

    def _defined_names(node):
        defs = set()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    defs.add(t.id)
        elif isinstance(node, ast.Try):
            for n in node.body:
                defs.update(_defined_names(n))
            for h in node.handlers:
                for n in h.body:
                    defs.update(_defined_names(n))
        elif isinstance(node, ast.If):
            for n in node.body + node.orelse:
                defs.update(_defined_names(n))
        return defs

    _UNEVALUATED = object()

    class Preprocessor(ast.NodeTransformer):
        """AST transformer enforcing constant propagation, branch pruning, and inlining."""

        def __init__(self):
            self.constants = dict(defines)

        def _eval_const(self, node):
            """Evaluate constant expressions statically at compile time."""
            if isinstance(node, ast.Constant):
                return node.value
            if isinstance(node, ast.Name):
                if node.id in self.constants:
                    return self.constants[node.id]
                return _UNEVALUATED
            if isinstance(node, ast.UnaryOp):
                v = self._eval_const(node.operand)
                if v is not _UNEVALUATED:
                    try:
                        if isinstance(node.op, ast.Not):
                            return not v
                        if isinstance(node.op, ast.USub):
                            return -v
                        if isinstance(node.op, ast.UAdd):
                            return +v
                        if isinstance(node.op, ast.Invert):
                            return ~v
                    except Exception:
                        pass
                return _UNEVALUATED
            if isinstance(node, ast.BinOp):
                left = self._eval_const(node.left)
                right = self._eval_const(node.right)
                if left is not _UNEVALUATED and right is not _UNEVALUATED:
                    try:
                        if isinstance(node.op, ast.Add):
                            return left + right
                        if isinstance(node.op, ast.Sub):
                            return left - right
                        if isinstance(node.op, ast.Mult):
                            return left * right
                        if isinstance(node.op, ast.Div):
                            return left / right
                        if isinstance(node.op, ast.FloorDiv):
                            return left // right
                        if isinstance(node.op, ast.Mod):
                            return left % right
                        if isinstance(node.op, ast.Pow):
                            return left**right
                        if isinstance(node.op, ast.LShift):
                            return left << right
                        if isinstance(node.op, ast.RShift):
                            return left >> right
                        if isinstance(node.op, ast.BitOr):
                            return left | right
                        if isinstance(node.op, ast.BitXor):
                            return left ^ right
                        if isinstance(node.op, ast.BitAnd):
                            return left & right
                    except Exception:
                        pass
                return _UNEVALUATED
            if isinstance(node, ast.BoolOp):
                vals = [self._eval_const(v) for v in node.values]
                if isinstance(node.op, ast.And):
                    for v in vals:
                        if v is not _UNEVALUATED and not bool(v):
                            return False
                        if v is _UNEVALUATED:
                            return _UNEVALUATED
                    return vals[-1]
                elif isinstance(node.op, ast.Or):
                    for v in vals:
                        if v is not _UNEVALUATED and bool(v):
                            return True
                        if v is _UNEVALUATED:
                            return _UNEVALUATED
                    return vals[-1]
                return _UNEVALUATED
            if isinstance(node, ast.Compare):
                left = self._eval_const(node.left)
                if left is _UNEVALUATED:
                    return _UNEVALUATED
                cur = left
                for op, comp in zip(node.ops, node.comparators):
                    right = self._eval_const(comp)
                    if right is _UNEVALUATED:
                        return _UNEVALUATED
                    try:
                        if isinstance(op, ast.Eq):
                            matched = cur == right
                        elif isinstance(op, ast.NotEq):
                            matched = cur != right
                        elif isinstance(op, ast.Lt):
                            matched = cur < right
                        elif isinstance(op, ast.LtE):
                            matched = cur <= right
                        elif isinstance(op, ast.Gt):
                            matched = cur > right
                        elif isinstance(op, ast.GtE):
                            matched = cur >= right
                        elif isinstance(op, ast.Is):
                            matched = cur is right
                        elif isinstance(op, ast.IsNot):
                            matched = cur is not right
                        elif isinstance(op, ast.In):
                            matched = cur in right
                        elif isinstance(op, ast.NotIn):
                            matched = cur not in right
                        else:
                            return _UNEVALUATED
                    except Exception:
                        return _UNEVALUATED
                    if not matched:
                        return False
                    cur = right
                return True
            if isinstance(node, (ast.Tuple, ast.List)):
                items = [self._eval_const(e) for e in node.elts]
                if any(x is _UNEVALUATED for x in items):
                    return _UNEVALUATED
                return tuple(items) if isinstance(node, ast.Tuple) else list(items)
            return _UNEVALUATED

        def _strip_docstrings(self, body):
            """Strip docstring expression nodes when optimization level >= 2 (-OO / -O2)."""
            if opt_level and opt_level >= 2 and body:
                if (
                    isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)
                ):
                    body.pop(0)
                    if not body:
                        body.append(ast.Pass())

        def visit_Module(self, node):
            """Process top-level module statements, stripping astpp imports and inlining helpers."""
            self._strip_docstrings(node.body)
            new_body = []
            for stmt in node.body:
                if (
                    isinstance(stmt, ast.ImportFrom)
                    and stmt.module in ("astpp", "pypp")
                    and not stmt.level
                ):
                    names = {a.name for a in stmt.names}
                    needed = set(names)
                    if any(k in needed for k in ("keep_if", "omit_if")):
                        needed.update(["keep", "omit"])
                    if "micropython" in needed:
                        needed.update(["keep", "_real_mpy", "_MicroPythonShim"])

                    for helper in self_helpers:
                        h_defs = _defined_names(helper)
                        if needed.intersection(h_defs):
                            new_body.append(self.visit(copy.deepcopy(helper)))
                    continue

                transformed = self.visit(stmt)
                if transformed is not None:
                    if isinstance(transformed, list):
                        new_body.extend(transformed)
                    else:
                        new_body.append(transformed)
            node.body = [stmt for stmt in new_body if not isinstance(stmt, ast.Pass)]
            return node

        def visit_Assert(self, node):
            """Eliminate assert statements when optimization level >= 1 (-O / -O1)."""
            return None if (opt_level and opt_level >= 1) else node

        def _visit_stmts(self, stmts):
            res = []
            for stmt in stmts:
                t = self.visit(stmt)
                if t is not None:
                    if isinstance(t, list):
                        res.extend(t)
                    else:
                        res.append(t)
            return res

        def _ensure_non_empty(self, body):
            if isinstance(body, list) and len(body) == 0:
                body.append(ast.Pass())
            return body

        def generic_visit(self, node):
            had_orelse = isinstance(getattr(node, "orelse", None), list) and bool(
                node.orelse
            )
            had_final = isinstance(getattr(node, "finalbody", None), list) and bool(
                node.finalbody
            )

            super().generic_visit(node)

            # Ensure non-empty body for compound statement nodes (loops, with, try)
            if isinstance(
                node,
                (ast.While, ast.For, ast.AsyncFor, ast.With, ast.AsyncWith, ast.Try),
            ):
                if (
                    hasattr(node, "body")
                    and isinstance(node.body, list)
                    and len(node.body) == 0
                ):
                    node.body.append(ast.Pass())

            if hasattr(node, "handlers"):
                for h in node.handlers:
                    if (
                        hasattr(h, "body")
                        and isinstance(h.body, list)
                        and len(h.body) == 0
                    ):
                        h.body.append(ast.Pass())

            if isinstance(node, (ast.While, ast.For, ast.AsyncFor, ast.Try)):
                if (
                    had_orelse
                    and isinstance(getattr(node, "orelse", None), list)
                    and len(node.orelse) == 0
                ):
                    node.orelse.append(ast.Pass())

            if (
                had_final
                and isinstance(getattr(node, "finalbody", None), list)
                and len(node.finalbody) == 0
            ):
                node.finalbody.append(ast.Pass())

            return node

        def visit_Assign(self, node):
            """Harvest in-code const() definitions and apply -D constant overrides."""
            is_const = False
            for target in node.targets:
                if isinstance(target, ast.Name):
                    name = target.id
                    if name in defines:
                        val = defines[name]
                        self.constants[name] = val
                        node.value = ast.Call(
                            func=ast.Name(id="const", ctx=ast.Load()),
                            args=[ast.Constant(value=val)],
                            keywords=[],
                        )
                        is_const = True
                    elif (
                        isinstance(node.value, ast.Call)
                        and getattr(node.value.func, "id", None) == "const"
                    ):
                        val = self._eval_const(node.value.args[0])
                        if val is not _UNEVALUATED:
                            self.constants[name] = val
                        is_const = True

            if is_const:
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    if node.targets[0].id.startswith("_"):
                        return None
            return self.generic_visit(node)

        def visit_AnnAssign(self, node):
            if isinstance(node.target, ast.Name):
                name = node.target.id
                if name in defines:
                    val = defines[name]
                    self.constants[name] = val
                    if name.startswith("_"):
                        return None
                    node.value = ast.Call(
                        func=ast.Name(id="const", ctx=ast.Load()),
                        args=[ast.Constant(value=val)],
                        keywords=[],
                    )
                elif (
                    node.value is not None
                    and isinstance(node.value, ast.Call)
                    and getattr(node.value.func, "id", None) == "const"
                ):
                    val = self._eval_const(node.value.args[0])
                    if val is not _UNEVALUATED:
                        self.constants[name] = val
                    if name.startswith("_"):
                        return None
            return self.generic_visit(node)

        def visit_Name(self, node):
            """Inline known compile-time constants directly into load expressions."""
            if isinstance(node.ctx, ast.Load) and node.id in self.constants:
                val = self.constants[node.id]
                if isinstance(val, (bool, int, str, bytes, type(None))):
                    return ast.Constant(value=val)
            return node

        def visit_If(self, node):
            """Eliminate dead branches and promote reachable bodies for compile-time if/elif/else."""
            cond = self._eval_const(node.test)
            if cond is not _UNEVALUATED:
                if bool(cond):
                    return self._visit_stmts(node.body)
                else:
                    if not node.orelse:
                        return None
                    if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                        return self.visit(node.orelse[0])
                    return self._visit_stmts(node.orelse)

            # Condition cannot be statically evaluated: the if statement remains
            had_orelse = isinstance(getattr(node, "orelse", None), list) and bool(
                node.orelse
            )
            node.test = self.visit(node.test)
            node.body = self._visit_stmts(node.body)
            self._ensure_non_empty(node.body)

            if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                sub = self.visit(node.orelse[0])
                node.orelse = [sub] if sub is not None else []
            else:
                node.orelse = self._visit_stmts(node.orelse)
                if had_orelse and len(node.orelse) == 0:
                    node.orelse.append(ast.Pass())

            return node

        def _handle_def(self, node):
            """Handle conditional decorators (@keep, @omit, @keep_if, @omit_if, macro mappings)."""
            self._strip_docstrings(node.body)
            new_decs = []
            for d in node.decorator_list:
                dec_str = ast.unparse(d)
                if isinstance(d, ast.Call) and getattr(d.func, "id", None) in (
                    "keep_if",
                    "omit_if",
                ):
                    cond = self._eval_const(d.args[0])
                    if cond is not _UNEVALUATED:
                        if d.func.id == "omit_if":
                            cond = not cond
                        res = "keep" if bool(cond) else "omit"
                    else:
                        res = dec_str
                elif dec_str in ("keep", "omit"):
                    res = dec_str
                else:
                    res = defines.get(dec_str, dec_str)

                if res == "omit":
                    return None
                if res == "keep":
                    continue
                new_decs.append(d)
            node.decorator_list = new_decs
            if hasattr(node, "args"):
                node.args = self.visit(node.args)
            if hasattr(node, "returns") and node.returns:
                node.returns = self.visit(node.returns)
            node.body = self._visit_stmts(node.body)
            self._ensure_non_empty(node.body)
            return node

        def visit_FunctionDef(self, node):
            """Process function definitions, handling decorators and docstring stripping."""
            return self._handle_def(node)

        def visit_AsyncFunctionDef(self, node):
            """Process async function definitions, handling decorators and docstring stripping."""
            return self._handle_def(node)

        def visit_ClassDef(self, node):
            """Process class definitions, handling decorators and docstring stripping."""
            return self._handle_def(node)

    tree = ast.parse(in_source)
    out_text = ast.unparse(Preprocessor().visit(tree))
    if not out_text.endswith("\n"):
        out_text += "\n"

    if args.output == "-":
        stdout.write(out_text)
        if hasattr(stdout, "flush"):
            stdout.flush()
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out_text)

    return 0


@omit_if(_OMIT_MAIN)
def main():
    """Command-line entrypoint for pip-installed astpp console script."""
    import sys

    sys.exit(astpp_main(sys.argv[1:], sys.stdin, sys.stdout, sys.stderr))


if not _OMIT_MAIN:
    if __name__ == "__main__":
        main()
