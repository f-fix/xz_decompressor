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
_X = const(True)
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


# ==============================================================================
# 2. PREPROCESSOR ENTRYPOINT & SELF-TEST SUITE
# ==============================================================================
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

    # --- Self-Contained Test Mode (Compiled away when __debug__ is False) ---
    if __debug__:
        if "--test" in argv:
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

    class Preprocessor(ast.NodeTransformer):
        """AST transformer enforcing constant propagation, branch pruning, and inlining."""

        def __init__(self):
            self.constants = dict(defines)

        def _eval_const(self, node):
            """Evaluate constant expressions statically at compile time."""
            if isinstance(node, ast.Constant):
                return node.value
            if isinstance(node, ast.Name):
                return self.constants.get(node.id, None)
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
                v = self._eval_const(node.operand)
                return (not v) if v is not None else None
            return None

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
            node.body = new_body
            return node

        def visit_Assert(self, node):
            """Eliminate assert statements when optimization level >= 1 (-O / -O1)."""
            return None if (opt_level and opt_level >= 1) else node

        def visit_Assign(self, node):
            """Harvest in-code const() definitions and apply -D constant overrides."""
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
                    elif (
                        isinstance(node.value, ast.Call)
                        and getattr(node.value.func, "id", None) == "const"
                    ):
                        val = self._eval_const(node.value.args[0])
                        if val is not None:
                            self.constants[name] = val
            return self.generic_visit(node)

        def visit_Name(self, node):
            """Inline known compile-time constants directly into load expressions."""
            if (
                opt_level
                and opt_level >= 1
                and isinstance(node.ctx, ast.Load)
                and node.id in self.constants
            ):
                val = self.constants[node.id]
                if isinstance(val, (bool, int, str, bytes, type(None))):
                    return ast.Constant(value=val)
            return node

        def visit_If(self, node):
            """Eliminate dead branches and promote reachable bodies for compile-time if/elif/else."""
            self.generic_visit(node)
            cond = self._eval_const(node.test)
            if cond is True:
                node.orelse = []
                return node.body
            if cond is False:
                if not node.orelse:
                    return None
                if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
                    return node.orelse[0]
                return node.orelse
            if (
                len(node.orelse) == 1
                and isinstance(node.orelse[0], ast.If)
                and self._eval_const(node.orelse[0].test) is False
            ):
                node.orelse = node.orelse[0].orelse
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
                    if d.func.id == "omit_if":
                        cond = not cond
                    res = "keep" if cond else "omit"
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
            return self.generic_visit(node)

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


if __name__ == "__main__":
    import sys

    sys.exit(astpp_main(sys.argv[1:], sys.stdin, sys.stdout, sys.stderr))
