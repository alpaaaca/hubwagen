# Spike S2: code version from imports

Issue [#11](https://github.com/alpaaaca/hubwagen/issues/11). The result is
built into the SDK (`sdk/python/src/hubwagen/_codeversion.py`); the cases
below are `sdk/python/tests/test_codeversion.py`. Measured on an Apple M4,
Python 3.12–3.14.

## Questions and answers

### Can the SDK resolve a lift's project-local imports reliably through the AST?

Not through the AST of import statements alone, and it doesn't need to.
Parsing `import` lines means re-implementing what Python already did when
the worker imported the project: relative imports, `from pkg import sub`
(a submodule or an attribute?), namespace packages, `sys.path` order,
imports inside functions. Each is a place to get it subtly wrong, and a
wrong answer is a wrong reuse.

The worker has already imported everything, so the SDK **walks the live
objects** instead:

1. Start at the lift's function. Its code object (and the code nested in
   it) names every global and attribute it looks up (`co_names`).
2. Each name that resolves in the function's globals, or in `sys.modules`
   for imports inside the function, is followed:
   - a project function: its AST, then the same walk from it;
   - a project class: its AST, its bases and its methods;
   - a project module (`helpers.clean(x)`): the attributes the code names;
   - a constant (numbers, strings, containers of them, paths relative to
     the project root): its value;
   - anything else outside the project: the installed distribution that
     provides it (below).
3. Closures, default arguments and parameter annotations are followed too.
   Annotations decide how inputs are loaded (`pd.DataFrame`), so they count.
4. A literal module name in the code (`importlib.import_module("app.x")`)
   is followed if that project module is imported.

Each piece of code is hashed as its AST, without docstrings, so comments
and formatting don't count. For the lift itself, the decorator is dropped
too: `@hw.lift(timeout=30)` isn't code. "Project" means any file outside
the standard library, `site-packages` and the SDK itself, compared by real
path, as in S1.

| Change (in a small project) | Version changes for | Expected |
|---|---|---|
| Nothing; the same project in another directory | none | ✓ |
| Comments, docstrings, line breaks, `@hw.lift(...)` arguments | none | ✓ |
| The lift's body | that lift | ✓ |
| A helper in the same module | lifts (and hauls) calling it | ✓ |
| An unused function in the same or another module | none | ✓ |
| A constant a helper in another module reads (`from .helpers import clean`) | that lift only | ✓ |
| A function two modules away (`helpers.twice` → `deep.inner`) | lifts reaching it, incl. through a literal `import_module` | ✓ |
| A dict constant, a class method, a module in a namespace package, a project path constant | the lift using it | ✓ |
| A file under `deps=` (changed or added) | that lift | ✓ |
| A lift's body, for the haul calling it | not the haul (lifts count by name in hauls) | ✓ |

Function-level precision matters for the demo's story: in
`pipelines/examples/revenue`, changing `daily_revenue` reruns only that
lift and, through its new content hash, the report. Hashing whole modules
would rerun all three.

**Cost:** 13 lifts and 11 hauls (the test projects, importing pandas,
Polars and pyarrow) are versioned in 45 ms at worker start, once per
process.

**Blind spots**, documented in the SDK README with `version=` and `deps=`
as the answers:

- code reached through a computed name (`import_module(name)`,
  `getattr(module, name)`);
- data read at import time (a config file loaded into a module global),
  and the state of objects (an instance's attributes);
- C extensions in the project, and changes to the Python environment that
  no imported distribution's version shows.

### How often does the lockfile hash cause reruns?

There isn't enough Python history on hand to measure it over weeks: the
local projects have 1 to 21 commits and at most one lockfile change. A
proxy, this repository's `go.sum`, changed in 4 of 31 commits touching Go
code over two months, about one commit in eight.

The measurement matters less than the shape of the cost. A lockfile is one
hash over **every** dependency of the project, dev tools included. Adding
`ruff`, bumping `pytest` or a transitive dependency of the web framework
would rerun every memoized lift in the project, and a backfill after it
would recompute everything. That makes the bias toward strictness
expensive in exactly the case where nothing that matters changed.

**Instead:** per lift, the versions of the distributions its code actually
uses, found by the same walk (`importlib.metadata.packages_distributions`
maps a module to its distribution), plus what each requires, recursively
(pandas brings numpy, python-dateutil, six, tzdata). A pandas bump reruns
the lifts that use pandas; a pytest bump reruns nothing. The standard
library counts as the Python version (`cpython 3.12`). hubwagen itself
doesn't count: it runs lifts, like the server, and a release that changed
outputs would say so.

This doesn't need a lockfile at all, so it works the same with uv, Poetry,
pip or a bare virtualenv, and it reads what is installed, not what a file
says should be.

## Found along the way

- **The build ID ignored haul code.** It hashed haul names and lift
  versions, so a fix to a haul's code alone produced the same build ID,
  `RegisterBuild` was a no-op, and the stuck hauls it was meant to resume
  stayed stuck. Hauls now have code versions too (the same walk, with lifts
  counted by name), and the build ID covers them.
- **Absolute paths leak the machine.** A module constant like
  `Path(__file__).parent / "data.csv"` differs per checkout; hashed as is,
  two workers on two machines would never share a build. Paths inside the
  project root are hashed relative to it.

## Recommendation (built in #25)

1. Code version = AST hashes of the code reachable from the lift through
   live objects + versions of the distributions that code uses (and their
   requirements) + the Python version + `deps=` files. `version=` replaces
   all of it.
2. No lockfile term.
3. Hauls get code versions the same way; the build ID covers hauls and
   lifts.
4. `python -m hubwagen.worker --versions` prints what each version covers,
   so "code changed" can be traced to a piece of code or a package.
