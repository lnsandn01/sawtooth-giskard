# Coq refinement of Giskard node transition relations to functions

This directory contains a refinement of Giskard node transition relations from
the [formal Giskard model](https://github.com/runtimeverification/giskard-verification)
to functions in [Coq](https://coq.inria.fr). Refinement proofs are included.

## Dependencies

The refinement code depends on the
[Giskard model 1.1](https://github.com/runtimeverification/giskard-verification/releases/tag/v1.1)
and the [Coq Record Update](https://github.com/tchajed/coq-record-update)
library, and has been tested with Coq 8.16.1 and 8.17.0.

## Checking instructions

We recommend installing Coq 8.16, the Giskard model, and Coq Record Update
via [opam](http://opam.ocaml.org/doc/Install.html):
```shell
opam repo add coq-released https://coq.inria.fr/opam/released
opam install coq.8.16.1 coq-giskard.1.1 coq-record-update
```

To check the code after all dependencies are installed, run:
```shell
coqc refinement.v
```
