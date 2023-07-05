# Coq model, refinement and update of Giskard consensus protocol specification

This directory contains:

- the original [Coq](https://coq.inria.fr) definitions from the [Giskard model 1.1](https://github.com/runtimeverification/giskard-verification) including node transition relations (`lib.v`, `structures.v`, `local_orig.v`)
- refinement of the original node transition relations to executable Coq code (`refinement_orig.v`)
- updated Coq node transition relations (`local_updated.v`)
- refinement of updated node transition relations to executable Coq code (`refinement_updated.v`)

## License

The code is released under
[The University of Illinois/NCSA Open Source License](https://opensource.org/license/uoi-ncsa-php/),
which is the same license as for the
[Giskard model 1.1](https://github.com/runtimeverification/giskard-verification).

## Dependencies and checking instructions

The Coq code depends on the
[Coq Record Update](https://github.com/tchajed/coq-record-update)
library, and has been tested with Coq 8.16.1 and 8.17.1.

We recommend installing Coq 8.16 and Coq Record Update
via [opam](http://opam.ocaml.org/doc/Install.html):
```shell
opam repo add coq-released https://coq.inria.fr/opam/released
opam install coq.8.16.1 coq-record-update
```

To check the Coq code after all dependencies are installed, run:
```shell
make
```

## Files

- `lib.v` (from [Giskard model 1.1](https://github.com/runtimeverification/giskard-verification/releases/tag/v1.1)): supplementary general tactics and results
- `structures.v` (from [Giskard model 1.1](https://github.com/runtimeverification/giskard-verification/releases/tag/v1.1)): definitions of Giskard datatypes and axioms
- `local_orig.v` (from [Giskard model 1.1](https://github.com/runtimeverification/giskard-verification/releases/tag/v1.1)): local state operations, properties, and transitions
- `refinement_orig.v`: refinement of node transition relations from `local_orig.v` to executable functions, including correctness proofs
- `local_updated.v`: updated local state operations, properties, and transitions
- `refinement_updated.v`: refinement of node transition relations from `local_updated.v` to executable functions, including correctness proofs
