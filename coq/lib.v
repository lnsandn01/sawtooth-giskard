(*
Copyright (c) 2020 Giskard Verification Team. All Rights Reserved.

Developed by: Runtime Verification, Inc.

University of Illinois/NCSA
Open Source License

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal with
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

* Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimers.

* Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimers in the
      documentation and/or other materials provided with the distribution.

* Neither the names of the Giskard Verification Team,
      Runtime Verification, Inc., nor the names of
      its contributors may be used to endorse or promote products derived from
      this Software without specific prior written permission.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE
SOFTWARE.
*)
From Coq Require Import Arith List.

Import ListNotations.

Set Implicit Arguments.

(** * Supplementary general tactics and results *)

Tactic Notation "spec" hyp(H) := 
  match type of H with ?a -> _ => 
  let H1 := fresh in (assert (H1: a); 
  [|generalize (H H1); clear H H1; intro H]) end.
Tactic Notation "spec" hyp(H) constr(a) := 
  (generalize (H a); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) := 
  (generalize (H a b); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) constr(c) := 
  (generalize (H a b c); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) constr(c) constr(d) := 
  (generalize (H a b c d); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) constr(c) constr(d) constr(e) := 
  (generalize (H a b c d e); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) constr(c) constr(d) constr(e) constr(f) := 
  (generalize (H a b c d e f); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) constr(c) constr(d) constr(e) constr(f) constr (g):=  
  (generalize (H a b c d e f g); clear H; intro H).
Tactic Notation "spec" hyp(H) constr(a) constr(b) constr(c) constr(d) constr(e) constr(f) constr (g) constr(h) :=  
  (generalize (H a b c d e f g h); clear H; intro H).

Lemma remove_subset :
  forall (A : Type) (eq_dec : forall x y : A, {x = y} + {x <> y}) (l : list A) (x1 x2 : A),
    In x1 (remove eq_dec x2 l) ->
    In x1 l. 
Proof.
  intros A eq_dec l x1 x2 H_in.
  induction l as [|hd tl IHl].
  inversion H_in. 
  simpl in H_in.
  destruct (eq_dec x2 hd). subst.
  spec IHl H_in. right. assumption.
  simpl in H_in. destruct H_in. subst. left; reflexivity.
  right. now apply IHl.
Qed.

Theorem modus_tollens : forall (P Q : Prop), (P -> Q) -> (~ Q -> ~ P).
Proof.
  intros P Q HPQ HQ.
  intro HP.
  contradict HQ.
  apply HPQ.
  assumption.
Qed.

Lemma le_S_disj :
  forall n m : nat,
    n <= S m -> n <= m \/ n = S m.
Proof.
  intros.
  inversion H.
  right; reflexivity.
  left; exact H1.  
Qed.
