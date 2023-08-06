(*
Copyright (c) 2023 Karl Palmskog, Leon Sandner. All Rights Reserved.

Developed by: Karl Palmskog, Leon Sandner

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

* Neither the names of the Giskard Verification Team, Karl Palmskog,
      Leon Sandner, Runtime Verification, Inc., nor the names of
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
From Coq Require Import Arith Bool List Lia.
From GiskardUpdated Require Import structures local_updated.
From RecordUpdate Require Import RecordSet.

Import ListNotations.
Import RecordSetNotations.

Set Implicit Arguments.

(* BEGIN UTILITY *)

Lemma before_refl : forall A (A_eq_dec : forall (a a' : A), {a = a'}+{a <> a'})
 (x : A) l, In x l -> before x x l.
Proof.
intros A A_eq_dec.
induction l; simpl; auto.
intros [Ha|Hin].
- left; assumption.
- destruct (A_eq_dec a x).
  * left; assumption.
  * right.
    specialize (IHl Hin).
    split; assumption.
Qed.

Lemma before_antisymmetric : forall A (x y : A) l,
 before x y l -> before y x l -> x = y.
Proof.
intros; induction l; simpl in *; intuition; congruence.
Qed.

Lemma find_before : forall A (A_eq_dec : forall (a a' : A), {a = a'}+{a <> a'})
 (f : A -> bool) (x : A) l, 
 find f l = Some x ->
 In x l /\ f x = true /\ (forall y, In y l -> f y = true -> before x y l).
Proof.
intros A A_eq_dec f.
induction l; simpl; [congruence|].
simpl; case_eq (f a).
- intros Hfa Ha.
  inversion Ha; subst.
  split; [left; reflexivity|].
  split; [assumption|].
  intros; left; reflexivity.    
- intros Hfa Ha.
  specialize (IHl Ha).
  destruct IHl as [Hx [Hfx Hb]].
  split; [right;assumption|].
  split; [assumption|].
  intros y Hy Hfy.
  destruct (A_eq_dec a y); [congruence|].
  right.
  split; [assumption|].
  destruct Hy; [congruence|].    
  apply Hb; assumption.
Qed.

Lemma message_view_block_eqb : forall mt msg view b,
 (message_type_eqb (get_message_type msg) mt &&
  (Nat.eqb (get_view msg) view) && block_eqb (get_block msg) b = true) <->
 (get_view msg = view /\ get_block msg = b /\ get_message_type msg = mt).
Proof.
split.
- intros Hand. 
  apply andb_true_iff in Hand.
  destruct Hand as [Hand1 Hand2].
  apply andb_true_iff in Hand1.
  destruct Hand1 as [Hand11 Hand12].
  apply Nat.eqb_eq in Hand12.
  apply block_eqb_correct in Hand2.
  apply message_type_eqb_correct in Hand11.
  tauto.
- intros [Hg1 [Hg2 Hg3]].
  apply andb_true_iff.
  split; [|apply block_eqb_correct; assumption].
  apply andb_true_iff.
  split.
  * apply message_type_eqb_correct; assumption.
  * apply Nat.eqb_eq; assumption.
Qed.

Lemma prepare_stage_in_view_eq : forall (s : NState) (view : nat) (b : block),
 prepare_stage_in_view s view b <-> prepare_stage_in_viewb s view b = true.
Proof.
intros s view b; split; unfold prepare_stage_in_view, prepare_stage_in_viewb,
 vote_quorum_in_view, PrepareQC_in_view, quorum, quorumb, has_at_least_two_thirds.
- intros [Hh|Hex].
  * apply orb_true_iff; left; assumption.
  * apply orb_true_iff; right.
    destruct Hex as [msg [Hin [Hv [Hb Hm]]]].
    apply existsb_exists; exists msg.
    split; [assumption|].
    apply message_view_block_eqb; tauto.
- intros Hbe.
  apply orb_true_iff in Hbe.
  destruct Hbe as [Hb|He]; [left; assumption|].
  right.
  apply existsb_exists in He.
  destruct He as [msg [Hin Hm]].
  exists msg; split; [assumption|].
  apply message_view_block_eqb in Hm; tauto.
Qed.

(* END UTILITY *)

(* BEGIN BOILERPLATE *)

#[export] Instance message_Settable : Settable _ :=
  settable! mkMessage <get_message_type;get_view;get_sender;get_block;get_piggyback_block>.

#[export] Instance NState_Settable : Settable _ :=
  settable! mkNState <node_view;node_id;in_messages;counting_messages;out_messages;timeout>.

(* END BOILERPLATE *)

(* BEGIN BASIC STATE FUNCTIONS *)

Definition record_set (s : NState) (m : message) : NState :=
 s <| out_messages := m :: s.(out_messages) |>.

Definition record_plural_set (s : NState) (lm : list message) : NState :=
 s <| out_messages := lm ++ s.(out_messages) |>.

Definition add_set (s : NState) (m : message) : NState :=
 s <| in_messages := m :: s.(in_messages) |>.

Definition add_plural_set (s : NState) (lm : list message) : NState :=
 s <| in_messages := lm ++ s.(in_messages) |>.

Definition discard_set (s : NState) (m : message) : NState :=
 s <| in_messages := remove message_eq_dec m s.(in_messages) |>.

Definition process_set (s : NState) (msg : message) : NState :=
 s <| in_messages := remove message_eq_dec msg s.(in_messages) |>
   <| counting_messages := msg :: s.(counting_messages) |>.

Definition increment_view_set (s : NState) : NState :=
 s <| node_view := S (node_view s) |>
   <| in_messages := [] |>
   <| timeout := false |>.

Definition flip_timeout_set (s : NState) : NState :=
 s <| timeout := true |>.

Definition get_vote_quorum_msg_in_view (s : NState) (view : nat) (b : block) : option message :=
 if quorumb (processed_PrepareVote_in_view_about_block s view b) then
   final (processed_PrepareVote_in_view_about_block s view b)
 else None.

Definition get_quorum_msg_in_view (s : NState) (view : nat) (b : block) : option message :=
 match
   find (fun msg => message_type_eqb (get_message_type msg) PrepareQC
                 && Nat.eqb (get_view msg) view
                 && block_eqb (get_block msg) b)
     (counting_messages s) with
 | Some msg => Some msg
 | None => get_vote_quorum_msg_in_view s view b
 end.

Fixpoint prepare_stage_in_view_quorum_msg (s : NState) (b : block) (view : nat)
 : option message :=
 if prepare_stage_in_viewb s view b then
  get_quorum_msg_in_view s view b
 else 
  match view with
  | S n => prepare_stage_in_view_quorum_msg s b n
  | 0 => None
  end.

Definition get_quorum_msg_for_block (s : NState) (b : block) : option message :=
match block_eq_dec GenesisBlock b with
| left Hdec => Some (GenesisBlock_message s)
| right Hdec => prepare_stage_in_view_quorum_msg s b (node_view s)
end.

(* END BASIC STATE FUNCTIONS *)

(* BEGIN STATE UPDATE FUNCTIONS *)

Definition propose_block_init_set (s : NState) (msg : message)
 : NState * list message :=
 let lm := make_PrepareBlocks s (GenesisBlock_message s) in
 let s' := s <| out_messages := lm ++ s.(out_messages) |> in
 (s', lm).

Definition process_timeout_set (s : NState) (msg : message)
 : NState * list message :=
 let lm := [make_ViewChange s; highest_prepare_block_message s] in
 let s' := s <| out_messages := lm ++ s.(out_messages) |> in
 (s', lm).

Definition discard_view_invalid_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s <| in_messages := remove message_eq_dec msg s.(in_messages) |> in
 (s', []).

Definition process_PrepareBlock_duplicate_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s <| in_messages := remove message_eq_dec msg s.(in_messages) |> in
 (s', []).

Definition process_PrepareBlock_pending_vote_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
 in (s', []).

Definition process_PrepareBlock_vote_set (s : NState) (msg : message)
 : option (NState * list message) :=
 match get_quorum_msg_for_block s (parent_of (get_block msg)) with
 | Some quorum_msg =>
   let s' := s
    <| in_messages := remove message_eq_dec msg s.(in_messages) |>
    <| counting_messages := msg :: s.(counting_messages) |>
   in
   let lm := pending_PrepareVote s' quorum_msg in
   let s'' := s' <| out_messages := lm ++ s'.(out_messages) |>
   in Some (s'', lm)
 | None => None
 end.

Definition process_PrepareVote_wait_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
 in (s', []).

Definition process_PrepareVote_vote_set (s : NState) (msg : message)
 : NState * list message :=
  let lm_prime :=
    if negb (prepare_qc_already_sent s (get_block msg)) then
      make_PrepareQC s msg :: pending_PrepareVote s msg
    else
      pending_PrepareVote s msg
  in
  let s' := s
    <| in_messages := remove message_eq_dec msg s.(in_messages) |>
    <| counting_messages := msg :: s.(counting_messages) |>
    <| out_messages := lm_prime ++ s.(out_messages) |>
 in (s', lm_prime).

Definition process_PrepareQC_last_block_new_proposer_set (s : NState) (msg : message)
 : option (NState * list message) :=
 match get_quorum_msg_for_block s (parent_of (get_block msg)) with
 | Some quorum_msg =>
   let s' := s
    <| in_messages := remove message_eq_dec msg s.(in_messages) |>
    <| counting_messages := msg :: s.(counting_messages) |>
    <| node_view := S (node_view s) |>
   in
   let lm := make_PrepareBlocks s' quorum_msg in
   let s'' := s'
    <| in_messages := [] |>
    <| timeout := false |>
    <| out_messages := lm ++ s.(out_messages) |>
   in Some (s'', lm)
 | None => None
 end.

Definition process_PrepareQC_last_block_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
   <| in_messages := remove message_eq_dec msg s.(in_messages) |>
   <| counting_messages := msg :: s.(counting_messages) |>
   <| node_view := S (node_view s) |>
   <| in_messages := [] |>
   <| timeout := false |>
 in (s', []).

Definition process_PrepareQC_non_last_block_set (s : NState) (msg : message)
 : NState * list message :=
 let lm := pending_PrepareVote s msg in
 let s' := s
   <| out_messages := lm ++ s.(out_messages) |>
   <| in_messages := remove message_eq_dec msg s.(in_messages) |>
   <| counting_messages := msg :: s.(counting_messages) |>
 in (s', lm).

Definition process_ViewChange_quorum_new_proposer_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
   <| in_messages := remove message_eq_dec msg s.(in_messages) |>
   <| counting_messages := msg :: s.(counting_messages) |>
 in
 let msg_vc :=
   highest_ViewChange_message s'
 in
 let lm :=
   mkMessage PrepareQC (node_view s) (node_id s) (get_block msg_vc) GenesisBlock ::
     make_ViewChangeQC s msg_vc :: make_PrepareBlocks (increment_view s) msg_vc
 in
 let msg_pr :=
   mkMessage PrepareQC (get_view msg) (get_sender msg) (get_block msg_vc) GenesisBlock
 in
 let s'' := s'
   <| counting_messages := msg_pr :: s'.(counting_messages) |>
   <| node_view := S (node_view s') |>
   <| in_messages := [] |>
   <| timeout := false |>
   <| out_messages := lm ++ s'.(out_messages) |>
 in (s'', lm).

Definition process_ViewChange_quorum_not_new_proposer_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
   <| in_messages := remove message_eq_dec msg s.(in_messages) |>
   <| counting_messages := msg :: s.(counting_messages) |>
 in
 let msg_vc := highest_ViewChange_message s' in
 let lm_prime :=
   if negb (prepare_qc_already_sent s (get_block msg_vc)) then
     [make_PrepareQC s msg; make_ViewChangeQC s' msg_vc]
   else
     [make_ViewChangeQC s' msg_vc]
 in
 let s'' := s' <| out_messages := lm_prime ++ s'.(out_messages) |>
 in (s'', lm_prime).

Definition process_ViewChange_pre_quorum_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
 in (s', []).

Definition process_ViewChangeQC_single_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
  <| counting_messages := msg :: s.(counting_messages) |>
  <| node_view := S (node_view s) |>
  <| in_messages := [] |>
  <| timeout := false |>
 in (s', []).

Definition malicious_ignore_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s <| in_messages := remove message_eq_dec msg s.(in_messages) |> in
 (s', []).

Definition process_PrepareBlock_malicious_vote_set (s : NState) (msg : message)
 : option (NState * list message) :=
 match get_quorum_msg_for_block s (parent_of (get_block msg)) with
 | Some quorum_msg =>
   let s' := s
     <| in_messages := remove message_eq_dec msg s.(in_messages) |>
     <| counting_messages := msg :: s.(counting_messages) |>
   in
   let lm := pending_PrepareVote_malicious s' quorum_msg in
   let s'' := s' <| out_messages := lm ++ s.(out_messages) |>
   in Some (s'', lm)
 | None => None
 end.

(* END STATE UPDATE FUNCTIONS *)

(* BEGIN REFINEMENT PROOFS *)

Lemma record_set_eq : forall s m,
 record s m = record_set s m.
Proof. reflexivity. Qed.

Lemma record_plural_set_eq : forall s lm,
 record_plural s lm = record_plural_set s lm.
Proof. reflexivity. Qed.

Lemma add_set_eq : forall s m,
 add s m = add_set s m.
Proof. reflexivity. Qed.

Lemma add_plural_set_eq : forall s lm,
 add_plural s lm = add_plural_set s lm.
Proof. reflexivity. Qed.

Lemma discard_set_eq : forall s m,
 discard s m = discard_set s m.
Proof. reflexivity. Qed.

Lemma process_set_eq : forall s msg,
 process s msg = process_set s msg.
Proof. reflexivity. Qed.

Lemma increment_view_set_eq : forall s,
 increment_view s = increment_view_set s.
Proof. reflexivity. Qed.

Lemma flip_timeout_set_eq : forall s,
 flip_timeout s = flip_timeout_set s.
Proof. reflexivity. Qed.

Lemma get_vote_quorum_msg_in_view_eq : forall s view b msg,
 vote_quorum_msg_in_view s view b msg <-> (get_vote_quorum_msg_in_view s view b = Some msg).
Proof.
 split; unfold get_vote_quorum_msg_in_view, vote_quorum_msg_in_view,
  quorum, quorumb, has_at_least_two_thirds.
 - intros [Heq Hf].
   rewrite Heq; assumption.
 - case_eq (has_at_least_two_thirdsb
    (map get_sender (processed_PrepareVote_in_view_about_block s view b))).
   * intros Hb Hf.
     split; [reflexivity|assumption].
   * intros Hb Hs.
     congruence.
Qed.

Lemma get_quorum_msg_in_view_eq : forall s view b msg,
 (quorum_msg_in_view s view b msg <-> get_quorum_msg_in_view s view b = Some msg).
Proof.
 intros s view b msg.
 unfold quorum_msg_in_view, get_quorum_msg_in_view.
 set (fm := find _ (counting_messages s)).
 split.
 - intros [Hp|Hnp].
   * case_eq fm.
     + intros msg' Hfm.
       apply find_before in Hfm; [|apply message_eq_dec].
       cut (msg' = msg); [intro H; rewrite H; reflexivity|].
       destruct Hfm as [Hin [Hb Hbef]].
       destruct Hp as [Hin' [Hv [Hb' [Hm Hbef']]]].
       apply before_antisymmetric with (l := counting_messages s).
       -- apply Hbef; [assumption|].
          apply message_view_block_eqb.
          tauto.
       -- apply message_view_block_eqb in Hb.
          apply Hbef'; tauto.
     + intros Hfm.
       pose proof (find_none _ _ Hfm) as Hn.
       destruct Hp as [Hin' [Hv [Hb' [Hm Hbef']]]].
       specialize (Hn _ Hin').
       simpl in Hn.
       assert (get_view msg = view /\ get_block msg = b /\
                 get_message_type msg = PrepareQC) as Hb by tauto.
       apply message_view_block_eqb in Hb.
       congruence.
   * destruct Hnp as [Hp Hv].
     case_eq fm.
     + intros msg' Hfm.
       apply find_some in Hfm.
       destruct Hfm as [Hin Hb].
       apply message_view_block_eqb in Hb.
       contradict Hp.
       exists msg'; tauto.
     + intros Hfm.
       apply get_vote_quorum_msg_in_view_eq.
       assumption.
 - case_eq fm.
   * intros msg' Hfm Hm.
     inversion Hm; subst.
     left.
     apply find_before in Hfm; [|apply message_eq_dec].
     destruct Hfm as [Hin [Hm' Hy]].
     apply message_view_block_eqb in Hm'.
     split; [assumption|].
     split; [tauto|].
     split; [tauto|].
     split; [tauto|].
     intros msg' Hmsg' Hg Hb Ht.
     apply Hy; [assumption|].
     apply message_view_block_eqb; tauto.
   * intros Hfm Hv.
     right.
     apply get_vote_quorum_msg_in_view_eq in Hv.
     split; [|assumption].
     intros [msg' [Hm' Hp]].
     apply message_view_block_eqb in Hp.
     pose proof (find_none _ _ Hfm msg' Hm') as Hb.
     simpl in Hb.
     congruence.
Qed.

Lemma prepare_stage_in_view_quorum_msg_eq : forall n s b msg,
 (exists v_prime, v_prime <= n /\ prepare_stage_in_view s v_prime b /\
   (forall v0 : nat, v_prime < v0 <= n -> ~ prepare_stage_in_view s v0 b) /\
   quorum_msg_in_view s v_prime b msg) <->
  prepare_stage_in_view_quorum_msg s b n = Some msg.
Proof.
induction n; simpl.
- intros s b msg.
  split; intros Hq.
  * destruct Hq as [v_prime [Hp [Hv [Hnp Hq]]]].
    assert (v_prime = 0) by lia.
    subst.
    case_eq (prepare_stage_in_viewb s 0 b).
    + intros Hpv.
      apply get_quorum_msg_in_view_eq.
      assumption.
    + intros Hpv.
      apply prepare_stage_in_view_eq in Hv.
      congruence.
  * revert Hq.
    case_eq (prepare_stage_in_viewb s 0 b).
    + intros Hp Hq.
      exists 0; split; [lia|].
      apply prepare_stage_in_view_eq in Hp.
      split; [assumption|].
      apply get_quorum_msg_in_view_eq in Hq.
      split; [|assumption].
      intros v0 Hv.
      lia.
    + intros Hp Hm; congruence.
- intros s b msg.
  split; intros Hq.
  * case_eq (prepare_stage_in_viewb s (S n) b); intros Hp.
    + apply prepare_stage_in_view_eq in Hp.
      destruct Hq as [v' [Hv' [Hp' [Hnp' Hq]]]].
      assert (~ v' < S n) as Hlt. {
        intros Hvs.
        assert (v' < S n <= S n) as Hlt by lia.
        specialize (Hnp' _ Hlt).
        contradiction.
      }
      assert (v' = S n) as Heq by lia.
      subst.
      apply get_quorum_msg_in_view_eq; assumption.
    + assert (~ prepare_stage_in_view s (S n) b) as Hsp'. {
        intros Hp'.
        apply prepare_stage_in_view_eq in Hp'.
        congruence.
      }
      destruct Hq as [v' [Hv' [Hp' [Hnp' Hq]]]].
      assert (v' <= n) as Hvn. {
        cut (v' <> S n). { lia. }
        intros Hvs'.
        subst.
        contradiction.
      }
      apply IHn; exists v'; split; [assumption|].
      split; [assumption|].
      split; [|assumption].
      intros v0 Hv0.
      apply Hnp'.
      lia.
  * revert Hq.
    case_eq (prepare_stage_in_viewb s (S n) b); intros Hp Hq.
    + exists (S n); split; [lia|].
      apply prepare_stage_in_view_eq in Hp.
      apply get_quorum_msg_in_view_eq in Hq.
      split; [assumption|].
      split; [|assumption].
      intros v0 Hlt.
      lia.
    + apply IHn in Hq.
      assert (~ prepare_stage_in_view s (S n) b) as Hsp'. {
        intros Hp'.
        apply prepare_stage_in_view_eq in Hp'.
        congruence.
      }
      destruct Hq as [v' [Hv' [Hp' [Hnp' Hq]]]].
      exists v'; split; [lia|].
      split; [assumption|].
      split; [|assumption].
      intros v0 Hlt.
      assert (v0 = S n \/ v' < v0 <= n) as Hv0 by lia.
      destruct Hv0; [subst; assumption|].
      apply Hnp'; assumption.
Qed.

Lemma quorum_msg_for_block_eq : forall s b msg,
 quorum_msg_for_block s b msg <-> get_quorum_msg_for_block s b = Some msg.
Proof.
 intros s b msg.
 unfold quorum_msg_for_block, get_quorum_msg_for_block.
 split.
 - intros Hb.
   destruct (block_eq_dec GenesisBlock b) as [Hg|Hg].
   * destruct Hb as [[Hb Hm]|[Hb Hm]]; [|congruence].
     rewrite Hm; reflexivity.
   * destruct Hb as [[Hb Hm]|[Hb Hm]]; [congruence|].
     clear Hb Hg.
     revert Hm.
     apply prepare_stage_in_view_quorum_msg_eq.
 - destruct (block_eq_dec GenesisBlock b) as [Hg|Hg].
   * intros Hm.
     inversion Hm; subst.
     tauto.
   * intros Hm.
     right; split; [congruence|].
     apply prepare_stage_in_view_quorum_msg_eq; assumption.
Qed.

Lemma propose_block_init_set_eq : forall s msg s' lm,
 s = NState_init s.(node_id) ->
 s.(timeout) = false ->
 is_block_proposer s.(node_id) 0 ->
 honest_node s.(node_id) ->
 (propose_block_init_set s msg = (s',lm) <-> propose_block_init s msg s' lm).
Proof.
unfold propose_block_init, propose_block_init_set.
split.
- intros Heq; inversion Heq; subst; tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  destruct Heq'' as [Heq'' Heq'''].
  subst; reflexivity.
Qed.

Lemma process_timeout_set_eq : forall s msg s' lm,
 honest_node (node_id s) ->
 timeout s = true ->
 (process_timeout_set s msg = (s',lm) <-> process_timeout s msg s' lm).
Proof.
unfold process_timeout, process_timeout_set.
split.
- intros Heq; inversion Heq; subst; tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  destruct Heq'' as [Heq'' Heq'''].
  subst; reflexivity.
Qed.

Lemma discard_view_invalid_set_eq : forall s msg s' lm,
 ~ view_valid s msg ->
 received s msg ->
 honest_node (node_id s) ->
 (discard_view_invalid_set s msg = (s',lm) <-> discard_view_invalid s msg s' lm).
Proof.
unfold discard_view_invalid, discard_view_invalid_set.
split.
- intros Heq; inversion Heq; subst; tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareBlock_duplicate_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareBlock ->
 view_valid s msg ->
 timeout s = false ->
 exists_same_height_PrepareBlock s (get_block msg) ->
 (process_PrepareBlock_duplicate_set s msg = (s',lm) <->
  process_PrepareBlock_duplicate s msg s' lm).
Proof.
unfold process_PrepareBlock_duplicate, process_PrepareBlock_duplicate_set.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareBlock_pending_vote_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareBlock ->
 view_valid s msg ->
 timeout s = false ->
 ~ exists_same_height_PrepareBlock s (get_block msg) ->
 ~ prepare_stage s (parent_of (get_block msg)) ->
 (process_PrepareBlock_pending_vote_set s msg = (s', lm) <->
   process_PrepareBlock_pending_vote s msg s' lm).
Proof.
unfold process_PrepareBlock_pending_vote, process_PrepareBlock_pending_vote_set.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareBlock_vote_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareBlock ->
 view_valid s msg ->
 timeout s = false ->
 prepare_stage s (parent_of (get_block msg)) ->
 (process_PrepareBlock_vote_set s msg = Some (s', lm) <->
   process_PrepareBlock_vote s msg s' lm).
Proof.
unfold process_PrepareBlock_vote, process_PrepareBlock_vote_set.
intros s msg s' lm.
case_eq (get_quorum_msg_for_block s (parent_of (get_block msg))); split.
- intros Heq.
  inversion Heq; subst; clear Heq.
  apply quorum_msg_for_block_eq in H.
  exists m; split; [assumption|].
  split; [reflexivity|].
  split; [reflexivity|].
  tauto.
- intros Heq.
  destruct Heq as [quorum_msg [Hq Heq]].
  apply quorum_msg_for_block_eq in Hq.
  rewrite H in Hq.
  inversion Hq; subst.
  destruct Heq as [Hs [Hl Heq]].
  subst; reflexivity.
- congruence.
- intros Heq.
  destruct Heq as [quorum_msg [Hq Heq]].
  apply quorum_msg_for_block_eq in Hq.
  congruence.
Qed.

Lemma process_PrepareVote_wait_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node s.(node_id) ->
 get_message_type msg = PrepareVote ->
 view_valid s msg ->
 timeout s = false ->
 ~ prepare_stage (process s msg) (get_block msg) ->
 (process_PrepareVote_wait_set s msg = (s', lm) <->
   process_PrepareVote_wait s msg s' lm).
Proof.
unfold process_PrepareVote_wait, process_PrepareVote_wait_set.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareVote_vote_set_eq : forall s msg s' lm,
 honest_node (node_id s) ->
 received s msg ->
 get_message_type msg = PrepareVote ->
 view_valid s msg ->
 timeout s = false ->
 (Forall (fun m => ~ exists_same_height_block s (get_block m)) lm) ->
 vote_quorum_in_view (process s msg) (get_view msg) (get_block msg) ->
 (process_PrepareVote_vote_set s msg = (s', lm) <->
   process_PrepareVote_vote s msg s' lm).
Proof.
unfold process_PrepareVote_vote, process_PrepareVote_vote_set.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Hl [Hs [Hlm Heq]]].
  subst; reflexivity.
Qed.

Lemma process_PrepareQC_last_block_new_proposer_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareQC ->
 view_valid s msg ->
 is_block_proposer (node_id s) (S (node_view s)) ->
 (process_PrepareQC_last_block_new_proposer_set s msg = Some (s', lm) <->
   process_PrepareQC_last_block_new_proposer s msg s' lm).
Proof.
unfold process_PrepareQC_last_block_new_proposer_set,process_PrepareQC_last_block_new_proposer.
intros s msg s' lm.
case_eq (get_quorum_msg_for_block s (parent_of (get_block msg))); split.
- intros Heq.
  inversion Heq; subst; clear Heq.
  apply quorum_msg_for_block_eq in H.
  exists m; split; [assumption|].
  split; [reflexivity|].
  split; [reflexivity|].
  tauto.
- intros Heq.
  destruct Heq as [quorum_msg [Hq Heq]].
  apply quorum_msg_for_block_eq in Hq.
  rewrite H in Hq.
  inversion Hq; subst.
  destruct Heq as [Hs [Hl Heq]].
  subst; reflexivity.
- congruence.
- intros Heq.
  destruct Heq as [quorum_msg [Hq Heq]].
  apply quorum_msg_for_block_eq in Hq.
  congruence.
Qed.

Lemma process_PrepareQC_last_block_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareQC ->
 view_valid s msg ->
 ~ is_block_proposer (node_id s) (S (node_view s)) ->
 (process_PrepareQC_last_block_set s msg = (s', lm) <->
  process_PrepareQC_last_block s msg s' lm).
Proof.
unfold process_PrepareQC_last_block_set,process_PrepareQC_last_block.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareQC_non_last_block_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareQC ->
 view_valid s msg ->
 ~ last_block (get_block msg) ->
 (process_PrepareQC_non_last_block_set s msg = (s', lm) <->
   process_PrepareQC_non_last_block s msg s' lm).
Proof.
unfold process_PrepareQC_non_last_block_set,process_PrepareQC_non_last_block.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_ViewChange_quorum_new_proposer_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = ViewChange ->
 view_valid s msg ->
 view_change_quorum_in_view (process s msg) (node_view s) ->
 is_block_proposer (node_id s) (S (node_view s)) ->
 (process_ViewChange_quorum_new_proposer_set s msg = (s', lm) <->
  process_ViewChange_quorum_new_proposer s msg s' lm).
Proof.
unfold process_ViewChange_quorum_new_proposer_set,process_ViewChange_quorum_new_proposer.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_ViewChange_quorum_not_new_proposer_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = ViewChange ->
 view_valid s msg ->
 view_change_quorum_in_view (process s msg) (node_view s) ->
  ~ is_block_proposer (node_id s) (S (node_view s)) ->
 (process_ViewChange_quorum_not_new_proposer_set s msg = (s', lm) <->
  process_ViewChange_quorum_not_new_proposer s msg s' lm).
Proof.
unfold process_ViewChange_quorum_not_new_proposer_set,process_ViewChange_quorum_not_new_proposer.
split.
- intros Heq; inversion Heq; subst.
  split; [reflexivity|].
  split; [reflexivity|].
  tauto.
- intros Heq.
  destruct Heq as [Hs [Hl Heq]].
  subst; reflexivity.
Qed.

Lemma process_ViewChange_pre_quorum_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = ViewChange ->
 view_valid s msg -> 
 ~ view_change_quorum_in_view (process s msg) (node_view s) ->
 (process_ViewChange_pre_quorum_set s msg = (s',lm) <->
  process_ViewChange_pre_quorum s msg s' lm).
Proof.
unfold process_ViewChange_pre_quorum_set,process_ViewChange_pre_quorum.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_ViewChangeQC_single_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = ViewChangeQC ->
 view_valid s msg ->
 (process_ViewChangeQC_single_set s msg = (s',lm) <->
   process_ViewChangeQC_single s msg s' lm).
Proof.
unfold process_ViewChangeQC_single_set,process_ViewChangeQC_single.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma malicious_ignore_set_eq : forall s msg s' lm,
 received s msg ->
 ~ honest_node (node_id s) ->
 (malicious_ignore_set s msg = (s',lm) <->
   malicious_ignore s msg s' lm).
Proof.
unfold malicious_ignore_set,malicious_ignore.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareBlock_malicious_vote_set_eq : forall s msg s' lm,
 received s msg ->
 ~ honest_node (node_id s) ->
 get_message_type msg = PrepareBlock ->
 view_valid s msg ->
 exists_same_height_block s (get_block msg) ->
 (process_PrepareBlock_malicious_vote_set s msg = Some (s',lm) <->
   process_PrepareBlock_malicious_vote s msg s' lm).
Proof.
unfold process_PrepareBlock_malicious_vote_set,process_PrepareBlock_malicious_vote.
intros s msg s' lm.
case_eq (get_quorum_msg_for_block s (parent_of (get_block msg))); split.
- intros Heq.
  inversion Heq; subst; clear Heq.
  apply quorum_msg_for_block_eq in H.
  exists m; split; [assumption|].
  split; [reflexivity|].
  split; [reflexivity|].
  tauto.
- intros Heq.
  destruct Heq as [quorum_msg [Hq Heq]].
  apply quorum_msg_for_block_eq in Hq.
  rewrite H in Hq.
  inversion Hq; subst.
  destruct Heq as [Hs [Hl Heq]].
  subst; reflexivity.
- congruence.
- intros Heq.
  destruct Heq as [quorum_msg [Hq Heq]].
  apply quorum_msg_for_block_eq in Hq.
  congruence.
Qed.

(* END REFINEMENT PROOFS *)

(* BEGIN TEST CASES *)

Section TestCases.

Variable n : node.
Variable b : block.

Example propose_block_init_set_example_0 :
 let s := NState_init n in
 let lm := make_PrepareBlocks s (GenesisBlock_message s) in
 propose_block_init_set s (mkMessage PrepareQC 0 n b b) =
  ({| node_view := 0; node_id := n; in_messages := []; counting_messages := [];
      out_messages := lm; timeout := false |}, lm).
Proof.
 reflexivity.
Qed.

End TestCases.

(* END TEST CASES *)
