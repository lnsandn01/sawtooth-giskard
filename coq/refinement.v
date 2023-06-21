(*
MIT License

Copyright (c) 2023 Karl Palmskog

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*)

From Coq Require Import Arith Bool List.
From Giskard Require Import structures local.
From RecordUpdate Require Import RecordSet.

Import ListNotations.
Import RecordSetNotations.

Set Implicit Arguments.

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
 : NState * list message :=
 let lm := pending_PrepareVote s msg in
 let s' := s
   <| in_messages := remove message_eq_dec msg s.(in_messages) |>
   <| counting_messages := msg :: s.(counting_messages) |>
   <| out_messages := lm ++ s.(out_messages) |>
 in (s', lm).

Definition process_PrepareVote_wait_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
 in (s', []).

Definition process_PrepareVote_vote_set (s : NState) (msg : message)
 : NState * list message :=
 let lm := make_PrepareQC s msg :: pending_PrepareVote s msg in
 let s' := s
  <| out_messages := lm ++ s.(out_messages) |>
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
 in (s', lm).

Definition process_PrepareQC_last_block_new_proposer_set (s : NState) (msg : message)
 : NState * list message :=
 let lm := make_PrepareBlocks (increment_view (process_set s msg)) msg in
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
  <| node_view := S (node_view s) |>
  <| in_messages := [] |>
  <| timeout := false |>
  <| out_messages := lm ++ s.(out_messages) |>
 in (s', lm).

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

Definition process_ViewChange_pre_quorum_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
 in (s', []).

Definition process_ViewChangeQC_single_set (s : NState) (msg : message)
 : NState * list message :=
 let msg' :=
   mkMessage PrepareQC (node_view s) (get_sender msg) (get_block msg) GenesisBlock
 in
 let s' := s
  <| counting_messages := msg :: msg' :: s.(counting_messages) |>
  <| node_view := S (node_view s) |>
  <| in_messages := [] |>
  <| timeout := false |>
 in (s', []).

Definition malicious_ignore_set (s : NState) (msg : message)
 : NState * list message :=
 let s' := s <| in_messages := remove message_eq_dec msg s.(in_messages) |> in
 (s', []).

Definition process_PrepareBlock_malicious_vote_set (s : NState) (msg : message)
 : NState * list message :=
 let lm := pending_PrepareVote s msg in
 let s' := s
  <| in_messages := remove message_eq_dec msg s.(in_messages) |>
  <| counting_messages := msg :: s.(counting_messages) |>
  <| out_messages := lm ++ s.(out_messages) |>
 in (s', lm).

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
 (process_PrepareBlock_vote_set s msg = (s', lm) <->
   process_PrepareBlock_vote s msg s' lm).
Proof.
unfold process_PrepareBlock_vote, process_PrepareBlock_vote_set.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
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
 ~ exists_same_height_block s (get_block msg) ->
 vote_quorum_in_view (process s msg) (get_view msg) (get_block msg) ->
 (process_PrepareVote_vote_set s msg = (s', lm) <->
   process_PrepareVote_vote s msg s' lm).
Proof.
unfold process_PrepareVote_vote, process_PrepareVote_vote_set.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareQC_last_block_new_proposer_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareQC ->
 view_valid s msg ->
 last_block (get_block msg) /\ is_block_proposer (node_id s) (S (node_view s)) ->
 (process_PrepareQC_last_block_new_proposer_set s msg = (s', lm) <->
   process_PrepareQC_last_block_new_proposer s msg s' lm).
Proof.
unfold process_PrepareQC_last_block_new_proposer_set,process_PrepareQC_last_block_new_proposer.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
Qed.

Lemma process_PrepareQC_last_block_set_eq : forall s msg s' lm,
 received s msg ->
 honest_node (node_id s) ->
 get_message_type msg = PrepareQC ->
 view_valid s msg ->
 last_block (get_block msg) ->
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
 timeout s = false ->
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
 received s (mkMessage PrepareQC (get_view msg) (get_sender msg)
  (get_block (highest_ViewChange_message (process s msg))) GenesisBlock) ->
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
 received s (mkMessage PrepareQC (node_view s) (get_sender msg) (get_block msg) GenesisBlock) ->
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
 (process_PrepareBlock_malicious_vote_set s msg = (s',lm) <->
   process_PrepareBlock_malicious_vote s msg s' lm).
Proof.
unfold process_PrepareBlock_malicious_vote_set,process_PrepareBlock_malicious_vote.
split.
- intros Heq; inversion Heq; subst.
  tauto.
- intros Heq.
  destruct Heq as [Heq Heq'].
  destruct Heq' as [Heq' Heq''].
  subst; reflexivity.
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
