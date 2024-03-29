PROPOSE_BLOCK_INIT_TYPE = 1
DISCARD_VIEW_INVALID_TYPE = 2
PROCESS_PREPAREBLOCK_DUPLICATE_TYPE = 3
PROCESS_PREPAREBLOCK_PENDING_VOTE_TYPE = 4
PROCESS_PREPAREBLOCK_VOTE_TYPE = 5
PROCESS_PREPAREVOTE_VOTE_TYPE = 6
PROCESS_PREPAREVOTE_WAIT_TYPE = 7
PROCESS_PREPAREQC_LAST_BLOCK_NEW_PROPOSER_TYPE = 8
PROCESS_PREPAREQC_LAST_BLOCK_TYPE = 9
PROCESS_PREPAREQC_NON_LAST_BLOCK_TYPE = 10
PROCESS_VIEWCHANGE_QUORUM_NEW_PROPOSER_TYPE = 11
PROCESS_VIEWCHANGE_QUORUM_NOT_NEW_PROPOSER_TYPE = 12  # CHANGE from the original specification
PROCESS_VIEWCHANGE_PRE_QUORUM_TYPE = 13
PROCESS_VIEWCHANGEQC_SINGLE_TYPE = 14
PROCESS_PREPAREBLOCK_MALICIOUS_VOTE_TYPE = 15

all_transition_types = [PROPOSE_BLOCK_INIT_TYPE,
                        DISCARD_VIEW_INVALID_TYPE,
                        PROCESS_PREPAREBLOCK_DUPLICATE_TYPE,
                        PROCESS_PREPAREBLOCK_PENDING_VOTE_TYPE,
                        PROCESS_PREPAREBLOCK_VOTE_TYPE,
                        PROCESS_PREPAREVOTE_VOTE_TYPE,
                        PROCESS_PREPAREVOTE_WAIT_TYPE,
                        PROCESS_PREPAREQC_LAST_BLOCK_NEW_PROPOSER_TYPE,
                        PROCESS_PREPAREQC_LAST_BLOCK_TYPE,
                        PROCESS_PREPAREQC_NON_LAST_BLOCK_TYPE,
                        PROCESS_VIEWCHANGE_QUORUM_NEW_PROPOSER_TYPE,
                        PROCESS_VIEWCHANGE_QUORUM_NOT_NEW_PROPOSER_TYPE,
                        PROCESS_VIEWCHANGE_PRE_QUORUM_TYPE,
                        PROCESS_VIEWCHANGEQC_SINGLE_TYPE,
                        PROCESS_PREPAREBLOCK_MALICIOUS_VOTE_TYPE]
