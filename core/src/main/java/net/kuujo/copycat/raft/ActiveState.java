/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract active state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ActiveState extends PassiveState {
  protected boolean transition;

  protected ActiveState(RaftContext context) {
    super(context);
  }

  /**
   * Transitions to a new state.
   */
  protected CompletableFuture<Type> transition(Type state) {
    if (transitionHandler != null) {
      return transitionHandler.apply(state);
    }
    return exceptionalFuture(new IllegalStateException("No transition handler registered"));
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();
    CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      transition(Type.FOLLOWER);
      transition = false;
    }
    return future;
  }

  /**
   * Starts the append process.
   */
  private AppendResponse handleAppend(AppendRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
      transition = true;
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      LOGGER.warn("{} - Rejected {}: request term is less than the current term ({})", context.getLocalMember(), request, context.getTerm());
      return AppendResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    } else if (request.logIndex() != null && request.logTerm() != null) {
      return doCheckPreviousEntry(request);
    } else {
      try {
        return doAppendEntries(request);
      } catch (Exception e) {
         LOGGER.warn("doAppendEntries", e);
        throw e;
      }
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private AppendResponse doCheckPreviousEntry(AppendRequest request) {
    if (request.logIndex() != null && context.log().lastIndex() == null) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getLocalMember(), request, request.logIndex(), context.log().lastIndex());
      return AppendResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    } else if (request.logIndex() != null && context.log().lastIndex() != null && request.logIndex() > context.log().lastIndex()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getLocalMember(), request, request.logIndex(), context.log().lastIndex());
      return AppendResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    }

    // If the previous log entry is not in the log then reject the request.
    if (!context.log().containsIndex(request.logIndex())) {
      LOGGER.warn("{} - Rejected {}: Request entry not found in local log", context.getLocalMember(), request);
      return AppendResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    }

    // If the previous entry term doesn't match the local previous term then reject the request.
    ByteBuffer entry = context.log().getEntry(request.logIndex());
    long localLogTerm = entry.getLong();
    if (localLogTerm != request.logTerm()) {
      AppendResponse response = AppendResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
      LOGGER.warn("{} - Rejected {}: Request entry term does not match local log. Local log term is {}. Replying with {}", context.getLocalMember(), request, localLogTerm, response);
      return response;
    } else {
      try {
        return doAppendEntries(request);
      } catch (Exception e) {
        LOGGER.warn("doAppendEntries", e);
        throw e;
      }
    }
  }

  /**
   * Appends entries to the local log.
   */
  private AppendResponse doAppendEntries(AppendRequest request) {
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {
      long index = request.logIndex() != null ? request.logIndex() : 0L;

      // If the request contains the first entries in the log, check whether the local log needs to be rolled over.
      Long rollOverIndex = null;
      if (request.firstIndex() && (context.log().segment().firstIndex() == null || context.log().segment().firstIndex() != index + 1)) {
        rollOverIndex = index + 1;
        try {
          context.log().rollOver(rollOverIndex);
        } catch (IOException e) {
          LOGGER.error("{} - Failed to roll over local log", context.getLocalMember());

          // Apply any commits before returning.
          if (!context.log().isEmpty()) {
            doApplyCommits(Math.min(context.log().lastIndex(), request.commitIndex()));
          }

          return AppendResponse.builder()
            .withUri(context.getLocalMember())
            .withTerm(context.getTerm())
            .withSucceeded(false)
            .withLogIndex(context.log().lastIndex())
            .build();
        }
      }

      // Iterate through request entries and append them to the log.
      for (ByteBuffer entry : request.entries()) {
        index++;
        // Replicated snapshot entries are *always* immediately logged and applied to the state machine
        // since snapshots are only taken of committed state machine state. This will cause all previous
        // entries to be removed from the log.
        if (context.log().containsIndex(index)) {
          // Compare the term of the received entry with the matching entry in the log.
          ByteBuffer match = context.log().getEntry(index);
          if (entry.getLong() != match.getLong()) {
            // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
            // If appending to the log fails, apply commits and reply false to the append request.
            LOGGER.warn("{} - Synced entry does not match local log, removing incorrect entries", context.getLocalMember());
            try {
              context.log().removeAfter(index - 1);
              context.log().appendEntry(entry);
            } catch (IOException e) {
              doApplyCommits(request.commitIndex());
              return AppendResponse.builder()
                .withUri(context.getLocalMember())
                .withTerm(context.getTerm())
                .withSucceeded(false)
                .withLogIndex(context.log().lastIndex())
                .build();
            }
            LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember(), entry, index);
          }
        } else {
          // If appending to the log fails, apply commits and reply false to the append request.
          try {
            context.log().appendEntry(entry);
          } catch (IOException e) {
            doApplyCommits(request.commitIndex());
            return AppendResponse.builder()
              .withUri(context.getLocalMember())
              .withTerm(context.getTerm())
              .withSucceeded(false)
              .withLogIndex(context.log().lastIndex())
              .build();
          }
          LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember(), entry, index);
        }
      }

      // If the log was rolled over, compact the log and then flush the log to disk.
      try {
        if (rollOverIndex != null) {
          context.log().compact(rollOverIndex);
        }
      } catch (IOException e) {
        LOGGER.error("{} - Failed to roll over local log", context.getLocalMember());
        doApplyCommits(request.commitIndex());
        return AppendResponse.builder()
          .withUri(context.getLocalMember())
          .withTerm(context.getTerm())
          .withSucceeded(false)
          .withLogIndex(context.log().lastIndex())
          .build();
      } finally {
        context.log().flush();
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    doApplyCommits(request.commitIndex());
    return AppendResponse.builder()
      .withUri(context.getLocalMember())
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.log().lastIndex())
      .build();
  }

  /**
   * Applies commits to the local state machine.
   */
  private void doApplyCommits(Long commitIndex) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (commitIndex != null && !context.log().isEmpty()) {
      LOGGER.debug("{} - Applying {} commits", context.getLocalMember(), context.getLastApplied() != null ? commitIndex - Math.max(context.getLastApplied(), context.log().firstIndex()) : commitIndex);
      if (context.getCommitIndex() == null || commitIndex > context.getCommitIndex() || context.getCommitIndex() > context.getLastApplied()) {
        // Update the local commit index with min(request commit, last log // index)
        Long firstIndex = context.log().firstIndex();
        Long lastIndex = context.log().lastIndex();
        if (lastIndex != null) {
          context.setCommitIndex(Math.min(Math.max(commitIndex, context.getCommitIndex() != null ? context.getCommitIndex() : commitIndex), lastIndex));

          // If the updated commit index indicates that commits remain to be
          // applied to the state machine, iterate entries and apply them.
          if (context.getLastApplied() == null || context.getCommitIndex() > context.getLastApplied()) {
            // Starting after the last applied entry, iterate through new entries
            // and apply them to the state machine up to the commit index.
            Long firstIndexToApply = context.getLastApplied() == null || Long.valueOf(context.getLastApplied() + 1) < firstIndex ? firstIndex : Long.valueOf(context.getLastApplied() + 1);
            for (long i = firstIndexToApply; i <= Math.min(context.getCommitIndex(), lastIndex); i++) {
              // Apply the entry to the state machine.
              applyEntry(i);
            }
          }
        }
      }
    }
  }

  /**
   * Applies the given entry.
   */
  protected void applyEntry(long index) {
    if (index == context.log().firstIndex() || (context.getLastApplied() != null && context.getLastApplied() == index - 1)) {
      ByteBuffer entry = context.log().getEntry(index);

      // Extract a view of the entry after the entry term.
      long term = entry.getLong();
      ByteBuffer userEntry = entry.slice();

      try {
        context.consumer().apply(term, index, userEntry);
      } catch (Exception e) {
      } finally {
        context.setLastApplied(index);
      }
    }
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    return CompletableFuture.completedFuture(logResponse(handlePoll(logRequest(request))));
  }

  /**
   * Handles a poll request.
   */
  protected PollResponse handlePoll(PollRequest request) {
    if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
      return PollResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withAccepted(true)
        .build();
    } else {
      return PollResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build();
    }
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    return CompletableFuture.completedFuture(logResponse(handleVote(logRequest(request))));
  }

  /**
   * Handles a vote request.
   */
  protected VoteResponse handleVote(VoteRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", context.getLocalMember(), request);
      return VoteResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is our self then always vote for our self. Votes
    // for self are done by calling the local node. Note that this obviously
    // doesn't make sense for a leader.
    else if (request.candidate().equals(context.getLocalMember())) {
      context.setLastVotedFor(context.getLocalMember());
      LOGGER.debug("{} - Accepted {}: candidate is the local member", context.getLocalMember(), request);
      return VoteResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(true)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getMembers().contains(request.candidate())) {
      LOGGER.debug("{} - Rejected {}: candidate is not known to the local member", context.getLocalMember(), request);
      return VoteResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.getLastVotedFor() == null || context.getLastVotedFor().equals(request.candidate())) {
      if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
        context.setLastVotedFor(request.candidate());
        return VoteResponse.builder()
          .withUri(context.getLocalMember())
          .withTerm(context.getTerm())
          .withVoted(true)
          .build();
      } else {
        return VoteResponse.builder()
          .withUri(context.getLocalMember())
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
      }
    }
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", context.getLocalMember(), request, context.getLastVotedFor());
      return VoteResponse.builder()
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
  }

  /**
   * Returns a boolean value indicating whether the given candidate's log is up-to-date.
   */
  private boolean logUpToDate(Long index, Long term, Request request) {
    // If the log is empty then vote for the candidate.
    if (context.log().isEmpty()) {
      LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
      return true;
    } else {
      // Otherwise, load the last entry in the log. The last entry should be
      // at least as up to date as the candidates entry and term.
      Long lastIndex = context.log().lastIndex();
      if (lastIndex != null) {
        ByteBuffer entry = context.log().getEntry(lastIndex);
        if (entry == null) {
          LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
          return true;
        }

        long lastTerm = entry.getLong();
        if (index != null && index >= lastIndex) {
          if (term >= lastTerm) {
            LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
            return true;
          } else {
            LOGGER.debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", context.getLocalMember(), request, term, lastTerm);
            return false;
          }
        } else {
          LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getLocalMember(), request, index, lastIndex);
          return false;
        }
      } else {
        LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
        return true;
      }
    }
  }

}
