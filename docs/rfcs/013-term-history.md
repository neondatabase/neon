# What

Currently, apart from WAL safekeeper persistently stores only two logical clock
counter (aka term) values, sourced from the same sequence. The first is bumped
whenever safekeeper gives vote to proposer (or acknowledges already elected one)
and e.g. prevents electing two proposers with the same term -- it is actually
called `term` in the code. The second, called `epoch`, reflects progress of log
receival and this might lag behind `term`; safekeeper switches to epoch `n` when
it has received all committed log records from all `< n` terms. This roughly
corresponds to proposed in

https://github.com/neondatabase/rfcs/pull/3/files


This makes our biggest our difference from Raft. In Raft, every log record is
stamped with term in which it was generated; while we essentially store in
`epoch` only the term of the highest record on this safekeeper -- when we know
it -- because during recovery generally we don't, and `epoch` is bumped directly
to the term of the proposer who performs the recovery when it is finished. It is
not immediately obvious that this simplification is safe. I thought and I still
think it is; model checking confirmed that. However, some details now make me
believe it is better to keep full term switching history (which is equivalent to
knowing term of each record).

# Why

Without knowing full history (list of <term, LSN> pairs) of terms it is hard to
determine the exact divergence point, and if we don't perform truncation at that
point safety becomes questionable. Consider the following history, with
safekeepers A, B, C, D, E. n_m means record created by proposer in term n with
LSN m; (t=x, e=y) means safekeeper currently has term x and epoch y.

1) P1 in term 1 writes 1.1 everywhere, which is committed, and some more only
on A.

<pre>
A(t=1, e=1) 1.1 1.2 1.3 1.4
B(t=1, e=1) 1.1
C(t=1, e=1) 1.1
D(t=1, e=1) 1.1
E(t=1, e=1) 1.1
</pre>

2) P2 is elected by CDE in term 2, epochStartLsn is 2, and writes 2.2, 2.3 on CD:

<pre>
A(t=1, e=1) 1.1 1.2 1.3 1.4
B(t=1, e=1) 1.1
C(t=2, e=2) 1.1 2.2 2.3
D(t=2, e=2) 1.1 2.2 2.3
E(t=2, e=1) 1.1
</pre>


3) P3 is elected by CDE in term 3, epochStartLsn is 4, and writes 3.4 on D:

<pre>
A(t=1, e=1) 1.1 1.2 1.3 1.4
B(t=1, e=1) 1.1
C(t=3, e=2) 1.1 2.2 2.3
D(t=3, e=3) 1.1 2.2 2.3 3.4
E(t=3, e=1) 1.1
</pre>


Now, A gets back and P3 starts recovering it. How it should proceed? There are
two options.

## Don't try to find divergence point at all

...start sending WAL conservatively since the horizon (1.1), and truncate
obsolete part of WAL only when recovery is finished, i.e. epochStartLsn (4) is
reached, i.e. 2.3 transferred -- that's what https://github.com/neondatabase/neon/pull/505 proposes.

Then the following is possible:

4) P3 moves one record 2.2 to A.

<pre>
A(t=1, e=1) 1.1 <b>2.2</b> 1.3 1.4
B(t=1, e=1) 1.1 1.2
C(t=3, e=2) 1.1 2.2 2.3
D(t=3, e=3) 1.1 2.2 2.3 3.4
E(t=3, e=1) 1.1
</pre>

Now log of A is basically corrupted. Moreover, since ABE are all in epoch 1 and
A's log is the longest one, they can elect P4 who will commit such log.

Note that this particular history couldn't happen if we forbid to *create* new
records in term n until majority of safekeepers switch to it. It would force CDE
to switch to 2 before 2.2 is created, and A could never become donor while his
log is corrupted. Generally with this additional barrier I believe the algorithm
becomes safe, but
 - I don't like this kind of artificial barrier;
 - I also feel somewhat discomfortable about even temporary having intentionally
   corrupted WAL;
 - I'd still model check the idea.

## Find divergence point and truncate at it

Then step 4 would delete 1.3 1.4 on A, and we are ok. The question is, how do we
do that? Without term switching history we have to resort to sending again since
the horizon and memcmp'ing records, which is inefficient and ugly. Or we can
maintain full history and determine truncation point by comparing 'wrong' and
'right' histories -- much like pg_rewind does -- and perform truncation + start
streaming right there.

# Proposal

- Add term history as array of <term, LSN> pairs to safekeeper controlfile.
- Return it to proposer with VoteResponse so 1) proposer can tell it to other
  nodes and 2) determine personal streaming starting point. However, since we
  don't append WAL and update controlfile atomically, let's first always update
  controlfile but send only the history of what we really have (up to highest
  term in history where begin_lsn >= end of wal; this highest term replaces
  current `epoch`). We also send end of wal as we do now to determine the donor.
- Create ProposerAnnouncement message which proposer sends before starting
  streaming. It announces proposer as elected and
  1) Truncates wrong part of WAL on safekeeper
     (divergence point is already calculated at proposer, but can be
     cross-verified here).
  2) Communicates the 'right' history of its term (taken from donor). Seems
     better to immediately put the history in the controlfile,
	 though safekeeper might not have full WAL for previous terms in it --
	 this way is simpler, and we can't update WAL and controlfile atomically anyway.

	 This also constitutes analogue of current epoch bump for those safekeepers
     which don't need recovery, which is important for sync-safekeepers (bump
     epoch without waiting records from new term).
- After ProposerAnnouncement proposer streams WAL since calculated starting
  point -- only what is missing.


pros/cons:
+ (more) clear safety of WAL truncation -- we get very close to Raft
+ no unnecessary data sending (faster recovery for not-oldest-safekeepers, matters
   only for 5+ nodes)
+ adds some observability at safekeepers

- complexity, but not that much


# Misc

- During model checking I did truncation on first locally non existent or
  different record -- analogue of 'memcmp' variant described above.
