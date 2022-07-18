This directory contains Request for Comments documents, or RFCs, for
features or concepts that have been proposed. Alternative names:
technical design doc, ERD, one-pager

To make a new proposal, create a new text file in this directory and
open a Pull Request with it. That gives others a chance and a forum
to comment and discuss the design.

When a feature is implemented and the code changes are committed, also
include the corresponding RFC in this directory.

Some of the RFCs in this directory have been implemented in some form
or another, while others are on the roadmap, while still others are
just obsolete and forgotten about. So read them with a grain of salt,
but hopefully even the ones that don't reflect reality give useful
context information.

## What

We use Tech Design RFC’s to summarize what we are planning to
implement in our system. These RFCs should be created for large or not
obvious technical tasks, e.g. changes of the architecture or bigger
tasks that could take over a week, changes that touch multiple
components or their interaction. RFCs should fit into a couple of
pages, but could be longer on occasion.

## Why

We’re using RFCs to enable early review and collaboration, reduce
uncertainties, risk and save time during the implementation phase that
follows the Tech Design RFC.

Tech Design RFCs also aim to avoid bus factor and are an additional
measure to keep more peers up to date & familiar with our design and
architecture.

This is a crucial part for ensuring collaboration across timezones and
setting up for success a distributed team that works on complex
topics.

## Prior art

- Rust: [https://github.com/rust-lang/rfcs/blob/master/0000-template.md](https://github.com/rust-lang/rfcs/blob/master/0000-template.md)
- React.js: [https://github.com/reactjs/rfcs/blob/main/0000-template.md](https://github.com/reactjs/rfcs/blob/main/0000-template.md)
- Google fuchsia: [https://fuchsia.dev/fuchsia-src/contribute/governance/rfcs/TEMPLATE](https://fuchsia.dev/fuchsia-src/contribute/governance/rfcs/TEMPLATE)
- Apache: [https://cwiki.apache.org/confluence/display/GEODE/RFC+Template](https://cwiki.apache.org/confluence/display/GEODE/RFC+Template) / [https://cwiki.apache.org/confluence/display/GEODE/Lightweight+RFC+Process](https://cwiki.apache.org/confluence/display/GEODE/Lightweight+RFC+Process)

## How

RFC lifecycle:

- Should be submitted in a pull request with and full RFC text in a committed markdown file and copy of the Summary and Motivation sections also included in the PR body.
- RFC should be published for review before most of the actual code is written. This isn’t a strict rule, don’t hesitate to experiment and build a POC in parallel with writing an RFC.
- Add labels to the PR in the same manner as you do Issues. Example TBD
- Request the review from your peers. Reviewing the RFCs from your peers is a priority, same as reviewing the actual code.
- The Tech Design RFC should evolve based on the feedback received and further during the development phase if problems are discovered with the taken approach
- RFCs stop evolving once the consensus is found or the proposal is implemented and merged.
- RFCs are not intended as a documentation that’s kept up to date **after** the implementation is finished. Do not update the Tech Design RFC when merged functionality evolves later on. In such situation a new RFC may be appropriate.

### RFC template

Note, a lot of the sections are marked as ‘if relevant’. They are included into the template as a reminder and to help inspiration.

```
# Name
Created on ..
Implemented on ..

## Summary

## Motivation

## Non Goals (if relevant)

## Impacted components (e.g. pageserver, safekeeper, console, etc)

## Proposed implementation

### Reliability, failure modes and corner cases (if relevant)

### Interaction/Sequence diagram (if relevant)

### Scalability (if relevant)

### Security implications (if relevant)

### Unresolved questions (if relevant)

## Alternative implementation (if relevant)

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)

```
