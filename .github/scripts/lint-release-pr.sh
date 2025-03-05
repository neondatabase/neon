#!/usr/bin/env bash

set -euo pipefail

case "$RELEASE_BRANCH" in
  "release") COMPONENT="Storage" ;;
  "release-proxy") COMPONENT="Proxy" ;;
  "release-compute") COMPONENT="Compute" ;;
  *)
    echo "❌ Unknown release branch: $RELEASE_BRANCH"
    exit 1
    ;;
esac


# Identify main and release branches
MAIN_BRANCH="origin/main"
REMOTE_RELEASE_BRANCH="origin/${RELEASE_BRANCH}"

# Find merge base
MERGE_BASE=$(git merge-base "$MAIN_BRANCH" "$REMOTE_RELEASE_BRANCH")
echo "Merge base of $MAIN_BRANCH and $RELEASE_BRANCH: $MERGE_BASE"

# Get the HEAD commit (last commit in PR, expected to be the merge commit)
LAST_COMMIT=$(git rev-parse HEAD)

MERGE_COMMIT_MESSAGE=$(git log -1 --format=%s "$LAST_COMMIT")
EXPECTED_MESSAGE_REGEX="^$COMPONENT release [0-9]{4}-[0-9]{2}-[0-9]{2}$"

if ! [[ "$MERGE_COMMIT_MESSAGE" =~ $EXPECTED_MESSAGE_REGEX ]]; then
  echo "❌ Merge commit message does not match expected pattern: '<component> release YYYY-MM-DD'"
  echo "   Expected component: $COMPONENT"
  echo "   Found: '$MERGE_COMMIT_MESSAGE'"
  exit 1
fi
echo "✅ Merge commit message is correctly formatted: '$MERGE_COMMIT_MESSAGE'"

LAST_COMMIT_PARENTS=$(git cat-file -p "$LAST_COMMIT" | jq -sR '[capture("parent (?<parent>[0-9a-f]{40})"; "g") | .parent]')

if [[ "$(echo "$LAST_COMMIT_PARENTS" | jq 'length')" -ne 2 ]]; then
  echo "❌ Last commit must be a merge commit with exactly two parents"
  exit 1
fi

EXPECTED_RELEASE_HEAD=$(git rev-parse "$REMOTE_RELEASE_BRANCH")
if echo "$LAST_COMMIT_PARENTS" | jq -e --arg rel "$EXPECTED_RELEASE_HEAD" 'index($rel) != null' > /dev/null; then
  LINEAR_HEAD=$(echo "$LAST_COMMIT_PARENTS" | jq -r '[.[] | select(. != $rel)][0]' --arg rel "$EXPECTED_RELEASE_HEAD")
else
  echo "❌ Last commit must merge the release branch ($RELEASE_BRANCH)"
  exit 1
fi
echo "✅ Last commit correctly merges the previous commit and the release branch"
echo "Top commit of linear history: $LINEAR_HEAD"

MERGE_COMMIT_TREE=$(git rev-parse "$LAST_COMMIT^{tree}")
LINEAR_HEAD_TREE=$(git rev-parse "$LINEAR_HEAD^{tree}")

if [[ "$MERGE_COMMIT_TREE" != "$LINEAR_HEAD_TREE" ]]; then
  echo "❌ Tree of merge commit ($MERGE_COMMIT_TREE) does not match tree of linear history head ($LINEAR_HEAD_TREE)"
  echo "  This indicates that the merge of ${RELEASE_BRANCH} into this branch was not performed using the merge strategy 'ours'"
  exit 1
fi
echo "✅ Merge commit tree matches the linear history head"

EXPECTED_PREVIOUS_COMMIT="$LINEAR_HEAD"

# Now traverse down the history, ensuring each commit has exactly one parent
CURRENT_COMMIT="$EXPECTED_PREVIOUS_COMMIT"
while [[ "$CURRENT_COMMIT" != "$MERGE_BASE" ]]; do
  CURRENT_COMMIT_PARENTS=$(git cat-file -p "$CURRENT_COMMIT" | jq -sR '[capture("parent (?<parent>[0-9a-f]{40})"; "g") | .parent]')

  if [[ "$(echo "$CURRENT_COMMIT_PARENTS" | jq 'length')" -ne 1 ]]; then
    echo "❌ Commit $CURRENT_COMMIT must have exactly one parent"
    exit 1
  fi

  NEXT_COMMIT=$(echo "$CURRENT_COMMIT_PARENTS" | jq -r '.[0]')

  if [[ "$NEXT_COMMIT" == "$MERGE_BASE" ]]; then
    echo "✅ Reached merge base ($MERGE_BASE)"
  elif [[ -z "$NEXT_COMMIT" ]]; then
    echo "❌ Unexpected end of commit history before reaching merge base"
    exit 1
  fi

  # Move to the next commit in the chain
  CURRENT_COMMIT="$NEXT_COMMIT"
done

echo "✅ All commits are properly ordered and linear"
echo "✅ Release PR structure is valid"

echo

echo "Commits that are part of this release:"
git log --oneline "$MERGE_BASE..$LINEAR_HEAD"
