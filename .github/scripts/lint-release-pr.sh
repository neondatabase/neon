#!/usr/bin/env bash

set -euo pipefail

DOCS_URL="https://docs.neon.build/overview/repositories/neon.html"

message() {
  if [[ -n "${GITHUB_PR_NUMBER:-}" ]]; then
    gh pr comment --repo "${GITHUB_REPOSITORY}" "${GITHUB_PR_NUMBER}" --edit-last --body "$1" \
      || gh pr comment --repo "${GITHUB_REPOSITORY}" "${GITHUB_PR_NUMBER}" --body "$1"
  fi
  echo "$1"
}

report_error() {
  message "❌ $1
  For more details, see the documentation: ${DOCS_URL}"

  exit 1
}

case "$RELEASE_BRANCH" in
  "release") COMPONENT="Storage" ;;
  "release-proxy") COMPONENT="Proxy" ;;
  "release-compute") COMPONENT="Compute" ;;
  *)
    report_error "Unknown release branch: ${RELEASE_BRANCH}"
    ;;
esac


# Identify main and release branches
MAIN_BRANCH="origin/main"
REMOTE_RELEASE_BRANCH="origin/${RELEASE_BRANCH}"

# Find merge base
MERGE_BASE=$(git merge-base "${MAIN_BRANCH}" "${REMOTE_RELEASE_BRANCH}")
echo "Merge base of ${MAIN_BRANCH} and ${RELEASE_BRANCH}: ${MERGE_BASE}"

# Get the HEAD commit (last commit in PR, expected to be the merge commit)
LAST_COMMIT=$(git rev-parse HEAD)

MERGE_COMMIT_MESSAGE=$(git log -1 --format=%s "${LAST_COMMIT}")
EXPECTED_MESSAGE_REGEX="^$COMPONENT release [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2} UTC$"

if ! [[ "${MERGE_COMMIT_MESSAGE}" =~ ${EXPECTED_MESSAGE_REGEX} ]]; then
  report_error "Merge commit message does not match expected pattern: '<component> release YYYY-MM-DD'
  Expected component: ${COMPONENT}
  Found: '${MERGE_COMMIT_MESSAGE}'"
fi
echo "✅ Merge commit message is correctly formatted: '${MERGE_COMMIT_MESSAGE}'"

LAST_COMMIT_PARENTS=$(git cat-file -p "${LAST_COMMIT}" | jq -sR '[capture("parent (?<parent>[0-9a-f]{40})"; "g") | .parent]')

if [[ "$(echo "${LAST_COMMIT_PARENTS}" | jq 'length')" -ne 2 ]]; then
  report_error "Last commit must be a merge commit with exactly two parents"
fi

EXPECTED_RELEASE_HEAD=$(git rev-parse "${REMOTE_RELEASE_BRANCH}")
if echo "${LAST_COMMIT_PARENTS}" | jq -e --arg rel "${EXPECTED_RELEASE_HEAD}" 'index($rel) != null' > /dev/null; then
  LINEAR_HEAD=$(echo "${LAST_COMMIT_PARENTS}" | jq -r '[.[] | select(. != $rel)][0]' --arg rel "${EXPECTED_RELEASE_HEAD}")
else
  report_error "Last commit must merge the release branch (${RELEASE_BRANCH})"
fi
echo "✅ Last commit correctly merges the previous commit and the release branch"
echo "Top commit of linear history: ${LINEAR_HEAD}"

MERGE_COMMIT_TREE=$(git rev-parse "${LAST_COMMIT}^{tree}")
LINEAR_HEAD_TREE=$(git rev-parse "${LINEAR_HEAD}^{tree}")

if [[ "${MERGE_COMMIT_TREE}" != "${LINEAR_HEAD_TREE}" ]]; then
  report_error "Tree of merge commit (${MERGE_COMMIT_TREE}) does not match tree of linear history head (${LINEAR_HEAD_TREE})
  This indicates that the merge of ${RELEASE_BRANCH} into this branch was not performed using the merge strategy 'ours'"
fi
echo "✅ Merge commit tree matches the linear history head"

EXPECTED_PREVIOUS_COMMIT="${LINEAR_HEAD}"

# Now traverse down the history, ensuring each commit has exactly one parent
CURRENT_COMMIT="${EXPECTED_PREVIOUS_COMMIT}"
while [[ "${CURRENT_COMMIT}" != "${MERGE_BASE}" && "${CURRENT_COMMIT}" != "${EXPECTED_RELEASE_HEAD}" ]]; do
  CURRENT_COMMIT_PARENTS=$(git cat-file -p "${CURRENT_COMMIT}" | jq -sR '[capture("parent (?<parent>[0-9a-f]{40})"; "g") | .parent]')

  if [[ "$(echo "${CURRENT_COMMIT_PARENTS}" | jq 'length')" -ne 1 ]]; then
    report_error "Commit ${CURRENT_COMMIT} must have exactly one parent"
  fi

  NEXT_COMMIT=$(echo "${CURRENT_COMMIT_PARENTS}" | jq -r '.[0]')

  if [[ "${NEXT_COMMIT}" == "${MERGE_BASE}" ]]; then
    echo "✅ Reached merge base (${MERGE_BASE})"
    PR_BASE="${MERGE_BASE}"
  elif [[ "${NEXT_COMMIT}" == "${EXPECTED_RELEASE_HEAD}" ]]; then
    echo "✅ Reached release branch (${EXPECTED_RELEASE_HEAD})"
    PR_BASE="${EXPECTED_RELEASE_HEAD}"
  elif [[ -z "${NEXT_COMMIT}" ]]; then
    report_error "Unexpected end of commit history before reaching merge base"
  fi

  # Move to the next commit in the chain
  CURRENT_COMMIT="${NEXT_COMMIT}"
done

echo "✅ All commits are properly ordered and linear"
echo "✅ Release PR structure is valid"

echo

message "Commits that are part of this release:
$(git log --oneline "${PR_BASE}..${LINEAR_HEAD}")"
