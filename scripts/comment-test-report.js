//
// The script parses Allure reports and posts a comment with a summary of the test results to the PR or to the latest commit in the branch.
//
// The comment is updated on each run with the latest results.
//
// It is designed to be used with actions/github-script from GitHub Workflows:
// - uses: actions/github-script@v6
//   with:
//     script: |
//       const script = require("./scripts/comment-test-report.js")
//       await script({
//         github,
//         context,
//         fetch,
//         report: {
//           reportUrl: "...",
//           reportJsonUrl: "...",
//         },
//       })
//

// Analog of Python's defaultdict.
//
// const dm = new DefaultMap(() => new DefaultMap(() => []))
// dm["firstKey"]["secondKey"].push("value")
//
class DefaultMap extends Map {
    constructor(getDefaultValue) {
        return new Proxy({}, {
            get: (target, name) => name in target ? target[name] : (target[name] = getDefaultValue(name))
        })
    }
}

module.exports = async ({ github, context, fetch, report }) => {
    // Marker to find the comment in the subsequent runs
    const startMarker = `<!--AUTOMATIC COMMENT START #${context.payload.number}-->`
    // If we run the script in the PR or in the branch (main/release/...)
    const isPullRequest = !!context.payload.pull_request
    // Latest commit in PR or in the branch
    const commitSha = isPullRequest ? context.payload.pull_request.head.sha : context.sha
    // Let users know that the comment is updated automatically
    const autoupdateNotice = `<div align="right"><sub>The comment gets automatically updated with the latest test results<br>${commitSha} at ${new Date().toISOString()} :recycle:</sub></div>`
    // GitHub bot id taken from (https://api.github.com/users/github-actions[bot])
    const githubActionsBotId = 41898282
    // Commend body itself
    let commentBody = `${startMarker}\n`

    // Common parameters for GitHub API requests
    const ownerRepoParams = {
        owner: context.repo.owner,
        repo: context.repo.repo,
    }

    const {reportUrl, reportJsonUrl} = report

    if (!reportUrl || !reportJsonUrl) {
        commentBody += `#### No tests were run or test report is not available\n`
        commentBody += autoupdateNotice
        return
    }

    const suites = await (await fetch(reportJsonUrl)).json()

    // Allure distinguishes "failed" (with an assertion error) and "broken" (with any other error) tests.
    // For this report it's ok to treat them in the same way (as failed).
    const failedTests = new DefaultMap(() => new DefaultMap(() => []))
    const passedTests = new DefaultMap(() => new DefaultMap(() => []))
    const skippedTests = new DefaultMap(() => new DefaultMap(() => []))
    const retriedTests = new DefaultMap(() => new DefaultMap(() => []))
    const flakyTests = new DefaultMap(() => new DefaultMap(() => []))

    let failedTestsCount = 0
    let passedTestsCount = 0
    let skippedTestsCount = 0
    let flakyTestsCount = 0

    const pgVersions = new Set()

    for (const parentSuite of suites.children) {
        for (const suite of parentSuite.children) {
            for (const test of suite.children) {
                let buildType, pgVersion
                const match = test.name.match(/[\[-](?<buildType>debug|release)-pg(?<pgVersion>\d+)[-\]]/)?.groups
                if (match) {
                    ({buildType, pgVersion} = match)
                } else {
                    // It's ok, we embed BUILD_TYPE and Postgres Version into the test name only for regress suite and do not for other suites (like performance).
                    console.info(`Cannot get BUILD_TYPE and Postgres Version from test name: "${test.name}", defaulting to "release" and "14"`)

                    buildType = "release"
                    pgVersion = "14"
                }

                pgVersions.add(pgVersion)

                // Removing build type and PostgreSQL version from the test name to make it shorter
                const testName = test.name.replace(new RegExp(`${buildType}-pg${pgVersion}-?`), "").replace("[]", "")
                test.pytestName = `${parentSuite.name.replace(".", "/")}/${suite.name}.py::${testName}`
                test.pgVersion = pgVersion
                test.buildType = buildType

                if (test.status === "passed") {
                    passedTests[pgVersion][testName].push(test)
                    passedTestsCount += 1
                } else if (test.status === "failed" || test.status === "broken") {
                    failedTests[pgVersion][testName].push(test)
                    failedTestsCount += 1
                } else if (test.status === "skipped") {
                    skippedTests[pgVersion][testName].push(test)
                    skippedTestsCount += 1
                }

                if (test.retriesCount > 0) {
                    retriedTests[pgVersion][testName].push(test)

                    if (test.retriesStatusChange) {
                        flakyTests[pgVersion][testName].push(test)
                        flakyTestsCount += 1
                    }
                }
            }
        }
    }

    const totalTestsCount = failedTestsCount + passedTestsCount + skippedTestsCount
    commentBody += `### ${totalTestsCount} tests run: ${passedTestsCount} passed, ${failedTestsCount} failed, ${skippedTestsCount} skipped ([full report](${reportUrl}))\n___\n`

    // Print test resuls from the newest to the oldest Postgres version for release and debug builds.
    for (const pgVersion of Array.from(pgVersions).sort().reverse()) {
        if (Object.keys(failedTests[pgVersion]).length > 0) {
            commentBody += `#### Failures on Posgres ${pgVersion}\n\n`
            for (const [testName, tests] of Object.entries(failedTests[pgVersion])) {
                const links = []
                for (const test of tests) {
                    const allureLink = `${reportUrl}#suites/${test.parentUid}/${test.uid}`
                    links.push(`[${test.buildType}](${allureLink})`)
                }
                commentBody += `- \`${testName}\`: ${links.join(", ")}\n`
            }

            const testsToRerun = Object.values(failedTests[pgVersion]).map(x => x[0].name)
            const command = `DEFAULT_PG_VERSION=${pgVersion} scripts/pytest -k "${testsToRerun.join(" or ")}"`

            commentBody += "```\n"
            commentBody += `# Run failed on Postgres ${pgVersion} tests locally:\n`
            commentBody += `${command}\n`
            commentBody += "```\n"
        }
    }

    if (flakyTestsCount > 0) {
        commentBody += `<details>\n<summary>Flaky tests (${flakyTestsCount})</summary>\n\n`
        for (const pgVersion of Array.from(pgVersions).sort().reverse()) {
            if (Object.keys(flakyTests[pgVersion]).length > 0) {
                commentBody += `#### Postgres ${pgVersion}\n\n`
                for (const [testName, tests] of Object.entries(flakyTests[pgVersion])) {
                    const links = []
                    for (const test of tests) {
                        const allureLink = `${reportUrl}#suites/${test.parentUid}/${test.uid}/retries`
                        const status = test.status === "passed" ? ":white_check_mark:" : ":x:"
                        links.push(`[${status} ${test.buildType}](${allureLink})`)
                    }
                    commentBody += `- \`${testName}\`: ${links.join(", ")}\n`
                }
            }
        }
        commentBody += "\n</details>\n"
    }

    commentBody += autoupdateNotice

    let createCommentFn, listCommentsFn, updateCommentFn, issueNumberOrSha
    if (isPullRequest) {
        createCommentFn  = github.rest.issues.createComment
        listCommentsFn   = github.rest.issues.listComments
        updateCommentFn  = github.rest.issues.updateComment
        issueNumberOrSha = {
            issue_number: context.payload.number,
        }
    } else {
        updateCommentFn  = github.rest.repos.updateCommitComment
        listCommentsFn   = github.rest.repos.listCommentsForCommit
        createCommentFn  = github.rest.repos.createCommitComment
        issueNumberOrSha = {
            commit_sha: commitSha,
        }
    }

    const { data: comments } = await listCommentsFn({
        ...issueNumberOrSha,
        ...ownerRepoParams,
    })

    const comment = comments.find(comment => comment.user.id === githubActionsBotId && comment.body.startsWith(startMarker))
    if (comment) {
        await updateCommentFn({
            comment_id: comment.id,
            body: commentBody,
            ...ownerRepoParams,
        })
    } else {
        await createCommentFn({
            body: commentBody,
            ...issueNumberOrSha,
            ...ownerRepoParams,
        })
    }
}
