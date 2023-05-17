//
// The script parses Allure reports and posts a comment with a summary of the test results to the PR.
//
// The comment is updated on each run with the latest results.
//
// It is designed to be used with actions/github-script from GitHub Workflows:
// - uses: actions/github-script@v6
//   with:
//     script: |
//       const script = require("./scripts/pr-comment-test-report.js")
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
    // Let users know that the comment is updated automatically
    const autoupdateNotice = `<div align="right"><sub>The comment gets automatically updated with the latest test results :recycle:</sub></div>`
    // GitHub bot id taken from (https://api.github.com/users/github-actions[bot])
    const githubActionsBotId = 41898282
    // The latest commit in the PR URL
    const commitUrl = `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/pull/${context.payload.number}/commits/${context.payload.pull_request.head.sha}`
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
    const buildTypes = new Set()

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
                buildTypes.add(buildType)

                // Removing build type and PostgreSQL version from the test name to make it shorter
                const testName = test.name.replace(new RegExp(`${buildType}-pg${pgVersion}-?`), "").replace("[]", "")
                test.pytestName = `${parentSuite.name.replace(".", "/")}/${suite.name}.py::${testName}`

                if (test.status === "passed") {
                    passedTests[pgVersion][buildType].push(test)
                    passedTestsCount += 1
                } else if (test.status === "failed" || test.status === "broken") {
                    failedTests[pgVersion][buildType].push(test)
                    failedTestsCount += 1
                } else if (test.status === "skipped") {
                    skippedTests[pgVersion][buildType].push(test)
                    skippedTestsCount += 1
                }

                if (test.retriesCount > 0) {
                    retriedTests[pgVersion][buildType].push(test)

                    if (test.retriesStatusChange) {
                        flakyTests[pgVersion][buildType].push(test)
                        flakyTestsCount += 1
                    }
                }
            }
        }
    }

    const totalTestsCount = failedTestsCount + passedTestsCount + skippedTestsCount
    commentBody += `### ${totalTestsCount} tests run: ${passedTestsCount} passed, ${failedTestsCount} failed, ${skippedTestsCount} skipped ([full report](${reportUrl}) for ${commitUrl})\n___\n`

    // Print test resuls from the newest to the oldest PostgreSQL version for release and debug builds.
    for (const pgVersion of Array.from(pgVersions).sort().reverse()) {
        for (const buildType of Array.from(buildTypes).sort().reverse()) {
            if (failedTests[pgVersion][buildType].length > 0) {
                commentBody += `#### PostgreSQL ${pgVersion} (${buildType} build)\n\n`
                commentBody += `Failed tests:\n`
                for (const test of failedTests[pgVersion][buildType]) {
                    const allureLink = `${reportUrl}#suites/${test.parentUid}/${test.uid}`

                    commentBody += `- [\`${test.pytestName}\`](${allureLink})`
                    if (test.retriesCount > 0) {
                        commentBody += ` (ran [${test.retriesCount + 1} times](${allureLink}/retries))`
                    }
                    commentBody += "\n"
                }
                commentBody += "\n"
            }
        }
    }

    if (flakyTestsCount > 0) {
        commentBody += "<details>\n<summary>Flaky tests</summary>\n\n"
        for (const pgVersion of Array.from(pgVersions).sort().reverse()) {
            for (const buildType of Array.from(buildTypes).sort().reverse()) {
                if (flakyTests[pgVersion][buildType].length > 0) {
                    commentBody += `#### PostgreSQL ${pgVersion} (${buildType} build)\n\n`
                    for (const test of flakyTests[pgVersion][buildType]) {
                        const status = test.status === "passed" ? ":white_check_mark:" : ":x:"
                        commentBody += `- ${status} [\`${test.pytestName}\`](${reportUrl}#suites/${test.parentUid}/${test.uid}/retries)\n`
                    }
                    commentBody += "\n"
                }
            }
        }
        commentBody += "\n</details>\n"
    }

    commentBody += autoupdateNotice

    const { data: comments } = await github.rest.issues.listComments({
        issue_number: context.payload.number,
        ...ownerRepoParams,
    })

    const comment = comments.find(comment => comment.user.id === githubActionsBotId && comment.body.startsWith(startMarker))
    if (comment) {
        await github.rest.issues.updateComment({
            comment_id: comment.id,
            body: commentBody,
            ...ownerRepoParams,
        })
    } else {
        await github.rest.issues.createComment({
            issue_number: context.payload.number,
            body: commentBody,
            ...ownerRepoParams,
        })
    }
}
