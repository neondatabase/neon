// The script is designed to be used in actions/github-script from GitHub Workflows::
//
// - uses: actions/github-script@v6
//   with:
//     script: |
//       const script = require("./scripts/pr-comment-test-report.js")
//       await script({
//         github,
//         context,
//         fetch,
//         reportUrlDebug: "...",
//         reportUrlRelease: "...",
//       })

module.exports = async ({ github, context, fetch, reportUrlDebug, reportUrlRelease }) => {
    // Marker to find the comment in the subsequent runs
    const startMarker = `<!--AUTOMATIC COMMENT START #${context.payload.number}-->`
    // GitHub bot id taken from (https://api.github.com/users/github-actions[bot])
    const githubActionsBotId = 41898282
    // The latest commit in the PR URL
    const commitUrl = `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/pull/${context.payload.number}/commits/${context.sha}`
    // Commend body itself
    let commentBody = `${startMarker}\n### Test results for ${commitUrl}:\n___\n`

    // Common parameters for GitHub API requests
    const ownerRepoParams = {
        owner: context.repo.owner,
        repo: context.repo.repo,
    }

    for (const reportUrl of [reportUrlDebug, reportUrlRelease]) {
        if (!reportUrl) {
            continue
        }

        const buildType = (reportUrl === reportUrlDebug) ? "Debug" : "Release"

        const suitesJsonUrl = reportUrl.replace("/index.html", "/data/suites.json")
        const suites = await (await fetch(suitesJsonUrl)).json()

        // Allure distinguishes "failed" (with an assertion error) and "broken" (with any other error) tests.
        // For this report it's ok to treat them in the same way (as failed).
        failedTests = []
        passedTests = []
        skippedTests = []

        retriedTests = []
        retriedStatusChangedTests = []

        for (const parentSuite of suites.children) {
            for (const suite of parentSuite.children) {
                for (const test of suite.children) {
                    pytestName = `${parentSuite.name.replace(".", "/")}/${suite.name}.py::${test.name}`
                    test.pytestName = pytestName

                    if (test.status === "passed") {
                        passedTests.push(test);
                    } else if (test.status === "failed" || test.status === "broken") {
                        failedTests.push(test);
                    } else if (test.status === "skipped") {
                        skippedTests.push(test);
                    }

                    if (test.retriesCount > 0) {
                        retriedTests.push(test);

                        if (test.retriedStatusChangedTests) {
                            retriedStatusChangedTests.push(test);
                        }
                    }
                }
            }
        }

        totalTestsCount = failedTests.length + passedTests.length + skippedTests.length
        commentBody += `#### ${buildType} build: ${totalTestsCount} tests run: ${passedTests.length} passed, ${failedTests.length} failed, ${skippedTests.length} ([full report](${reportUrl}))\n`
        if (failedTests.length > 0) {
            commentBody += `Failed tests:\n`
            for (const test of failedTests) {
                const allureLink = `${reportUrl}#suites/${test.parentUid}/${test.uid}`

                commentBody += `- [\`${test.pytestName}\`](${allureLink})`
                if (test.retriesCount > 0) {
                    commentBody += ` (ran [${test.retriesCount + 1} times](${allureLink}/retries))`
                }
                commentBody += "\n"
            }
            commentBody += "\n"
        }
        if (retriedStatusChangedTests > 0) {
            commentBody += `Flaky tests:\n`
            for (const test of retriedStatusChangedTests) {
                const status = test.status === "passed" ? ":white_check_mark:" : ":x:"
                commentBody += `- ${status} [\`${test.pytestName}\`](${reportUrl}#suites/${test.parentUid}/${test.uid}/retries)\n`
            }
            commentBody += "\n"
        }
        commentBody += "___\n"
    }

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
