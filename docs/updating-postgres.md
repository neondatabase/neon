# Updating Postgres

## Minor Versions

When upgrading to a new minor version of Postgres, please follow these steps:

_Example: 15.4 is the new minor version to upgrade to from 15.3._

1. Clone the Neon Postgres repository if you have not done so already.

    ```shell
    git clone git@github.com:neondatabase/postgres.git
    ```

1. Add the Postgres upstream remote.

    ```shell
    git remote add upstream https://git.postgresql.org/git/postgresql.git
    ```

1. Create a new branch based on the stable branch you are updating.

    ```shell
    git checkout -b my-branch-15 REL_15_STABLE_neon
    ```

1. Find the upstream release tags you're looking for. They are of the form `REL_X_Y`.

1. Merge the upstream tag into the branch you created on the tag and resolve any conflicts.

    ```shell
    git fetch upstream REL_15_4
    git merge REL_15_4
    ```

    In the commit message of the merge commit, mention if there were
    any non-trivial conflicts or other issues.

1. Run the Postgres test suite to make sure our commits have not affected
Postgres in a negative way.

    ```shell
    make check
    # OR
    meson test -C builddir
    ```

1. Push your branch to the Neon Postgres repository.

    ```shell
    git push origin my-branch-15
    ```

1. Clone the Neon repository if you have not done so already.

    ```shell
    git clone git@github.com:neondatabase/neon.git
    ```

1. Create a new branch.

1. Change the `revisions.json` file to point at the HEAD of your Postgres
branch.

1. Update the Git submodule.

    ```shell
    git submodule set-branch --branch my-branch-15 vendor/postgres-v15
    git submodule update --remote vendor/postgres-v15
    ```

1. Run the Neon test suite to make sure that Neon is still good to go on this
minor Postgres release.

    ```shell
    ./scripts/poetry -k pg15
    ```

1. Commit your changes.

1. Create a pull request, and wait for CI to go green.

1. Push the Postgres branches with the merge commits into the Neon Postgres repository.

    ```shell
    git push origin my-branch-15:REL_15_STABLE_neon
    ```

1. Update your Neon PR to point at the branches.

    ```shell
    git submodule set-branch --branch REL_15_STABLE_neon vendor/postgres-v15
    git commit --amend --no-edit
    git push --force origin
    ```

1. Merge the pull request after getting approval(s) and CI completion.
