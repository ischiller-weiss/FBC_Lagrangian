# Contribution guidelines

Contributions are very welcome. Feel free to open issues and PRs.

## Developing

This repository uses `pre-commit` to create consistent source code. Please install pre-commit with `pip install pre-commit` and activate the git-hooks of this repository with `pre-commit install` before doing the first commit.

If pre-commit finds errors, the commit is stopped and the errors need to be fixed, although often pre-commits hooks are able to fix them themselves and only a `git add -p` followed by a `git commit -m "your commit message"` needs to follow.
