# Issue Tracker: GitHub

Issues and PRDs for this repo live as GitHub issues in `makoMakoGo/novel-proofer`. Use the `gh` CLI from inside the repository so it infers the correct remote.

## Conventions

- Issue titles must start with one of the repository title prefixes:
  - `[Bug]`
  - `[Feature]`
  - `[Suggestion]`
  - `[General]`
- Create an issue with `gh issue create --title "..." --body "..."`.
- Read an issue with `gh issue view <number> --comments`.
- List issues with `gh issue list --state open --json number,title,body,labels,comments`.
- Comment with `gh issue comment <number> --body "..."`.
- Apply or remove labels with `gh issue edit <number> --add-label "..."` and `--remove-label "..."`.
- Close with `gh issue close <number> --comment "..."`.

## Publishing

When an engineering skill says to publish a PRD or issue to the issue tracker, create a GitHub issue.
