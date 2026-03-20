# Claude Code Guidelines
## Git Commits
- Always set the commit author to the user who is prompting Claude. If the user's name and email are not known, ask before making any commits.
- Always add `Co-authored-by: claude <noreply@anthropic.com>` as a trailer in the commit message

## Code Quality
- Run `cargo fmt --all` and `cargo clippy --all-targets` before committing and fix any issues
