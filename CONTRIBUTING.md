# Contributing

Thank you for your interest in contributing!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/REPO_NAME.git`
3. Create a feature branch: `git checkout -b feat/your-feature`
4. Install dependencies (see README.md)
5. Make your changes
6. Run tests: `pytest` or `npm test` depending on the stack
7. Commit with conventional commits: `git commit -m "feat: add X"`
8. Push and open a Pull Request

## Commit Message Format

```
<type>: <short description>

Types: feat | fix | docs | refactor | test | chore | perf | ci
```

## Code Standards

- Python: follow PEP 8, use type hints, run `ruff check`
- TypeScript: strict mode, no `any`, run `tsc --noEmit`
- All new features need tests (target 80%+ coverage)
- No hardcoded secrets — use environment variables

## Pull Request Checklist

- [ ] Tests pass locally
- [ ] No secrets or credentials in code
- [ ] README updated if behaviour changed
- [ ] One focused change per PR

## Reporting Issues

Open a GitHub Issue with:
- What you expected vs what happened
- Steps to reproduce
- Environment (OS, Python/Node version)

## License

By contributing you agree your work is released under this project's [MIT License](LICENSE).
