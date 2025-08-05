# Documentation Standards

## File Naming Conventions
- Use `lowercase_with_underscores` for all documentation and code files (except task files, which remain capitalized for clarity)
- No spaces, dashes, or capital letters in filenames (except TASK files)

## Folder Organization Rules
- Business and technical documentation: `docs/`
- Database-specific documentation: `db/docs/`
- AI memory and task tracking: `memory-bank/`

## Documentation Hierarchy
```
docs/
├── architecture/
├── analysis/
├── plans/
├── api/
├── user_guide/
├── deployment/
└── standards/

db/docs/
├── schema/
├── migrations/
└── performance/

memory-bank/
├── tasks/
├── activeContext.md
├── progress.md
└── (core AI context files)
```

## AI Memory-Bank vs Business Documentation
- `memory-bank/` is for AI context, task tracking, and progress only
- All business, technical, and project documentation must reside in `docs/` or `db/docs/`

## Updating Documentation
- Update documentation and references whenever files are moved or renamed
- Keep README files up to date with folder structure and standards

## Review
- Review documentation structure quarterly or after major refactors
- Remove obsolete files and folders promptly
