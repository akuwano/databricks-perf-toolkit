# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

### How to Report

1. **Do not** create a public GitHub issue for security vulnerabilities
2. Open a private security advisory at: https://github.com/akuwano/databricks-perf-toolkit/security/advisories/new
3. Alternatively, contact the maintainer directly

### What to Include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Initial Response**: Within 48 hours
- **Status Update**: Within 7 days
- **Fix Timeline**: Depends on severity

### Scope

This security policy applies to:
- The main codebase in this repository
- Dependencies used by this project

### Out of Scope

- Vulnerabilities in Databricks platform itself
- Issues related to user-provided credentials or configuration

## Security Best Practices for Users

1. **Never commit credentials**: Use environment variables for `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
2. **Review generated reports**: Before sharing reports, ensure they don't contain sensitive query data
3. **Keep dependencies updated**: Regularly update the `openai` package

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who report valid vulnerabilities.
