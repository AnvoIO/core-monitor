# Contributing to Core Monitor

Interested in contributing? That's great! Here are some guidelines to help you get started.

## Reporting An Issue

If you're about to raise an issue because you think you've found a problem with core-monitor, or you'd like to make a request for a new feature, or any other reason, please read this first.

The GitHub issue tracker is the preferred channel for [bug reports](#bug-reports), [feature requests](#feature-requests), and [change requests](#change-requests), but please respect the following restrictions:

* Please **search for existing issues**. Help us keep duplicate issues to a minimum by checking to see if someone has already reported your problem or requested your idea.

* Please **be civil**. Keep the discussion on topic and respect the opinions of others.

### Bug Reports

A bug is a _demonstrable problem_ that is caused by the code in the repository. Good bug reports are extremely helpful!

Guidelines for bug reports:

1. **Use the GitHub issue search** — check if the issue has already been reported.
2. **Check if the issue has been fixed** — try to reproduce it using the latest `main` branch.
3. **Isolate the problem** — ideally create a reduced test case.

A good bug report shouldn't leave others needing to chase you up for more information. Please try to be as detailed as possible in your report.

### Feature Requests

Feature requests are welcome. But take a moment to find out whether your idea fits with the scope and aims of the project. It's up to *you* to make a strong case to convince the project's developers of the merits of this feature. Please provide as much detail and context as possible.

### Change Requests

Change requests cover both architectural and functional changes to how core-monitor works. If you have an idea for a new or different dependency, a refactor, or an improvement to a feature, etc., please first discuss it in an issue.

## Working on Core Monitor

### Feature Branches

To get it out of the way:

- **main** is the development branch.
- **Never** push directly to `main`.

To start work on a new change:

1. Fork and/or clone the repository.
2. Create a new branch off `main`: `git checkout -b my-feature main`
3. Make your changes, following the coding style of the existing codebase.
4. Write or update tests as appropriate.
5. Push your branch and submit a pull request against `main`.

### Submitting Pull Requests

Pull requests are welcome! Before submitting:

1. Ensure all tests pass: `npm test`
2. Ensure the project builds: `npm run build`
3. Update documentation if your changes affect it.
4. Write a clear PR description explaining the change and its motivation.

### Testing and Quality Assurance

- Unit tests: `npm run test:unit`
- Integration tests: `npm run test:integration`
- E2E tests: `npm run test:e2e` (requires network access to SHiP endpoints)
- Full suite: `npm test`

## Conduct

We are committed to providing a friendly, safe, and welcoming environment for all. Please be kind and courteous. There's no need to be mean or rude.

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/1/4/code-of-conduct/) code of conduct, version 1.4. Please read and follow it.

## Contributor License & Acknowledgments

All contributions are made under the MIT License. For more details, see the [LICENSE](LICENSE) file.

Whenever you make a contribution to this project, you agree to the [Developer Certificate of Origin (DCO)](DCO). This is not a copyright assignment; it simply means that you assert you are legally permitted to make the contribution and that you agree to the terms of the project's license.

## References

This contributing guide is adapted from the [MathJax](https://github.com/mathjax/MathJax/blob/master/CONTRIBUTING.md) contributing guide and the [Contributor Covenant](https://www.contributor-covenant.org/).
