### What

[TODO: Short statement about what is changing.]

### Why

[TODO: Why this change is being made. Include any context required to understand the why.]

### Known limitations

[TODO or N/A]

### Issue that this PR addresses

[TODO: Attach the link to the GitHub issue or task. Include the priority of the task here in addition to the link.]

### Checklist

#### PR Structure

- [ ] It is not possible to break this PR down into smaller PRs.
- [ ] This PR does not mix refactoring changes with feature changes.
- [ ] This PR's title starts with name of package that is most changed in the PR, or `all` if the changes are broad or impact many packages.

#### Thoroughness

- [ ] This PR adds tests for the new functionality or fixes.
- [ ] All updated queries have been tested (refer to [this](https://stackoverflow.com/a/29163465) check if the data set returned by the updated query is expected to be same as the original one).

#### Release

- [ ] This is not a breaking change.
- [ ] This is ready to be tested in development.
- [ ] The new functionality is gated with a feature flag if this is not ready for production.
