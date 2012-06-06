# MongoDB Ruby Driver Release Plan

This is a description of a formalized release plan that will take effect
with version 1.3.0.

## Semantic versioning

The most significant difference is that releases will now adhere to the conventions of
[semantic versioning](http://semver.org). In particular, we will strictly abide by the
following release rules:

1. Patch versions of the driver (Z in x.y.Z) will be released only when backward-compatible bug fixes are introduced. A bug fix is defined as an internal change that fixes incorrect behavior.

2. Minor versions (Y in x.Y.z) will be released if new, backward-compatible functionality is introduced to the public API.

3. Major versions (X in X.y.z) will be incremented if any backward-incompatible changes are introduced to the public API.

This policy will clearly indicate to users when an upgrade may affect their code. As a side effect, version numbers will climb more quickly than before.


## Release checklist

Before each relese to Rubygems.org, the following steps will be taken:

1. All driver tests will be run on Linux, OS X, and Windows via continuous integration system.

2. Update the HISTORY file and document all significant commits.

3. Update the version in lib/bson.rb, lib/mongo/version.rb, and ext/cbson/version.h.

4. Commit: "RELEASE [VERSION]"

5. git tag [version]

6. Build gems. Ensure that they have the correct versions.

7. Push tags and commit to GitHub (git push origin master, git push --tags).

8. Build and push docs. (git: mongodb/apidocs)

9. Push gems to Rubygems.org.

10. Test that the gem is downloadable from Rubygems.org.

11. Close out release in JIRA.

12. Annouce release on mongodb-user and mongodb-dev.

## Rake Deploy Tasks
1. rake deploy:change_version[x.x.x]
2. rake deploy:git_prepare
3. rake deploy:git_push
4. rake deploy:gem_build
5. rake deploy:gem_push
