# How to contribute to Datomish

This project is very new, so we'll probably revise these guidelines. Please
comment on a bug before putting significant effort in, if you'd like to
contribute.

## Guidelines

* Follow the Style Guide (see below).
* Keep work branches in your own GitHub fork; rebase your own branches at will.
* Squash or rebase branches before merging to master so the commits make sense
on their own.
* Get a SGTM from someone relevant before merging.
* Keep commits to master bisect-safe (i.e., each commit should pass all tests).
* Sign-off commits before merging (see below).
* Make sure your commit message: references the issue or bug number, if there is
one; identifies the reviewers; and follows a readable style, with the long
description including any additional information that might help future
spelunkers (see below).

```
Frobnicate the URL bazzer before flattening pilchard, r=mossop,rnewman. Fixes #6.

The frobnication method used is as described in Podder's Miscellany, page 15.
Note that this pull request doesn't include tests, because we're bad people.

Signed-off-by: Random J Developer <random@developer.example.org>
```

## Example

* Fork this repo at [github.com/mozilla/datomish](https://github.com/mozilla/datomish#fork-destination-box).
* Clone your fork locally. Make sure you use the correct clone URL.
```
git clone git@github.com:YOURNAME/tofino.git
```
Check your remotes:
```
git remote --verbose
```
Make sure you have an upstream remote defined:
```
git remote add upstream https://github.com/mozilla/datomish
```

* Create a new branch to start working on a bug or feature:
```
git checkout -b some-new-branch
```

* Do some work, making sure you signoff every commit:
```
git commit --signoff --message "Some commit message"
```

* Rebase your work during development and before submitting a pull request,
avoiding merge commits, such that your commits are a logical sequence to
read rather than a record of your fevered typing.
* Make sure you're on the correct branch and are pulling from the correct upstream:
```
git checkout some-new-branch
git pull upstream master --rebase
```
Or using `git reset --soft` (as described in [a tale of three trees](http://www.infoq.com/presentations/A-Tale-of-Three-Trees))

* Update your fork with the local changes on your branch:
```
git push origin some-new-branch
```

* Submit a pull request. It would be helpful if you also flagged somebody
for review, by typing their `@username` in the comments section.

### Addressing review comments

#### Adding more commits
After submitting a pull request, certain review comments might need to be
addressed. All you have to do is commit your new work, and simply update
your fork with the local changes on your branch again. The pull request
will automatically update with your new changes.

#### Signoff earlier commits
If you forgot to signoff some earlier commits, do an incremental rebase
on the branch you're working on. Find the earliest commit hash you want to
change, e.g., "1234567" (via `git log`), then use it in the rebase command
to start an interactive rebase. Type `edit` instead of `pick` for the
commits you want to edit.
```
git rebase --interactive '1234567^'
git commit --amend --signoff --no-edit
git rebase --continue
```

#### Squashing earlier commits
While you're working, committing often is a good idea. However, it might
not make sense to have commits that are too granular or don't make sense
on their own before closing a pull request and merging back to upstream master.
Find the earliest commit hash you want to change, e.g., "1234567"
(via `git log`), then use it in the rebase command to start an interactive
rebase. Type `squash` instead of `pick` for the commits you want to squash
into their parents.
```
git rebase --interactive '1234567^'
```

#### Properly set name and email
Update your `.gitconfig` with the proper information. You might need to
update the earlier commits and sign them off as well, see above.
```
git config --global user.name "Foo Bar"
git config --global user.email john@doe.com
git commit --amend --reset-author --no-edit
```

# Style Guide

Our JavaScript code follows the [airbnb style](https://github.com/airbnb/javascript)
with a [few exceptions](../../blob/master/.eslintrc). The precise rules are
likely to change a little as we get started so for now let eslint be your guide.

Our ClojureScript code follows… well, no guide so far.

# How to sign-off your commits

To help tracking who did what, we have a "sign-off" procedure on patches. This
avoids the need for physically signed "[Committers|Contributors] License
Agreements".

The sign-off is a simple line at the end of the commit message, which certifies
that you wrote it or otherwise have the right to pass it on as an open-source
patch. The rules are pretty simple: if you can certify the below:

    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
        have the right to submit it under the open source license
        indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.

then you just add a line saying

    Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)

If you're using the command line, you can get this done automatically with

    $ git commit --signoff

Some GUIs (e.g. SourceTree) have an option to automatically sign commits.

If you need to slightly modify patches you receive in order to merge them,
because the code is not exactly the same in your tree and the submitters'.
If you stick strictly to rule (c), you should ask the submitter to submit, but
this is a totally counter-productive waste of time and energy.
Rule (b) allows you to adjust the code, but then it is very impolite to change
one submitter's code and make them endorse your bugs. To solve this problem,
it is recommended that you add a line between the last Signed-off-by header and
yours, indicating the nature of your changes. While there is nothing mandatory
about this, it seems like prepending the description with your mail and/or name,
all enclosed in square brackets, is noticeable enough to make it obvious that
you are responsible for last-minute changes. Example :

    Signed-off-by: Random J Developer <random@developer.example.org>
    [lucky@maintainer.example.org: struct foo moved from foo.c to foo.h]
    Signed-off-by: Lucky K Maintainer <lucky@maintainer.example.org>

This practice is particularly helpful if you maintain a stable branch and
want at the same time to credit the author, track changes, merge the fix,
and protect the submitter from complaints. Note that under no circumstances
can you change the author's identity (the From header), as it is the one
which appears in the change-log.
