#!/bin/bash

if [[ "$TRAVIS_PULL_REQUEST" = "false" && "$TRAVIS_BRANCH" == "master" ]]; then
    cargo doc &&
    echo "<meta http-equiv=refresh content=0;url=mentat/index.html>" > target/doc/index.html &&
    git clone https://github.com/davisp/ghp-import.git &&
    ./ghp-import/ghp_import.py -n -p -f -r https://"$GH_TOKEN"@github.com/"$TRAVIS_REPO_SLUG.git" target/doc
fi
