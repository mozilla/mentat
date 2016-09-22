cp ../target/release-browser/datomish.js release/
node_modules/.bin/webpack -p
cat src/wrapper.prefix built/index.js > release/index.js
