## Getting ready to go

You'll need [Leiningen](http://leiningen.org).

```
# If you use nvm.
nvm use 6

lein deps
npm install

# If you want a decent REPL.
brew install rlwrap
```

## To run a ClojureScript REPL

```
rlwrap lein run -m clojure.main repl.clj
```

Run `lein cljsbuild auto advanced` to generate JavaScript into `target/`.

### Preparing an NPM release

The intention is that the `release-js/` directory is roughly the shape of an npm-ready JavaScript package.

To generate a require/import-ready `release-js/datomish.js`, run
```
lein cljsbuild once release
```
To verify that importing into Node.js succeeds, run
```
node release-js/test
```

Many thanks to ([David Nolen](https://github.com/swannodette)) and ([Nikita Prokopov](https://github.com/tonsky)) for demonstrating how to package ClojureScript for distribution via npm.
