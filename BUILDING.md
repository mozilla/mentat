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

## To run a REPL

```
rlwrap lein run -m clojure.main repl.clj
```
