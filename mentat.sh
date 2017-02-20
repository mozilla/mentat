#!/bin/sh

doc() {
  cargo doc \
      --package edn \
      --package mentat_parser_utils \
      --package mentat_core \
      --package mentat_sql \
      --package mentat_db \
      --package mentat_query \
      --package mentat_query_parser \
      --package mentat_query_algebrizer \
      --package mentat_query_translator \
      --package mentat_tx_parser \
      --no-deps
}

test() {
  cargo test \
      --package edn \
      --package mentat_parser_utils \
      --package mentat_core \
      --package mentat_sql \
      --package mentat_db \
      --package mentat_query \
      --package mentat_query_parser \
      --package mentat_query_algebrizer \
      --package mentat_query_translator \
      --package mentat_tx_parser \
      -- --nocapture
}

case $1 in
  "doc" | "d")
    doc
    ;;

  "test" | "t")
    test
    ;;

  *)
    echo "Usage: $0 [test|doc]"
    ;;
esac
