package com.nishtahir

open class CargoExtension {
    var module: String = ""
    var targets: List<String> = emptyList()

    /**
     * The Cargo [release profile](https://doc.rust-lang.org/book/second-edition/ch14-01-release-profiles.html#customizing-builds-with-release-profiles) to build.
     *
     * Defaults to `"debug"`.
     */
    var profile: String = "debug"

    /**
     * The target directory into Cargo which writes built outputs.
     *
     * Defaults to `${module}/target`.
     */
    var targetDirectory: String? = null

    /**
     * Which Cargo built outputs to consider JNI libraries.
     *
     * Defaults to `"*.so"`.
     */
    var targetInclude: String = "*.so"
}
