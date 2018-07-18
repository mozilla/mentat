package com.nishtahir;

import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.tasks.TaskAction
import java.io.File

open class CargoBuildTask : DefaultTask() {

    @Suppress("unused")
    @TaskAction
    fun build() = with(project) {
        extensions[CargoExtension::class].apply {
            targets.forEach { target ->
                val toolchain = toolchains.find { (arch) -> arch == target }
                if (toolchain != null) {
                    buildProjectForTarget(project, toolchain, this)

                    val targetDirectory = targetDirectory ?: "${module}/target"

                    copy { spec ->
                        spec.into(File(buildDir, "rustJniLibs/${toolchain.folder}"))
                        spec.from(File(project.projectDir, "${targetDirectory}/${toolchain.target}/${profile}"))
                        spec.include(targetInclude)
                    }
                } else {
                    println("No such target $target")
                }
            }
        }
    }

    private fun buildProjectForTarget(project: Project, toolchain: Toolchain, cargoExtension: CargoExtension) {
        project.exec { spec ->
            val cc = "${project.getToolchainDirectory()}/${toolchain.cc()}"
            val ar = "${project.getToolchainDirectory()}/${toolchain.ar()}"
            println("using CC: $cc")
            println("using AR: $ar")
            with(spec) {
                standardOutput = System.out
                workingDir = File(project.project.projectDir, cargoExtension.module)
                environment("CC", cc)
                environment("AR", ar)
                environment("RUSTFLAGS", "-C linker=$cc")
                commandLine = listOf("cargo", "build", "--target=${toolchain.target}")
                if (cargoExtension.profile != "debug") {
                     // Cargo is rigid: it accepts "--release" for release (and
                     // nothing for dev).  This is a cheap way of allowing only
                     // two values.
                     commandLine.add("--${cargoExtension.profile}")
                }
            }
        }.assertNormalExitValue()
    }
}
