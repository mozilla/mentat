package com.nishtahir

import org.gradle.api.Project
import org.gradle.api.plugins.ExtensionContainer
import java.io.File
import kotlin.reflect.KClass

const val TOOLS_FOLDER = ".cargo/toolchain"

operator fun <T : Any> ExtensionContainer.get(type: KClass<T>): T = getByType(type.java)



fun Project.getToolchainDirectory(): File = File(projectDir, TOOLS_FOLDER)

