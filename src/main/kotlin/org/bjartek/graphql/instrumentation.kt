
package org.bjartek.graphql

import graphql.ExecutionInput
import graphql.execution.ExecutionContext
import graphql.execution.instrumentation.SimpleInstrumentation
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters
import graphql.language.Field
import graphql.language.SelectionSet
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import org.springframework.stereotype.Component

fun String.removeNewLines() =
        this.replace("\n", " ")
                .replace(Regex("\\s+"), " ")

@Component
class UsageInstrumentation : SimpleInstrumentation() {

    private val logger = LoggerFactory.getLogger(UsageInstrumentation::class.java)

    val fieldUsage = FieldUsage()
    val userUsage=UserUsage()

    override fun instrumentExecutionInput(
            executionInput: ExecutionInput?,
            parameters: InstrumentationExecutionParameters?
    ): ExecutionInput {

        executionInput?.let {
            if (it.operationName != "IntrospectionQuery") {
                val query = it.query.removeNewLines()
                if (query.trimStart().startsWith("mutation")) {
                    logger.info("mutation=\"$query\" - variable-keys=${it.variables.keys}")
                } else {
                    val variables = if (it.variables.isEmpty()) "" else " - variables=${it.variables}"
                    logger.info("query=\"$query\"$variables")
                }
            }
        }
        return super.instrumentExecutionInput(executionInput, parameters)
    }

    override fun instrumentExecutionContext(
            executionContext: ExecutionContext?,
            parameters: InstrumentationExecutionParameters?
    ): ExecutionContext {
        val selectionSet = executionContext?.operationDefinition?.selectionSet ?: SelectionSet(emptyList())

        if(executionContext?.operationDefinition?.name != "IntrospectionQuery") {
            fieldUsage.update(selectionSet)
            userUsage.update(executionContext)
        }
        return super.instrumentExecutionContext(executionContext, parameters)
    }

}

class FieldUsage {
    private val _fields: ConcurrentHashMap<String, LongAdder> = ConcurrentHashMap()
    val fields: Map<String, LongAdder>
        get() = _fields.toSortedMap()

    fun update(selectionSet: SelectionSet?, parent: String? = null) {
        selectionSet?.selections?.map {
            if (it is Field) {
                val fullName = if (parent == null) it.name else "$parent.${it.name}"
                if(!fullName.startsWith("__schema")) {
                    _fields.computeIfAbsent(fullName) { LongAdder() }.increment()
                    update(it.selectionSet, fullName)
                }
            }
        }
    }
}


class UserUsage {
    val users: ConcurrentHashMap<String, LongAdder> = ConcurrentHashMap()

    fun update(executionContext: ExecutionContext?) {
        val context = executionContext?.getContext<MyGraphQLContext>()
        if (context is MyGraphQLContext) {
            users.computeIfAbsent(context.user ?: "anonymous") { LongAdder() }.increment()
        }
    }
}
