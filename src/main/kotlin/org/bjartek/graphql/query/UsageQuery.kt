package org.bjartek.graphql.query

import com.expediagroup.graphql.annotations.GraphQLDescription
import com.expediagroup.graphql.spring.operations.Query
import org.bjartek.graphql.UsageInstrumentation
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class UsageQuery(private val goboInstrumentation: UsageInstrumentation) : Query {

    private val startTime = Instant.now()

    @GraphQLDescription("query for usage of this graphql api")
    fun usage(): Usage {
        val fields = goboInstrumentation.fieldUsage.fields.map { UsageField(it.key, it.value.sum()) }
        val users = goboInstrumentation.userUsage.users.map { UserUsage(it.key, it.value.sum()) }
        return Usage(startTime.toString(), fields, users)
    }
}


data class UsageField(val name: String, val count: Long)

data class UserUsage(val name: String, val count: Long)

data class Usage(val startTime: String, val usedFields: List<UsageField>, val users: List<UserUsage>) {
    fun usedFields(nameContains: String?) =
            if (nameContains == null) {
                usedFields
            } else {
                usedFields.filter { it.name.contains(nameContains) }
            }
}

