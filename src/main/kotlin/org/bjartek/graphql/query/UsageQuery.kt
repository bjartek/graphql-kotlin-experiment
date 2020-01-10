package org.bjartek.graphql.query

import com.expediagroup.graphql.annotations.GraphQLDescription
import com.expediagroup.graphql.spring.operations.Query
import graphql.schema.DataFetchingEnvironment
import org.bjartek.graphql.MyGraphQLContext
import org.bjartek.graphql.UsageInstrumentation
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Instant

fun DataFetchingEnvironment.isUser(expectedUser:String) : Boolean {
    val user = this.getContext<MyGraphQLContext>().user
    return user == expectedUser
}

@Component
class UsageQuery(
        private val goboInstrumentation: UsageInstrumentation,
        @Value("\${org.bjartek.usage.user:graphql}") val expectedUser: String
) : Query {

    private val startTime = Instant.now()

    @GraphQLDescription("query for usage of this graphql api")
    fun usage(dataFetchingEnvironment: DataFetchingEnvironment): Usage {
        if(!dataFetchingEnvironment.isUser(expectedUser)) {
            throw RuntimeException("Not authorized")
        }
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

