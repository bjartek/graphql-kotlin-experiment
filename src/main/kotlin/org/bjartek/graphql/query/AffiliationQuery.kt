package org.bjartek.graphql.query

import com.expediagroup.graphql.annotations.GraphQLDescription
import com.expediagroup.graphql.spring.operations.Query
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.reactive.awaitSingle
import org.bjartek.graphql.*
import org.bjartek.graphql.service.DatabaseSchemaResource
import org.bjartek.graphql.service.DatabaseSchemaServiceReactive
import org.bjartek.graphql.service.DatabaseUserResource
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Mono
import java.lang.IllegalArgumentException


@Service
class AffiliationService(@TargetService(ServiceTypes.MOKEY) val webClient: WebClient) {

    fun getAllVisibleAffiliations(token: String): Mono<List<String>> =
            webClient
                    .get()
                    .uri("/api/auth/affiliation")
                    .header(HttpHeaders.AUTHORIZATION, "Bearer $token")
                    .retrieve()
                    .bodyToMono()

    fun getAllAffiliations(): Mono<List<String>> =
            webClient
                    .get()
                    .uri("/api/affiliation")
                    .retrieve()
                    .bodyToMono()
}

@Service
class AffiliationQuery(
        val affiliationService: AffiliationService
) : Query {


    @GraphQLDescription("Get all available affiliations, optionally filter on the given list")
    suspend fun publicAffiliations(
            affiliation: List<String>?,
            dfe: DataFetchingEnvironment
    ): List<PublicAffiliation> {
        return affiliationService.getAllAffiliations().awaitSingle().filter {
            affiliation == null || affiliation.contains(it)
        }.map { PublicAffiliation(it) }
    }


    @GraphQLDescription("Get all affiliations you can see and operate on. Required Bearer Authentication tken")
    suspend fun affiliations(
            affiliation: List<String>?,
            dfe: DataFetchingEnvironment
    ): List<Affiliation> {
        return affiliationService.getAllVisibleAffiliations(dfe.user()).awaitLast()
                .filter {
                    affiliation == null || affiliation.contains(it)
                }.map {
                    Affiliation(it)
                }
    }

}


fun DataFetchingEnvironment.user() = this.getContext<MyGraphQLContext>().user
        ?: throw IllegalArgumentException("User is not set")


data class PublicAffiliation(val name: String)

data class Affiliation(val name: String) {

    @GraphQLDescription("Get all database schemas for the given affiliation")
    suspend fun databaseSchemas(dfe: DataFetchingEnvironment): List<DatabaseSchema> {
        return dfe.loadMany<String, DatabaseSchemaResource>(name).map {
            DatabaseSchema.create(it, Affiliation(name))
        }
    }
}

data class DatabaseSchema(
        val id: String,
        val type: String,
        val jdbcUrl: String,
        val name: String,
        val environment: String,
        val application: String,
        val discriminator: String,
        val description: String?,
        val affiliation: Affiliation,
        val databaseEngine: String,
        val createdBy: String?,
        val createdDate: String,
        val lastUsedDate: String,
        val sizeInMb: Double,
        val users: List<DatabaseUser>
) {
    companion object {
        fun create(databaseSchema: DatabaseSchemaResource, affiliation: Affiliation) =
                DatabaseSchema(
                        id = databaseSchema.id,
                        type = databaseSchema.type,
                        jdbcUrl = databaseSchema.jdbcUrl,
                        name = databaseSchema.name,
                        environment = databaseSchema.environment,
                        application = databaseSchema.application,
                        discriminator = databaseSchema.discriminator,
                        description = databaseSchema.description,
                        affiliation = affiliation,
                        databaseEngine = databaseSchema.databaseInstance.engine,
                        createdBy = databaseSchema.createdBy,
                        createdDate = databaseSchema.createdDateAsInstant().toString(),
                        lastUsedDate = databaseSchema.lastUsedDateAsInstant().toString(),
                        sizeInMb = databaseSchema.metadata.sizeInMb,
                        users = databaseSchema.users.map { DatabaseUser.create(it) }
                )
    }
}

data class DatabaseUser(val username: String, val password: String, val type: String) {
    companion object {
        fun create(userResource: DatabaseUserResource) =
                DatabaseUser(username = userResource.username, password = userResource.password, type = userResource.type)
    }
}

@Component
class DatabaseSchemaResourceListDataLoader(
        val databaseSchemaServiceReactive: DatabaseSchemaServiceReactive
) : KeyDataLoader<String, List<DatabaseSchemaResource>> {
    override suspend fun getByKey(key: String, ctx: MyGraphQLContext): List<DatabaseSchemaResource> =
            databaseSchemaServiceReactive.getDatabaseSchemas(key).awaitSingle()
}




