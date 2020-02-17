package org.bjartek.graphql.service

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.bjartek.graphql.MissingLabelException
import org.bjartek.graphql.ServiceTypes
import org.bjartek.graphql.SourceSystemException
import org.bjartek.graphql.TargetService
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.io.File
import java.io.IOException
import java.time.Instant

const val HEADER_AURORA_TOKEN = "aurora-token"

private val logger = KotlinLogging.logger {}

/**
 * Component for reading the shared secret used for authentication. You may specify the shared secret directly using
 * the aurora.token.value property, or specify a file containing the secret with the aurora.token.location property.
 */
@Component
class SharedSecretReader(
        @Value("\${aurora.token.location:}") private val secretLocation: String?,
        @Value("\${aurora.token.value:}") private val secretValue: String?
) {

    val secret = initSecret(secretValue)

    private fun initSecret(secretValue: String?) =
            if (secretLocation.isNullOrEmpty() && secretValue.isNullOrEmpty()) {
                throw IllegalArgumentException("Either aurora.token.location or aurora.token.value must be specified")
            } else {
                if (secretValue.isNullOrEmpty()) {
                    val secretFile = File(secretLocation).absoluteFile
                    try {
                        logger.info("Reading token from file {}", secretFile.absolutePath)
                        secretFile.readText()
                    } catch (e: IOException) {
                        throw IllegalStateException("Unable to read shared secret from specified location [${secretFile.absolutePath}]")
                    }
                } else {
                    secretValue
                }
            }
}

@Service
class DatabaseSchemaServiceReactive(
        private val sharedSecretReader: SharedSecretReader,
        @TargetService(ServiceTypes.DBH) private val webClient: WebClient,
        val objectMapper: ObjectMapper
) {
    companion object {
        const val HEADER_COOLDOWN_DURATION_HOURS = "cooldown-duration-hours"
    }

    fun getDatabaseSchemas(affiliation: String): Mono<List<DatabaseSchemaResource>> {
        val response: Mono<DbhResponse<*>> = webClient
                .get()
                .uri {
                    it.path("/api/v1/schema/").queryParam("labels", "affiliation=$affiliation").build()
                }
                .header(HttpHeaders.AUTHORIZATION, "$HEADER_AURORA_TOKEN ${sharedSecretReader.secret}")
                .retrieve()
                .bodyToMono()

        return response.items()
    }

    fun getDatabaseSchema(id: String) = webClient
            .get()
            .uri("/api/v1/schema/$id")
            .header(HttpHeaders.AUTHORIZATION, "$HEADER_AURORA_TOKEN ${sharedSecretReader.secret}")
            .retrieve()
            .bodyToMono<DbhResponse<*>>()
            .item()

    fun updateDatabaseSchema(input: SchemaUpdateRequest) = webClient
            .put()
            .uri("/api/v1/schema/${input.id}")
            .body(BodyInserters.fromValue(input))
            .header(HttpHeaders.AUTHORIZATION, "aurora-token ${sharedSecretReader.secret}")
            .retrieve()
            .bodyToMono<DbhResponse<*>>()
            .item()

    fun deleteDatabaseSchemas(input: List<SchemaDeletionRequest>): Flux<SchemaDeletionResponse> {
        val responses = input.map { request ->
            val requestSpec = webClient
                    .delete()
                    .uri("/api/v1/schema/${request.id}")
                    .header(HttpHeaders.AUTHORIZATION, "$HEADER_AURORA_TOKEN ${sharedSecretReader.secret}")

            request.cooldownDurationHours?.let {
                requestSpec.header(HEADER_COOLDOWN_DURATION_HOURS, it.toString())
            }

            requestSpec.retrieve().bodyToMono<DbhResponse<*>>().map {
                request.id to it
            }
        }

        return Flux.merge(responses).map { SchemaDeletionResponse(id = it.first, success = it.second.isOk()) }
    }

    fun testJdbcConnection(id: String? = null, user: JdbcUser? = null): Mono<Boolean> {
        val response: Mono<DbhResponse<Boolean>> = webClient
                .put()
                .uri("/api/v1/schema/validate")
                .body(
                        BodyInserters.fromValue(
                                mapOf(
                                        "id" to id,
                                        "jdbcUser" to user
                                )
                        )
                )
                .header(HttpHeaders.AUTHORIZATION, "$HEADER_AURORA_TOKEN ${sharedSecretReader.secret}")
                .retrieve()
                .bodyToMono()
        return response.flatMap {
            it.items.first().toMono()
        }
    }

    fun createDatabaseSchema(input: SchemaCreationRequest): Mono<DatabaseSchemaResource> {
        val missingLabels = input.findMissingOrEmptyLabels()
        if (missingLabels.isNotEmpty()) {
            return Mono.error(MissingLabelException("Missing labels in mutation input: $missingLabels"))
        }

        return webClient
                .post()
                .uri("/api/v1/schema/")
                .body(BodyInserters.fromValue(input))
                .header(HttpHeaders.AUTHORIZATION, "$HEADER_AURORA_TOKEN ${sharedSecretReader.secret}")
                .retrieve()
                .bodyToMono<DbhResponse<*>>()
                .item()
    }

    private fun Mono<DbhResponse<*>>.item() = this.items().map { it.first() }

    private fun Mono<DbhResponse<*>>.items() =
            this.flatMap {
                when {
                    it.isFailure() -> onFailure(it)
                    it.isEmpty() -> Mono.empty()
                    else -> onSuccess(it)
                }
            }

    private fun onFailure(r: DbhResponse<*>): Mono<List<DatabaseSchemaResource>> {
        return Mono.error(SourceSystemException(
                message = "status=${r.status} error=${(r.items.firstOrNull() ?: "") as String}",
                sourceSystem = "dbh"
        ))
    }

    private fun onSuccess(r: DbhResponse<*>) =
            r.items.map {
                objectMapper.convertValue(it, DatabaseSchemaResource::class.java)
            }.filter {
                it.containsRequiredLabels()
            }.toMono()
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class DatabaseInstanceResource(val engine: String)

@JsonIgnoreProperties(ignoreUnknown = true)
data class DatabaseUserResource(val username: String, val password: String, val type: String)

@JsonIgnoreProperties(ignoreUnknown = true)
data class DatabaseMetadataResource(val sizeInMb: Double)

/**
 * labels:
 * createdBy == userId
 * discriminator == name
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class DatabaseSchemaResource(
        val id: String,
        val type: String,
        val jdbcUrl: String,
        val name: String,
        val createdDate: Long,
        val lastUsedDate: Long?,
        val databaseInstance: DatabaseInstanceResource,
        val users: List<DatabaseUserResource>,
        val metadata: DatabaseMetadataResource,
        val labels: Map<String, String>
) {
    val environment: String by labels
    val application: String by labels
    val affiliation: String by labels
    val description: String? by labels.withDefault { null }

    private val userId: String by labels
    val createdBy: String
        get() = userId

    // Using filter because there is a collision on "name", property and label is called "name"
    val discriminator: String
        get() = labels.filter { it.key == "name" }.values.first()

    fun createdDateAsInstant(): Instant = Instant.ofEpochMilli(createdDate)

    fun lastUsedDateAsInstant(): Instant? =
            lastUsedDate?.let {
                return Instant.ofEpochMilli(it)
            }

    fun containsRequiredLabels() =
            labels.containsKey("affiliation") &&
                    labels.containsKey("userId") &&
                    labels.containsKey("name") &&
                    labels.containsKey("environment") &&
                    labels.containsKey("application")
}

data class SchemaCreationRequest(
        val labels: Map<String, String>,
        @JsonProperty("schema")
        val jdbcUser: JdbcUser? = null
) {
    private val requiredLabels = listOf(
            "affiliation", "name", "environment", "application"
    )

    fun findMissingOrEmptyLabels(): List<String> = requiredLabels.filter { labels[it]?.isEmpty() ?: true }
}

data class SchemaUpdateRequest(
        val id: String,
        val labels: Map<String, String>,
        @JsonProperty("schema")
        val jdbcUser: JdbcUser? = null
)

data class SchemaDeletionRequest(
        val id: String,
        val cooldownDurationHours: Long? = null
)

data class JdbcUser(
        val username: String,
        val password: String,
        val jdbcUrl: String
)

data class DbhResponse<T>(val status: String, val items: List<T>, val totalCount: Int = items.size) {
    companion object {
        fun <T> ok(vararg items: T) = DbhResponse("OK", items.toList())
        fun <T> ok(item: T) = DbhResponse("OK", listOf(item))
        fun <T> ok() = DbhResponse("OK", emptyList<T>())
        fun failed(item: String) = DbhResponse("Failed", listOf(item))
        fun failed() = DbhResponse("Failed", emptyList<String>())
    }

    fun isOk() = status.toLowerCase() == "ok"
    fun isFailure() = status.toLowerCase() == "failed"
    fun isEmpty() = totalCount == 0
}

data class SchemaDeletionResponse(val id: String, val success: Boolean)
