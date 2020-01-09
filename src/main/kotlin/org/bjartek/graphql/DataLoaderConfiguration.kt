package org.bjartek.graphql

import com.expediagroup.graphql.spring.exception.SimpleKotlinGraphQLError
import com.expediagroup.graphql.spring.execution.DataLoaderRegistryFactory
import graphql.execution.DataFetcherResult
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.*
import kotlinx.coroutines.future.asCompletableFuture
import kotlinx.coroutines.future.await
import org.dataloader.DataLoader
import org.dataloader.DataLoaderRegistry
import org.dataloader.Try
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.Executors

@Configuration
class DataLoaderConfiguration(
        val keyLoaders: List<KeyDataLoader<*, *>>,
        val multipleKeysDataLoaders: List<MultipleKeysDataLoader<*, *>>
) {
    @Bean
    fun dataLoaderRegistryFactory(): DataLoaderRegistryFactory {
        return object : DataLoaderRegistryFactory {

            override fun generate(): DataLoaderRegistry {
                return DataLoaderRegistry().apply {
                    keyLoaders.forEach {
                        register(it::class.simpleName, batchDataLoaderMappedSingle(it))
                    }
                    multipleKeysDataLoaders.forEach {
                        register(it::class.simpleName, batchDataLoaderMappedMultiple(it))
                    }
                }
            }
        }
    }
}


interface KeyDataLoader<K, V> {
    suspend fun getByKey(key: K): V
}

interface MultipleKeysDataLoader<K, V> {
    suspend fun getByKeys(keys: Set<K>): Map<K, Try<V>>
}

/*
  Use this if you have a service that loads multiple ids.
 */
fun <K, V> batchDataLoaderMappedMultiple(keysDataLoader: MultipleKeysDataLoader<K, V>): DataLoader<K, V> =
    DataLoader.newMappedDataLoaderWithTry { keys: Set<K> ->
        GlobalScope.async {
            keysDataLoader.getByKeys(keys)
        }.asCompletableFuture()
    }


/*
  Use this if you have a service that loads a single id. Will make requests in parallel
 */
fun <K, V> batchDataLoaderMappedSingle(keyDataLoader: KeyDataLoader<K, V>): DataLoader<K, V> =
    DataLoader.newMappedDataLoaderWithTry { keys: Set<K> ->

        GlobalScope.async {
            val deferred: List<Deferred<Pair<K, Try<V>>>> = keys.map { key ->
                async {
                    key to try {
                        Try.succeeded(keyDataLoader.getByKey(key))
                    } catch (e: Exception) {
                        Try.failed<V>(e)
                    }
                }
            }
            deferred.awaitAll().toMap()
        }.asCompletableFuture()
    }

suspend inline fun <Key, reified Value> DataFetchingEnvironment.load(key: Key): Value {
    val loaderName = "${Value::class.java.simpleName}DataLoader"
    return this.getDataLoader<Key, Value>(loaderName).load(key).await()
}

suspend inline fun <Key, reified Value> DataFetchingEnvironment.loadOptional(key: Key): DataFetcherResult<Value?> {

    val dfr = DataFetcherResult.newResult<Value>()
    return try {
        dfr.data(load(key))
    }catch(e:Exception) {
        dfr.error(SimpleKotlinGraphQLError(e, listOf(mergedField.singleField.sourceLocation), path = executionStepInfo.path.toList()))
    }.build()
}
