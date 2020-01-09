package org.bjartek.graphql.query

import com.expediagroup.graphql.annotations.GraphQLDescription
import com.expediagroup.graphql.annotations.GraphQLIgnore
import com.expediagroup.graphql.spring.operations.Query
import graphql.schema.DataFetchingEnvironment
import org.bjartek.graphql.KeyDataLoader
import org.bjartek.graphql.MultipleKeysDataLoader
import org.bjartek.graphql.loadOptional
import org.dataloader.Try
import org.springframework.stereotype.Component

@Component
class EmployeeQuery : Query {
    private val employees = listOf(
            Employee(name = "Mike", companyId = 1),
            Employee(name = "John", companyId = 1),
            Employee(name = "Steve", companyId = 2)
    )

    @GraphQLDescription("Get all employees")
    fun employees(): List<Employee> {
        return employees
    }
}


data class Employee(
    val name: String,
    @GraphQLIgnore
    val companyId: Int
) {

    @GraphQLDescription("The company for the employee")
    suspend fun company(dfe: DataFetchingEnvironment)= dfe.loadOptional<Int, Company>(companyId)

}

data class Company(val id: Int, val name: String)

//resolve all companies in one remote call
@Component
class CompanyDataLoader : MultipleKeysDataLoader<Int, Company> {
    override suspend fun getByKeys(keys: Set<Int>): Map<Int, Try<Company>> {
        return keys.associateWith {
            if(it != 1) {
                Try.failed<Company>(RuntimeException("Failed"))
            } else {
                Try.succeeded(Company(id = it, name = "Teh Company"))
            }
        }
    }
}

//this is not in use right now, only an example on how to resolve individual companies against something
@Component
class CompanyDataLoader2 : KeyDataLoader<Int, Company> {
    override suspend fun getByKey(key: Int): Company {
        if (key != 1) {
            throw RuntimeException("Failed")
        }
        return Company(id = 1, name = "FirstCompany")
    }
}
