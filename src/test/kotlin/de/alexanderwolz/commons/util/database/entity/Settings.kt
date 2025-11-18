package de.alexanderwolz.commons.util.database.entity

import com.fasterxml.jackson.databind.JsonNode
import jakarta.persistence.Column
import jakarta.persistence.Embeddable

@Embeddable
data class Settings(
    @Column
    val data: JsonNode? = null
)
