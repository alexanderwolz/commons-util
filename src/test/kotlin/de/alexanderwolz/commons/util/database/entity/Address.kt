package de.alexanderwolz.commons.util.database.entity

import jakarta.persistence.Column
import jakarta.persistence.Embeddable

@Embeddable
data class Address(

    @Column(nullable = false)
    val street: String = "",

    @Column(nullable = false)
    val city: String = ""
)
