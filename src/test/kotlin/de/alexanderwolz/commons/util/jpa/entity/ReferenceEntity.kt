package de.alexanderwolz.commons.util.jpa.entity

import jakarta.persistence.*

@Entity
@Table(name = "reference")
class ReferenceEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: String? = null,

    val name: String? = null
)
