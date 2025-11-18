package de.alexanderwolz.commons.util.database

import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "sample")
data class SampleEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,

    @Column(nullable = false)
    val name: String = "",

    @Column
    val createdAt: LocalDateTime? = null
)
