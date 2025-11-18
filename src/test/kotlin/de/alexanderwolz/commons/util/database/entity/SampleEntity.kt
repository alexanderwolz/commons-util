package de.alexanderwolz.commons.util.database.entity

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.util.JSONPObject
import jakarta.persistence.*
import java.time.LocalDateTime
import java.util.*

@Entity
@Table(
    name = "sample",
    indexes = [
        Index(name = "idx_sample_code", columnList = "code")
    ]
)
class SampleEntity(

    // PK – UUID
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    val id: UUID? = null,

    @Column(nullable = false)
    val code: String = "",

    @Column(nullable = false)
    val email: String = "",

    @Column(nullable = false)
    val created_at: LocalDateTime = LocalDateTime.now(),

    @Column(nullable = false)
    val updated_at: LocalDateTime = LocalDateTime.now(),

    @Enumerated(EnumType.STRING)
    val sampleStatus: SampleStatus = SampleStatus.ACTIVE,

    @ManyToOne
    @JoinColumn(name = "reference_id", nullable = false)
    val requiredReference: ReferenceEntity,

    @ManyToOne
    @JoinColumn(name = "optional_reference_id", nullable = true)
    val optionalReference: ReferenceEntity?,

    @Embedded
    val address: Address = Address("street", "city"),

    @Column(columnDefinition = "JSONB")
    val json: JsonNode? = null,

    @Column(nullable = false)
    val settings: Settings = Settings()


)
