package de.alexanderwolz.commons.util.database.entity.fu

import com.fasterxml.jackson.databind.JsonNode
import de.alexanderwolz.commons.util.database.entity.Address
import de.alexanderwolz.commons.util.database.entity.ReferenceEntity
import de.alexanderwolz.commons.util.database.entity.SampleStatus
import de.alexanderwolz.commons.util.database.entity.Settings
import jakarta.persistence.Column
import jakarta.persistence.Embedded
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Index
import jakarta.persistence.JoinColumn
import jakarta.persistence.ManyToOne
import jakarta.persistence.Table
import java.time.LocalDateTime
import java.util.UUID

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

    @Column
    val json: JsonNode? = null,

    @Column(nullable = false)
    val settings: Settings = Settings()


)