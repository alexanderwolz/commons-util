package de.alexanderwolz.commons.util.database

import jakarta.persistence.Basic
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.FetchType
import jakarta.persistence.Id
import jakarta.persistence.Lob
import jakarta.persistence.Table

@Entity
@Table(name = "sample")
class SampleEntity(

    @Id
    @Column(length = 255)
    var id: String,

    @Column(length = 100, unique = true)
    var username: String? = null,

    @Column(name = "given_name", length = 100)
    var givenName: String? = null,

    @Column(name = "family_name", length = 100)
    var familyName: String? = null,

    @Column(length = 255)
    var email: String? = null,

    @Lob
    @Column(name = "snapshot", columnDefinition = "jsonb")
    var snapshot: String? = null,

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "pdf_bytes", columnDefinition = "bytea")
    var pdfBytes: ByteArray? = null

    )
