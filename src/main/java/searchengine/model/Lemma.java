package searchengine.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.persistence.Index;  // For Javax Persistence

@Entity
@Table(name = "lemma", indexes = @Index(name = "idx_lemma", columnList = "lemma"))
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Lemma {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Integer id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "site_id", nullable = false)
    private Site site;

    @Column(name = "lemma", nullable = false, length = 500)
    private String lemma;

    @Column(name = "frequency", nullable = false)
    private Integer frequency;
}
