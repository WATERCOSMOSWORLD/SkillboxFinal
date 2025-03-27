package searchengine.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import searchengine.model.Lemma;
import org.springframework.data.jpa.repository.JpaRepository;
import searchengine.model.Site;

import java.util.List;
import java.util.Optional;


public interface LemmaRepository extends JpaRepository<Lemma, Long> {

    Optional<Lemma> findByLemmaAndSite(String lemma, Site site);

    @Modifying
    @Transactional
    @Query("DELETE FROM Lemma l WHERE l.site.id = :siteId")
    int deleteBySiteId(Long siteId);

    int countByLemma(String lemma);

    @Query("SELECT l FROM Lemma l WHERE l.lemma = :lemma")
    List<Lemma> findByLemma(@Param("lemma") String lemma);


    int countBySiteId(Long siteId);

}
