package searchengine.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;
import searchengine.model.Lemma;
import searchengine.model.Site;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import org.springframework.data.repository.query.Param;
import java.util.List;


public interface LemmaRepository extends JpaRepository<Lemma, Long> {

    @Query("SELECT l FROM Lemma l WHERE l.lemma = :lemma")
    List<Lemma> findByLemma(@Param("lemma") String lemma);


    @Modifying
    @Transactional
    @Query("DELETE FROM Lemma l WHERE l.site.id = :siteId")
    int deleteBySiteId(Long siteId);


    List<Lemma> findByLemmaAndSite(String lemma, Site site);


    int countByLemma(String lemma);


    int countBySiteId(Long siteId);

}
