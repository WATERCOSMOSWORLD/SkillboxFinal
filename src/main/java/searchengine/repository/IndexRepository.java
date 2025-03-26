package searchengine.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import searchengine.model.Index;
import searchengine.model.Lemma;
import searchengine.model.Page;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IndexRepository extends JpaRepository<Index, Integer> {

    @Modifying
    @Transactional
    @Query("DELETE FROM Index i WHERE i.page.site.id = :siteId")
    int deleteBySiteId(@Param("siteId") int siteId);

    Optional<Index> findByLemmaAndPage(Lemma lemma, Page page);
}
