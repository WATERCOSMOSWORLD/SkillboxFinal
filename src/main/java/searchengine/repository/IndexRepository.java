package searchengine.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import searchengine.model.Index;
import searchengine.model.Page;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;


public interface IndexRepository extends JpaRepository<Index, Integer> {

    @Modifying
    @Transactional
    @Query("DELETE FROM Index i WHERE i.page.site.id = :siteId")
    int deleteBySiteId(@Param("siteId") int siteId);

    @Query("SELECT SUM(i.rank) FROM Index i WHERE i.page = :page AND i.lemma.lemma IN :lemmas")
    double calculateRelevance(@Param("page") Page page, @Param("lemmas") List<String> lemmas);

}


