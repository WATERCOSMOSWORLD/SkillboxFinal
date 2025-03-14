package searchengine.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import searchengine.model.Site;
import searchengine.model.IndexingStatus;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import org.springframework.data.jpa.repository.Modifying;
import java.util.Optional;

@Repository
public interface SiteRepository extends JpaRepository<Site, Integer> {

    Site findByUrl(String url);


    List<Site> findAllByStatus(IndexingStatus status);
    List<Site> findAllByUrl(String url);


    @Modifying
    @Transactional
    void delete(Site site);

    long countByStatus(IndexingStatus status);
    Optional<Site> findFirstByUrl(String url);
}