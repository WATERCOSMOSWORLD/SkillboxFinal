package searchengine.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import searchengine.model.Site;
import searchengine.model.IndexingStatus;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import org.springframework.data.jpa.repository.Modifying;


@Repository
public interface SiteRepository extends JpaRepository<Site, Integer> {
    // Найти сайт по URL
    Site findByUrl(String url);

    // Найти все сайты по статусу
    List<Site> findAllByStatus(IndexingStatus status);


    @Modifying
    @Transactional
    void delete(Site site);

    long countByStatus(IndexingStatus status);
}