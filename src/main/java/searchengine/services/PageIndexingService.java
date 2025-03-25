package searchengine.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import searchengine.config.SitesList;
import searchengine.config.ConfigSite;
import searchengine.model.Site;
import searchengine.model.IndexingStatus;
import searchengine.repository.IndexRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.PageRepository;
import java.util.HashSet;
import org.springframework.context.annotation.Lazy;

import searchengine.repository.SiteRepository;
import java.time.LocalDateTime;
import java.util.concurrent.ForkJoinPool;

@Service
@Lazy
public class PageIndexingService {
    private static final Logger logger = LoggerFactory.getLogger(PageIndexingService.class);
    private final IndexingService indexingService;
    private final PageRepository pageRepository;
    private final SiteRepository siteRepository;
    private final SitesList sitesList;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;

    public PageIndexingService(PageRepository pageRepository,@Lazy IndexingService indexingService, LemmaRepository lemmaRepository,IndexRepository indexRepository, SiteRepository siteRepository, SitesList sitesList) {
        this.pageRepository = pageRepository;
        this.siteRepository = siteRepository;
        this.indexingService = indexingService;
        this.sitesList = sitesList;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;
    }

    public boolean indexPage(String baseUrl) {
        long startTime = System.currentTimeMillis();
        Site site = null;

        try {
            ConfigSite configSite = getConfigSiteByUrl(baseUrl);
            if (configSite == null) {
                logger.warn("⚠️ Сайт {} не найден в конфигурации!", baseUrl);
                return false;
            }

            deleteSiteData(baseUrl);

            site = new Site();
            site.setUrl(baseUrl);
            site.setName(configSite.getName());
            site.setStatus(IndexingStatus.INDEXING);
            site.setStatusTime(LocalDateTime.now());
            site = siteRepository.saveAndFlush(site);

            logger.info("🔄 Начинаем индексацию сайта: {}", baseUrl);

            // Создание ForkJoinPool и запуск PageCrawler с нужными параметрами
            ForkJoinPool forkJoinPool = new ForkJoinPool();
            forkJoinPool.invoke(new PageCrawler(
                    site,                          // объект Site
                    lemmaRepository,               // объект LemmaRepository
                    siteRepository,                // объект SiteRepository
                    indexRepository,               // объект IndexRepository
                    baseUrl,                       // URL для индексации
                    new HashSet<>(),               // множество посещенных URL
                    pageRepository,                // объект PageRepository
                    indexingService                // объект IndexingService
            ));

            site.setStatus(IndexingStatus.INDEXED);
            site.setStatusTime(LocalDateTime.now());
            site.setLastError(null);
            siteRepository.save(site);

            long endTime = System.currentTimeMillis();
            logger.info("✅ Индексация завершена за {} сек. Сайт помечен как INDEXED.", (endTime - startTime) / 1000);
            return true;
        } catch (Exception e) {
            logger.error("❌ Ошибка при индексации сайта {}: {}", baseUrl, e.getMessage(), e);

            if (site != null) {
                site.setStatus(IndexingStatus.FAILED);
                site.setStatusTime(LocalDateTime.now());
                site.setLastError("Ошибка индексации: " + e.getMessage());
                siteRepository.save(site);
            }

            return false;
        }
    }

    @Transactional
    private void deleteSiteData(String siteUrl) {
        searchengine.model.Site site = siteRepository.findByUrl(siteUrl);
        if (site != null) {
            Long siteId = (long) site.getId();

            int indexesDeleted = indexRepository.deleteBySiteId(site.getId());

            int lemmasDeleted = lemmaRepository.deleteBySiteId(siteId);

            int pagesDeleted = pageRepository.deleteAllBySiteId(site.getId());

            siteRepository.delete(site);

            logger.info("Удалено {} записей из таблицы index.", indexesDeleted);
            logger.info("Удалено {} записей из таблицы lemma.", lemmasDeleted);
            logger.info("Удалено {} записей из таблицы page для сайта {}.", pagesDeleted, siteUrl);
            logger.info("Сайт {} успешно удален.", siteUrl);
        } else {
            logger.warn("Сайт {} не найден в базе данных.", siteUrl);
        }
    }

    private ConfigSite getConfigSiteByUrl(String url) {
        return sitesList.getSites().stream()
                .filter(site -> site.getUrl().equalsIgnoreCase(url))
                .findFirst()
                .orElse(null);
    }
}
