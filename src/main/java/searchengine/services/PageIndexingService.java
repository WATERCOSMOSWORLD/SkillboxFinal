package searchengine.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import searchengine.config.SitesList;
import searchengine.config.ConfigSite;
import searchengine.model.Site;
import searchengine.model.IndexingStatus;
import searchengine.repository.IndexRepository;
import java.util.concurrent.ForkJoinPool;
import searchengine.repository.LemmaRepository;
import searchengine.repository.PageRepository;
import searchengine.repository.SiteRepository;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@Service
public class PageIndexingService {
    private static final Logger logger = LoggerFactory.getLogger(PageIndexingService.class);

    private final PageRepository pageRepository;
    private final SiteRepository siteRepository;
    private final SitesList sitesList;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;

    private final Set<String> visitedPages = new ConcurrentSkipListSet<>();
    private final ForkJoinPool forkJoinPool = new ForkJoinPool();

    public PageIndexingService(PageRepository pageRepository,LemmaRepository lemmaRepository,IndexRepository indexRepository, SiteRepository siteRepository, SitesList sitesList) {
        this.pageRepository = pageRepository;
        this.siteRepository = siteRepository;
        this.sitesList = sitesList;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;


    }

    public boolean indexPage(String baseUrl) {
        long startTime = System.currentTimeMillis();
        Site site = null;

        try {
            // Получаем сайт из конфигурации
            ConfigSite configSite = getConfigSiteByUrl(baseUrl);
            if (configSite == null) {
                logger.warn("⚠️ Сайт {} не найден в конфигурации!", baseUrl);
                return false;
            }

            // Удаление старых данных о сайте перед индексацией
            deleteSiteData(baseUrl);

            // Создаем новую запись о сайте
            site = new Site();
            site.setUrl(baseUrl);
            site.setName(configSite.getName());
            site.setStatus(IndexingStatus.INDEXING);
            site.setStatusTime(LocalDateTime.now());
            site = siteRepository.saveAndFlush(site);

            logger.info("🔄 Начинаем индексацию сайта: {}", baseUrl);
            ForkJoinPool forkJoinPool = new ForkJoinPool();
            forkJoinPool.invoke(new IndexPageSite(
                    site,
                    baseUrl,
                    new HashSet<>(),   // Посещенные страницы (передаем пустой Set)
                    pageRepository,
                    siteRepository,
                    lemmaRepository,
                    indexRepository,
                    logger
            ));


            // 🔹 После успешного завершения меняем статус на INDEXED
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
            Long siteId = (long) site.getId();  // Приведение к Long для LemmaRepository

            // 1. Удаляем все записи из таблицы index (по siteId через page)
            int indexesDeleted = indexRepository.deleteBySiteId(site.getId());

            // 2. Удаляем все записи из таблицы lemma (по siteId)
            int lemmasDeleted = lemmaRepository.deleteBySiteId(siteId);

            // 3. Удаляем все страницы, связанные с сайтом
            int pagesDeleted = pageRepository.deleteAllBySiteId(site.getId());

            // 4. Удаляем сам сайт
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

    // 🔹 Класс для обхода страниц сайта



}
