package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import searchengine.config.SitesList;
import searchengine.dto.statistics.DetailedStatisticsItem;
import searchengine.dto.statistics.StatisticsData;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.dto.statistics.TotalStatistics;
import searchengine.config.ConfigSite;
import searchengine.model.Site;
import java.time.LocalDateTime;

import searchengine.model.IndexingStatus;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.SiteRepository;
import org.springframework.context.annotation.Lazy;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsServiceImpl.class);
    private final SitesList sites;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final SiteRepository siteRepository;
    @Lazy
    private final IndexingService indexingService;

    private static final String SITE_NOT_FOUND_ERROR = "Сайт отсутствует в базе данных";

    @Override
    public StatisticsResponse getStatistics() {
        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setPages(0);
        total.setLemmas(0);
        total.setIndexing(indexingService.isIndexingInProgress());

        List<DetailedStatisticsItem> detailed = new ArrayList<>();

        for (ConfigSite siteConfig : sites.getSites()) {
            DetailedStatisticsItem item = createDetailedStatisticsItem(siteConfig);
            detailed.add(item);

            total.setPages(total.getPages() + item.getPages());
            total.setLemmas(total.getLemmas() + item.getLemmas());
        }

        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);

        StatisticsResponse response = new StatisticsResponse();
        response.setStatistics(data);
        response.setResult(true);

        return response;
    }

    private DetailedStatisticsItem createDetailedStatisticsItem(ConfigSite siteConfig) {
        DetailedStatisticsItem item = new DetailedStatisticsItem();
        item.setName(siteConfig.getName());
        item.setUrl(siteConfig.getUrl());

        // Находим сайт в базе данных
        Site site = siteRepository.findFirstByUrl(siteConfig.getUrl()).orElse(null);

        if (site != null) {
            updateSiteStatus(site);  // Обновляем статус сайта, если необходимо
            item.setStatus(site.getStatus().toString());
            item.setStatusTime(getStatusTime(site));

            if (site.getStatus() == IndexingStatus.FAILED) {
                item.setError(site.getLastError());
            }

            // Получаем количество страниц и лемм
            item.setPages(pageRepository.countBySiteUrl(siteConfig.getUrl()));
            item.setLemmas(lemmaRepository.countBySiteId((long) site.getId()));

            logger.info("Сайт найден: {}", site.getUrl());
        } else {
            // Если сайт не найден в базе
            item.setStatus("FAILED");
            item.setError(SITE_NOT_FOUND_ERROR);
            item.setStatusTime(System.currentTimeMillis());

            logger.error("Сайт не найден в базе данных: {}", siteConfig.getUrl());
        }

        return item;
    }

    private void updateSiteStatus(Site site) {
        logger.info("Проверяем статус для сайта: {}", site.getUrl());

        // Если сайт в статусе FAILED, не обновляем его на INDEXED
        if (site.getStatus() == IndexingStatus.FAILED) {
            logger.warn("Сайт {} имеет ошибку, не обновляем статус на INDEXED", site.getUrl());
            return;
        }

        // Если сайт индексируется, но индексация завершена, меняем статус
        if (site.getStatus() == IndexingStatus.INDEXING && !indexingService.isSiteIndexing(site.getUrl())) {
            logger.info("Индексация завершена для сайта: {}", site.getUrl());
            site.setStatus(IndexingStatus.INDEXED);
            site.setStatusTime(LocalDateTime.now());  // Обновляем время последнего обновления
            siteRepository.save(site);
        } else if (site.getStatus() != IndexingStatus.INDEXING && indexingService.isSiteIndexing(site.getUrl())) {
            logger.info("Индексация началась для сайта: {}", site.getUrl());
            site.setStatus(IndexingStatus.INDEXING);
            site.setStatusTime(LocalDateTime.now());  // Обновляем время начала индексации
            siteRepository.save(site);
        }
    }



    private long getStatusTime(Site site) {
        return site.getStatusTime() != null
                ? site.getStatusTime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000
                : System.currentTimeMillis();
    }
}
