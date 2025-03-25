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
import searchengine.model.IndexingStatus;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.SiteRepository;
import org.springframework.context.annotation.Lazy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;

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

        List<DetailedStatisticsItem> detailed = sites.getSites().stream()
                .map(this::createDetailedStatisticsItem)
                .peek(item -> {
                    total.setPages(total.getPages() + item.getPages());
                    total.setLemmas(total.getLemmas() + item.getLemmas());
                })
                .toList();

        // Используем сеттеры, так как нет конструктора с параметрами
        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);

        StatisticsResponse response = new StatisticsResponse();
        response.setResult(true);
        response.setStatistics(data);

        return response;
    }


    private DetailedStatisticsItem createDetailedStatisticsItem(ConfigSite siteConfig) {
        DetailedStatisticsItem item = new DetailedStatisticsItem();
        item.setName(siteConfig.getName());
        item.setUrl(siteConfig.getUrl());

        siteRepository.findFirstByUrl(siteConfig.getUrl())
                .ifPresentOrElse(
                        site -> populateSiteStatistics(item, site),
                        () -> {
                            item.setStatus("FAILED");
                            item.setError(SITE_NOT_FOUND_ERROR);
                            item.setStatusTime(System.currentTimeMillis());
                            logger.error("Сайт не найден в базе данных: {}", siteConfig.getUrl());
                        }
                );

        return item;
    }

    private void populateSiteStatistics(DetailedStatisticsItem item, Site site) {
        updateSiteStatusIfNeeded(site);

        item.setStatus(site.getStatus().toString());
        item.setStatusTime(getStatusTime(site));
        item.setPages(pageRepository.countBySiteUrl(site.getUrl()));
        item.setLemmas(lemmaRepository.countBySiteId((long) site.getId()));

        if (site.getStatus() == IndexingStatus.FAILED) {
            item.setError(site.getLastError());
        }

        logger.info("Сайт обработан: {}", site.getUrl());
    }

    private void updateSiteStatusIfNeeded(Site site) {
        boolean isCurrentlyIndexing = indexingService.isSiteIndexing(site.getUrl());

        if (site.getStatus() == IndexingStatus.FAILED) {
            logger.warn("Сайт {} имеет ошибку, статус не обновляется", site.getUrl());
            return;
        }

        if (site.getStatus() == IndexingStatus.INDEXING && !isCurrentlyIndexing) {
            logger.info("Индексация завершена для сайта: {}", site.getUrl());
            updateSiteStatus(site, IndexingStatus.INDEXED);
        } else if (site.getStatus() != IndexingStatus.INDEXING && isCurrentlyIndexing) {
            logger.info("Индексация началась для сайта: {}", site.getUrl());
            updateSiteStatus(site, IndexingStatus.INDEXING);
        }
    }

    private void updateSiteStatus(Site site, IndexingStatus newStatus) {
        site.setStatus(newStatus);
        site.setStatusTime(LocalDateTime.now());
        siteRepository.save(site);
    }

    private long getStatusTime(Site site) {
        return site.getStatusTime() != null
                ? site.getStatusTime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000
                : System.currentTimeMillis();
    }
}
