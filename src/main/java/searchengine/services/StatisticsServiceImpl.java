package searchengine.services;

import lombok.RequiredArgsConstructor;
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

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {

    private final SitesList sites;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final SiteRepository siteRepository;
    @Lazy
    private final IndexingService indexingService;

    private static final String SITE_NOT_FOUND_ERROR = "Сайт отсутствует в базе данных";

    @Override
    public StatisticsResponse getStatistics() {
        // Создаём итоговые данные
        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setPages(0);
        total.setLemmas(0);
        total.setIndexing(indexingService.isIndexingInProgress());

        // Список подробной статистики
        List<DetailedStatisticsItem> detailed = new ArrayList<>();

        // Итерация по конфигурации сайтов
        for (ConfigSite siteConfig : sites.getSites()) {
            DetailedStatisticsItem item = createDetailedStatisticsItem(siteConfig);
            detailed.add(item);

            // Обновление общей статистики
            total.setPages(total.getPages() + item.getPages());
            total.setLemmas(total.getLemmas() + item.getLemmas());
        }

        // Формируем и возвращаем статистику
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

        // Получаем сайт из базы по URL
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
        } else {
            // Если сайт не найден в базе
            item.setStatus("FAILED");
            item.setError(SITE_NOT_FOUND_ERROR);
            item.setStatusTime(System.currentTimeMillis());
        }

        return item;
    }

    // Метод для обновления статуса сайта
    private void updateSiteStatus(Site site) {
        if (site.getStatus() == IndexingStatus.INDEXING && !indexingService.isSiteIndexing(site.getUrl())) {
            site.setStatus(IndexingStatus.INDEXED);
            siteRepository.save(site);
        }
    }

    // Метод для получения времени статуса сайта
    private long getStatusTime(Site site) {
        return site.getStatusTime() != null
                ? site.getStatusTime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000
                : System.currentTimeMillis();
    }
}
