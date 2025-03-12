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

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.client.RestTemplate;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {

    private final SitesList sites;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final SiteRepository siteRepository;
    private final RestTemplate restTemplate = new RestTemplate();
    private final IndexingService indexingService;

    @Override
    public StatisticsResponse getStatistics() {
        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setPages(0);
        total.setLemmas(0);
        total.setIndexing(indexingService.isIndexingInProgress());

        List<DetailedStatisticsItem> detailed = new ArrayList<>();

        for (ConfigSite siteConfig : sites.getSites()) {
            DetailedStatisticsItem item = new DetailedStatisticsItem();
            item.setName(siteConfig.getName());
            item.setUrl(siteConfig.getUrl());

            List<Site> sitesList = siteRepository.findAllByUrl(siteConfig.getUrl());
            if (!sitesList.isEmpty()) {
                Site site = sitesList.get(0); // Берем первую запись

                if (site.getStatus() == IndexingStatus.INDEXING && !indexingService.isSiteIndexing(site.getUrl())) {
                    site.setStatus(IndexingStatus.INDEXED);
                    siteRepository.save(site);
                }

                item.setStatus(site.getStatus().toString());

                if (site.getStatus() == IndexingStatus.FAILED) {
                    item.setError(site.getLastError());
                }

                item.setStatusTime(site.getStatusTime() != null ? site.getStatusTime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000 : System.currentTimeMillis());
            } else {
                item.setStatus("FAILED");
                item.setError("Сайт отсутствует в базе данных");
                item.setStatusTime(System.currentTimeMillis());
            }

            int pages = pageRepository.countBySiteUrl(siteConfig.getUrl());
            int lemmas = (!sitesList.isEmpty()) ? lemmaRepository.countBySiteId((long) sitesList.get(0).getId()) : 0;

            item.setPages(pages);
            item.setLemmas(lemmas);

            total.setPages(total.getPages() + pages);
            total.setLemmas(total.getLemmas() + lemmas);

            detailed.add(item);
        }

        StatisticsData data = new StatisticsData();
        data.setTotal(total);
        data.setDetailed(detailed);

        StatisticsResponse response = new StatisticsResponse();
        response.setStatistics(data);
        response.setResult(true);

        return response;
    }
}
