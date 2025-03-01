package searchengine.services;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import searchengine.config.SitesList;
import searchengine.dto.statistics.DetailedStatisticsItem;
import searchengine.dto.statistics.StatisticsData;
import searchengine.dto.statistics.StatisticsResponse;
import searchengine.dto.statistics.TotalStatistics;
import searchengine.config.ConfigSite;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import java.util.ArrayList;
import java.util.List;
import org.springframework.web.client.RestTemplate;

@Service
@RequiredArgsConstructor
public class StatisticsServiceImpl implements StatisticsService {

    private final SitesList sites;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    @Override
    public StatisticsResponse getStatistics() {
        TotalStatistics total = new TotalStatistics();
        total.setSites(sites.getSites().size());
        total.setPages(0);
        total.setLemmas(0);
        total.setIndexing(true);

        List<DetailedStatisticsItem> detailed = new ArrayList<>();

        for (ConfigSite site : sites.getSites()) {
            DetailedStatisticsItem item = new DetailedStatisticsItem();
            item.setName(site.getName());
            item.setUrl(site.getUrl());

            boolean isAvailable = checkSiteAvailability(site.getUrl());
            String status = isAvailable ? "INDEXED" : "FAILED";
            item.setStatus(status);

            if (!isAvailable) {
                item.setError("Ошибка индексации: сайт не доступен");
            }

            int pages = pageRepository.countBySiteUrl(site.getUrl());
            int lemmas = lemmaRepository.countBySiteUrl(site.getUrl());


            item.setPages(pages);
            item.setLemmas(lemmas);
            item.setStatusTime(System.currentTimeMillis());

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

    private boolean checkSiteAvailability(String url) {
        try {
            restTemplate.headForHeaders(url);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
