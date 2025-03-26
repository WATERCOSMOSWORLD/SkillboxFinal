package searchengine.services;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import searchengine.dto.search.SearchResponse;
import searchengine.dto.search.SearchResult;
import searchengine.repository.PageRepository;
import searchengine.repository.SiteRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;

import searchengine.utils.LemmaProcessor;
import searchengine.model.Page;
import java.util.stream.Collectors;
import java.util.*;

@Service

public class SearchServiceImpl implements SearchService {
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private final LemmaProcessor lemmaProcessor;
    private final SiteRepository siteRepository;
    private static final Logger logger = LoggerFactory.getLogger(IndexingService.class);

    public SearchServiceImpl(PageRepository pageRepository,SiteRepository siteRepository, LemmaRepository lemmaRepository, IndexRepository indexRepository, LemmaProcessor lemmaProcessor) {
        this.pageRepository = pageRepository;
        this. siteRepository =  siteRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
        this.lemmaProcessor = lemmaProcessor;
    }

    @Override
    public SearchResponse search(String query, String site, int offset, int limit) {
        if (query == null || query.trim().isEmpty()) {
            return new SearchResponse("❌ Задан пустой поисковый запрос");
        }

        logger.info("🔎 Получен поисковый запрос: '{}'", query);

        // Извлекаем леммы
        List<String> lemmas = lemmaProcessor.extractLemmas(query);
        if (lemmas.isEmpty()) {
            return new SearchResponse("❌ Не удалось обработать запрос");
        }
        logger.info("📌 Извлеченные леммы: {}", lemmas);

        // Получаем страницы с этими леммами
        List<Page> pages;
        if (site == null || site.isEmpty()) {
            pages = new ArrayList<>(new HashSet<>(pageRepository.findPagesByLemmas(lemmas)));
        } else {
            pages = new ArrayList<>(new HashSet<>(pageRepository.findPagesByLemmas(lemmas, site)));
        }

        if (pages.isEmpty()) {
            return new SearchResponse("❌ Нет результатов по вашему запросу.");
        }

        logger.info("📌 Найдено страниц: {}", pages.size());

        // Подсчитываем частоту лемм
        Map<String, Integer> lemmaFrequencyMap = new HashMap<>();
        for (String lemma : lemmas) {
            int frequency = Optional.ofNullable(lemmaRepository.countByLemma(lemma)).orElse(0);
            lemmaFrequencyMap.put(lemma, frequency);
        }

        // Формируем результаты поиска
        List<SearchResult> results = pages.stream()
                .map(page -> {
                    String siteUrl = page.getSite().getUrl().replaceAll("/$", "");
                    String siteName = page.getSite().getName();
                    String pagePath = page.getPath().replaceAll("^/", "");
                    String fullUrl = siteUrl + "/" + pagePath;

                    return new SearchResult(
                            siteUrl,
                            siteName,
                            "/" + pagePath,
                            page.getTitle(),
                            generateSnippet(page.getContent(), lemmas),
                            calculateRelevance(page, lemmas, lemmaFrequencyMap)
                    );
                })
                .sorted(Comparator.comparingDouble(SearchResult::getRelevance).reversed())
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());

        logger.info("✅ Поиск завершен. Отдано результатов: {}", results.size());

        return new SearchResponse(true, results.size(), results);
    }


    private String generateSnippet(String content, List<String> lemmas) {
        int snippetLength = 200;
        String lowerContent = content.toLowerCase();

        TreeMap<Integer, String> positions = new TreeMap<>();
        for (String lemma : lemmas) {
            int index = lowerContent.indexOf(lemma.toLowerCase());
            while (index != -1) {
                positions.put(index, lemma);
                index = lowerContent.indexOf(lemma.toLowerCase(), index + 1);
            }
        }

        if (positions.isEmpty()) {
            return "...Совпадений не найдено...";
        }

        StringBuilder snippetBuilder = new StringBuilder();
        int fragments = 0;
        for (Map.Entry<Integer, String> entry : positions.entrySet()) {
            if (fragments >= 3) break;
            int start = Math.max(entry.getKey() - 50, 0);
            int end = Math.min(start + snippetLength, content.length());

            String snippetPart = content.substring(start, end);
            for (String lemma : lemmas) {
                snippetPart = snippetPart.replaceAll("(?i)" + lemma, "<b>$0</b>");
            }

            snippetBuilder.append("...").append(snippetPart).append("...");
            fragments++;
        }

        return snippetBuilder.toString();
    }

    private double calculateRelevance(Page page, List<String> lemmas, Map<String, Integer> lemmaFrequencyMap) {
        double relevance = 0.0;
        long totalPages = pageRepository.count();

        for (String lemma : lemmas) {
            int frequency = lemmaFrequencyMap.getOrDefault(lemma, 1);
            int docsWithLemma = Math.max(Optional.ofNullable(lemmaRepository.countByLemma(lemma)).orElse(1), 1);

            double tf = (double) frequency / Math.max(page.getContent().length(), 1);
            double idf = Math.log((double) totalPages / docsWithLemma + 1);

            relevance += tf * idf;
        }
        return relevance;
    }
}
