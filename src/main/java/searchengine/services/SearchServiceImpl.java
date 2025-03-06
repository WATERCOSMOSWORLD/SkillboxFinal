package searchengine.services;

import org.springframework.stereotype.Service;
import searchengine.dto.search.SearchResponse;
import searchengine.dto.search.SearchResult;
import searchengine.repository.PageRepository;
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

    public SearchServiceImpl(PageRepository pageRepository, LemmaRepository lemmaRepository, IndexRepository indexRepository, LemmaProcessor lemmaProcessor) {
        this.pageRepository = pageRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
        this.lemmaProcessor = lemmaProcessor;
    }

    @Override
    public SearchResponse search(String query, String site, int offset, int limit) {
        if (query == null || query.trim().isEmpty()) {
            return new SearchResponse("Задан пустой поисковый запрос");
        }

        List<String> lemmas = lemmaProcessor.extractLemmas(query);
        if (lemmas.isEmpty()) {
            return new SearchResponse("Не удалось обработать запрос");
        }

        List<Page> pages = (site == null || site.isEmpty())
                ? pageRepository.findPagesByLemmas(lemmas)
                : pageRepository.findPagesByLemmas(lemmas, site);

        // Подсчитываем частоту встречаемости каждой леммы
        Map<String, Integer> lemmaFrequencyMap = new HashMap<>();
        for (String lemma : lemmas) {
            int frequency = lemmaRepository.countByLemma(lemma);
            lemmaFrequencyMap.put(lemma, frequency);
        }

        // Формируем результаты поиска
        List<SearchResult> results = pages.stream()
                .map(page -> {
                    String siteUrl = page.getSite().getUrl();
                    String siteName = page.getSite().getName();
                    String pagePath = page.getPath();

                    // Убираем лишний слеш в siteUrl
                    siteUrl = siteUrl.replaceAll("/$", "");
                    // Убираем лишний слеш в начале pagePath
                    pagePath = pagePath.replaceAll("^/", "");

                    String fullUrl = siteUrl + "/" + pagePath;

                    return new SearchResult(
                            siteUrl,  // Теперь должно корректно передавать сайт
                            siteName,
                            "/" + pagePath,  // `uri` должен содержать только путь, а не полный URL
                            page.getTitle(),
                            generateSnippet(page.getContent(), lemmas),
                            calculateRelevance(page, lemmas, lemmaFrequencyMap)
                    );
                })
                .sorted(Comparator.comparingDouble(SearchResult::getRelevance).reversed())
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());


        return new SearchResponse(true, results.size(), results);
    }

    private String generateSnippet(String content, List<String> lemmas) {
        int snippetLength = 200;
        String lowerContent = content.toLowerCase();

        // Используем TreeMap для сортировки позиций в тексте
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

        // Берем 2-3 фрагмента с наиболее ранними вхождениями
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
        long totalPages = pageRepository.count(); // Общее число страниц в БД

        for (String lemma : lemmas) {
            int frequency = lemmaFrequencyMap.getOrDefault(lemma, 1);
            int docsWithLemma = Math.max(lemmaRepository.countByLemma(lemma), 1);

            // TF-IDF: (Частота в документе) * log(Общее число документов / Документы с данной леммой)
            double tf = (double) frequency / Math.max(page.getContent().length(), 1);
            double idf = Math.log((double) totalPages / docsWithLemma + 1); // +1 чтобы избежать деления на 0

            relevance += tf * idf;
        }
        return relevance;
    }
}
