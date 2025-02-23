package searchengine.services;

import org.springframework.stereotype.Service;
import searchengine.dto.search.SearchResponse;
import searchengine.dto.search.SearchResult;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;
import searchengine.utils.LemmaProcessor;
import searchengine.model.Page;

import java.util.List;
import java.util.stream.Collectors;

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

        List<Page> pages;
        if (site == null || site.isEmpty()) {
            pages = pageRepository.findPagesByLemmas(lemmas);
        } else {
            pages = pageRepository.findPagesByLemmas(lemmas, site);
        }

        List<SearchResult> results = pages.stream()
                .map(page -> new SearchResult(
                        page.getSite().getUrl(),
                        page.getSite().getName(),
                        page.getPath(),
                        page.getTitle(),
                        generateSnippet(page.getContent(), lemmas),
                        calculateRelevance(page, lemmas)
                ))
                .sorted((a, b) -> Double.compare(b.getRelevance(), a.getRelevance()))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());

        return new SearchResponse(true, results.size(), results);
    }

    private String generateSnippet(String content, List<String> lemmas) {
        int snippetLength = 200; // Длина сниппета
        String lowerContent = content.toLowerCase();

        // Ищем первое вхождение любой леммы в тексте
        int bestIndex = -1;
        for (String lemma : lemmas) {
            int index = lowerContent.indexOf(lemma.toLowerCase());
            if (index != -1 && (bestIndex == -1 || index < bestIndex)) {
                bestIndex = index;
            }
        }

        if (bestIndex == -1) {
            return "...Совпадений не найдено...";
        }

        // Определяем границы сниппета
        int start = Math.max(bestIndex - 50, 0);
        int end = Math.min(start + snippetLength, content.length());

        // Выделяем совпадения в <b>
        String snippet = content.substring(start, end);
        for (String lemma : lemmas) {
            snippet = snippet.replaceAll("(?i)" + lemma, "<b>$0</b>");
        }

        return "..." + snippet + "...";
    }


    private double calculateRelevance(Page page, List<String> lemmas) {
        return 1.0;
    }
}
