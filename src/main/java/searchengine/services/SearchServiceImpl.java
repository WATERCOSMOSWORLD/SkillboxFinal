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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class SearchServiceImpl implements SearchService {
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private final LemmaProcessor lemmaProcessor;

    public SearchServiceImpl(PageRepository pageRepository,
                             LemmaRepository lemmaRepository,
                             IndexRepository indexRepository,
                             LemmaProcessor lemmaProcessor) {
        this.pageRepository = pageRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
        this.lemmaProcessor = lemmaProcessor;
    }

    @Override
    public SearchResponse search(String query, String site, int offset, int limit) {
        // Проверка входного запроса
        if (query == null || query.trim().isEmpty()) {
            return new SearchResponse("Задан пустой поисковый запрос");
        }

        // Извлечение лемм
        List<String> lemmas = lemmaProcessor.extractLemmas(query);
        if (lemmas.isEmpty()) {
            return new SearchResponse("Не удалось обработать запрос");
        }

        // Получение списка страниц по леммам и, возможно, по сайту
        List<Page> pages;
        if (site == null || site.isEmpty()) {
            pages = pageRepository.findPagesByLemmas(lemmas);
        } else {
            pages = pageRepository.findPagesByLemmas(lemmas, site);
        }

        // Формирование результатов поиска
        List<SearchResult> results = pages.stream()
                .map(page -> new SearchResult(
                        buildFullUrl(page),
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


    private String buildFullUrl(Page page) {
        String baseUrl = page.getSite().getUrl();
        String path = page.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return baseUrl + path;
    }


    private String generateSnippet(String content, List<String> lemmas) {
        String lowerContent = content.toLowerCase();
        int minIndex = Integer.MAX_VALUE;
        int maxIndex = -1;

        // Определяем минимальный и максимальный индексы вхождений всех лемм
        for (String lemma : lemmas) {
            String lowerLemma = lemma.toLowerCase();
            int index = lowerContent.indexOf(lowerLemma);
            while (index != -1) {
                minIndex = Math.min(minIndex, index);
                maxIndex = Math.max(maxIndex, index + lowerLemma.length());
                index = lowerContent.indexOf(lowerLemma, index + 1);
            }
        }

        if (maxIndex == -1 || minIndex == Integer.MAX_VALUE) {
            return "...Совпадений не найдено...";
        }

        // Расширяем диапазон сниппета на 50 символов с каждой стороны
        int snippetStart = Math.max(minIndex - 50, 0);
        int snippetEnd = Math.min(maxIndex + 50, content.length());
        String snippet = content.substring(snippetStart, snippetEnd);

        // Выделяем найденные совпадения тегом <b>
        for (String lemma : lemmas) {
            Pattern pattern = Pattern.compile("(?i)" + Pattern.quote(lemma));
            Matcher matcher = pattern.matcher(snippet);
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(sb, "<b>" + matcher.group() + "</b>");
            }
            matcher.appendTail(sb);
            snippet = sb.toString();
        }
        return "..." + snippet + "...";
    }


    private double calculateRelevance(Page page, List<String> lemmas) {
        String lowerContent = page.getContent().toLowerCase();
        double relevance = 0.0;
        for (String lemma : lemmas) {
            String lowerLemma = lemma.toLowerCase();
            int index = 0;
            int count = 0;
            while ((index = lowerContent.indexOf(lowerLemma, index)) != -1) {
                count++;
                index += lowerLemma.length();
            }
            relevance += count;
        }
        return relevance;
    }
}
