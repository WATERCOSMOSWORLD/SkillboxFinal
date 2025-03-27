package searchengine.services;

import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.springframework.transaction.annotation.Transactional;
import searchengine.repository.PageRepository;
import searchengine.repository.SiteRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;
import searchengine.model.Page;
import searchengine.model.Lemma;
import searchengine.model.Index;
import searchengine.model.Site;
import searchengine.model.IndexingStatus;
import java.util.*;
import java.util.concurrent.RecursiveTask;

public class IndexPageSite extends RecursiveTask<Void> {
    private final Site site;
    private final String url;
    private final Set<String> visitedPages;
    private final PageRepository pageRepository;
    private final SiteRepository siteRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private LuceneMorphology russianMorphology;
    private LuceneMorphology englishMorphology;
    private final Logger logger;

    public IndexPageSite(Site site, String url, Set<String> visitedPages,
                         PageRepository pageRepository, SiteRepository siteRepository,
                         LemmaRepository lemmaRepository, IndexRepository indexRepository,
                         Logger logger) {
        this.site = site;
        this.url = url;
        this.visitedPages = visitedPages;
        this.pageRepository = pageRepository;
        this.siteRepository = siteRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexRepository = indexRepository;
        this.logger = logger;
        try {
            this.russianMorphology = new RussianLuceneMorphology();
            this.englishMorphology = new EnglishLuceneMorphology();
        } catch (IOException e) {
            throw new RuntimeException("Ошибка инициализации морфологии", e);
        }
    }

    @Override
    protected Void compute() {
        if (!visitedPages.add(url) || shouldSkipUrl(url) || pageRepository.existsByPath(url.replace(site.getUrl(), ""))) {
            return null;
        }

        long startTime = System.currentTimeMillis();

        try {
            long delay = 500 + (long) (Math.random() * 4500);
            Thread.sleep(delay);

            site.setStatusTime(LocalDateTime.now());
            siteRepository.save(site);

            logger.info("🌍 Загружаем страницу: {}", url);

            Document document = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0")
                    .referrer("http://www.google.com")
                    .ignoreContentType(true)
                    .get();

            String contentType = document.connection().response().contentType();
            int responseCode = document.connection().response().statusCode();

            Page page = new Page();
            page.setPath(url.replace(site.getUrl(), ""));
            page.setSite(site);
            page.setCode(responseCode);

            if (contentType.startsWith("text/html")) {
                page.setContent(document.html());
                indexFilesAndImages(document);
            } else if (contentType.startsWith("image/") || contentType.startsWith("application/")) {
                page.setContent("FILE: " + url);
            }

            pageRepository.save(page);

            // 🔹 Вызываем метод лемматизации после сохранения страницы
            processPageContent(page);

            long endTime = System.currentTimeMillis();
            logger.info("✅ [{}] Проиндексировано за {} мс: {}", responseCode, (endTime - startTime), url);

            Elements links = document.select("a[href]");
            List<IndexPageSite> subTasks = new ArrayList<>();
            for (var link : links) {
                String linkUrl = cleanUrl(link.absUrl("href"));
                if (linkUrl.startsWith(site.getUrl()) && !shouldSkipUrl(linkUrl)) {
                    subTasks.add(new IndexPageSite(site, linkUrl, visitedPages, pageRepository,
                            siteRepository, lemmaRepository, indexRepository, logger));
                }
            }

            logger.info("🔗 Найдено ссылок: {}", subTasks.size());
            invokeAll(subTasks);

        } catch (IOException e) {
            handleException("❌ Ошибка при загрузке", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleException("⏳ Поток прерван", e);
        }

        return null;
    }

    private void indexFilesAndImages(Document document) {
        Elements images = document.select("img[src]");
        Elements files = document.select("a[href]");

        for (var img : images) {
            String imgUrl = cleanUrl(img.absUrl("src"));
            saveMedia(imgUrl, "image");
        }

        for (var file : files) {
            String fileUrl = cleanUrl(file.absUrl("href"));
            if (fileUrl.matches(".*\\.(pdf|docx|xlsx|zip|rar)$")) {
                saveMedia(fileUrl, "file");
            }
        }
    }

    private void saveMedia(String url, String type) {
        Page mediaPage = new Page();
        mediaPage.setPath(url.replace(site.getUrl(), ""));
        mediaPage.setSite(site);
        mediaPage.setCode(200);
        mediaPage.setContent(type.toUpperCase() + ": " + url);
        pageRepository.save(mediaPage);

        logger.info("📂 Добавлен {}: {}", type, url);
    }

    private void handleException(String message, Exception e) {
        logger.error("{} {}: {}", message, url, e.getMessage(), e);
        site.setStatus(IndexingStatus.FAILED);
        site.setStatusTime(LocalDateTime.now());
        site.setLastError(message + " " + url + ": " + e.getMessage());
        siteRepository.save(site);
    }

    private String cleanUrl(String url) {
        return url.replaceAll("#.*", "").replaceAll("\\?.*", "");
    }

    private boolean shouldSkipUrl(String url) {
        return url.contains("/basket") || url.contains("/cart") || url.contains("/checkout");
    }

    @Transactional
    private void processPageContent(Page page) {
        // 🔥 Добавь сюда вызов твоей функции лемматизации!
        logger.info("🔍 Обрабатываем леммы для: {}", page.getPath());

        String text = extractTextFromHtml(page.getContent());
        Map<String, Integer> lemmas = lemmatizeText(text);

        for (Map.Entry<String, Integer> entry : lemmas.entrySet()) {
            String lemmaText = entry.getKey();
            int count = entry.getValue();

            Lemma lemma = lemmaRepository.findByLemmaAndSite(lemmaText, page.getSite())
                    .orElseGet(() -> new Lemma(null, page.getSite(), lemmaText, 0));

            lemma.setFrequency(lemma.getFrequency() + count);
            lemmaRepository.save(lemma);

            Index index = new Index(null, page, lemma, (float) count);
            indexRepository.save(index);
        }

        logger.info("✅ Лемматизация завершена для: {}", page.getPath());
    }

    private String extractTextFromHtml(String html) {
        return Jsoup.parse(html).text();
    }

    private Map<String, Integer> lemmatizeText(String text) {
        Map<String, Integer> lemmas = new HashMap<>();
        String[] words = text.toLowerCase().replaceAll("[^а-яa-z\\s]", "").split("\\s+");

        for (String word : words) {
            if (word.length() < 3) continue; // Пропускаем короткие слова

            List<String> normalForms;
            if (word.matches("[а-я]+")) {
                normalForms = russianMorphology.getNormalForms(word);
            } else if (word.matches("[a-z]+")) {
                normalForms = englishMorphology.getNormalForms(word);
            } else {
                continue;
            }

            for (String lemma : normalForms) {
                lemmas.put(lemma, lemmas.getOrDefault(lemma, 0) + 1);
            }
        }
        return lemmas;
    }
}
