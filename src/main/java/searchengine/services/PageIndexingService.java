package searchengine.services;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import searchengine.model.Lemma;
import searchengine.model.Index;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import searchengine.config.SitesList;
import searchengine.config.ConfigSite;
import searchengine.model.Page;
import searchengine.model.Site;
import searchengine.model.IndexingStatus;
import searchengine.repository.IndexRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.PageRepository;
import searchengine.repository.SiteRepository;
import java.io.IOException;
import java.time.LocalDateTime;

import java.util.ArrayList;
import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

@Service
public class PageIndexingService {
    private static final Logger logger = LoggerFactory.getLogger(PageIndexingService.class);

    private final PageRepository pageRepository;
    private final SiteRepository siteRepository;
    private final SitesList sitesList;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private LuceneMorphology russianMorphology;
    private LuceneMorphology englishMorphology;
    private final Set<String> visitedPages = new ConcurrentSkipListSet<>();

    public PageIndexingService(PageRepository pageRepository,LemmaRepository lemmaRepository,IndexRepository indexRepository, SiteRepository siteRepository, SitesList sitesList) {
        this.pageRepository = pageRepository;
        this.siteRepository = siteRepository;
        this.sitesList = sitesList;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;

        try {
            this.russianMorphology = new RussianLuceneMorphology();
            this.englishMorphology = new EnglishLuceneMorphology();
        } catch (IOException e) {
            throw new RuntimeException("Ошибка инициализации морфологии", e);
        }
    }


    @Transactional
    public void processPageContent(Page page) {
        String text = extractTextFromHtml(page.getContent());
        Map<String, Integer> lemmas = lemmatizeText(text);

        Set<String> processedLemmas = new HashSet<>();
        List<Lemma> lemmasToSave = new ArrayList<>();
        List<Index> indexesToSave = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : lemmas.entrySet()) {
            String lemmaText = entry.getKey();
            int count = entry.getValue();

            logger.info("🔤 Найдена лемма: '{}', частота: {}", lemmaText, count);

            List<Lemma> foundLemmas = lemmaRepository.findByLemma(lemmaText);
            Lemma lemma;

            if (foundLemmas.isEmpty()) {
                lemma = new Lemma(null, page.getSite(), lemmaText, 0);
            } else {
                lemma = foundLemmas.get(0);
            }

            if (!processedLemmas.contains(lemmaText)) {
                lemma.setFrequency(lemma.getFrequency() + 1);
                processedLemmas.add(lemmaText);
            }

            lemmasToSave.add(lemma);

            Index index = new Index(null, page, lemma, (float) count);
            indexesToSave.add(index);
        }

        lemmaRepository.saveAll(lemmasToSave);
        indexRepository.saveAll(indexesToSave);
    }

    private String extractTextFromHtml(String html) {
        return Jsoup.parse(html).text();
    }

    private Map<String, Integer> lemmatizeText(String text) {
        Map<String, Integer> lemmas = new HashMap<>();
        String[] words = text.toLowerCase().replaceAll("[^а-яa-z\\s]", "").split("\\s+");

        for (String word : words) {
            if (word.length() < 3) continue;

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


    public boolean indexPage(String baseUrl) {
        long startTime = System.currentTimeMillis();
        Site site = null;

        try {
            ConfigSite configSite = getConfigSiteByUrl(baseUrl);
            if (configSite == null) {
                logger.warn("⚠️ Сайт {} не найден в конфигурации!", baseUrl);
                return false;
            }

            deleteSiteData(baseUrl);

            site = new Site();
            site.setUrl(baseUrl);
            site.setName(configSite.getName());
            site.setStatus(IndexingStatus.INDEXING);
            site.setStatusTime(LocalDateTime.now());
            site = siteRepository.saveAndFlush(site);

            logger.info("🔄 Начинаем индексацию сайта: {}", baseUrl);
            ForkJoinPool forkJoinPool = new ForkJoinPool();
            forkJoinPool.invoke(new PageCrawler(site, baseUrl));

            site.setStatus(IndexingStatus.INDEXED);
            site.setStatusTime(LocalDateTime.now());
            site.setLastError(null);
            siteRepository.save(site);

            long endTime = System.currentTimeMillis();
            logger.info("✅ Индексация завершена за {} сек. Сайт помечен как INDEXED.", (endTime - startTime) / 1000);
            return true;
        } catch (Exception e) {
            logger.error("❌ Ошибка при индексации сайта {}: {}", baseUrl, e.getMessage(), e);

            if (site != null) {
                site.setStatus(IndexingStatus.FAILED);
                site.setStatusTime(LocalDateTime.now());
                site.setLastError("Ошибка индексации: " + e.getMessage());
                siteRepository.save(site);
            }

            return false;
        }
    }





    @Transactional
    private void deleteSiteData(String siteUrl) {
        searchengine.model.Site site = siteRepository.findByUrl(siteUrl);
        if (site != null) {
            Long siteId = (long) site.getId();

            int indexesDeleted = indexRepository.deleteBySiteId(site.getId());

            int lemmasDeleted = lemmaRepository.deleteBySiteId(siteId);

            int pagesDeleted = pageRepository.deleteAllBySiteId(site.getId());

            siteRepository.delete(site);

            logger.info("Удалено {} записей из таблицы index.", indexesDeleted);
            logger.info("Удалено {} записей из таблицы lemma.", lemmasDeleted);
            logger.info("Удалено {} записей из таблицы page для сайта {}.", pagesDeleted, siteUrl);
            logger.info("Сайт {} успешно удален.", siteUrl);
        } else {
            logger.warn("Сайт {} не найден в базе данных.", siteUrl);
        }
    }



    private ConfigSite getConfigSiteByUrl(String url) {
        return sitesList.getSites().stream()
                .filter(site -> site.getUrl().equalsIgnoreCase(url))
                .findFirst()
                .orElse(null);
    }

    // 🔹 Класс для обхода страниц сайта
    private class PageCrawler extends RecursiveTask<Void> {
        private final Site site;
        private final String url;

        public PageCrawler(Site site, String url) {
            this.site = site;
            this.url = url;
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

                processPageContent(page);

                long endTime = System.currentTimeMillis();
                logger.info("✅ [{}] Проиндексировано за {} мс: {}", responseCode, (endTime - startTime), url);

                Elements links = document.select("a[href]");
                List<PageCrawler> subTasks = links.stream()
                        .map(link -> cleanUrl(link.absUrl("href")))
                        .filter(link -> link.startsWith(site.getUrl()) && !shouldSkipUrl(link))
                        .map(link -> new PageCrawler(site, link))
                        .toList();

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
    }


}