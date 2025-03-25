package searchengine.services;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import searchengine.model.*;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;

import java.io.IOException;
import java.util.concurrent.ForkJoinPool;
import searchengine.repository.SiteRepository;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import org.springframework.context.annotation.Lazy;


@Lazy
public class PageCrawler extends RecursiveAction {
    private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);
    private final Site site;
    private final String url;
    private final Set<String> visitedUrls;
    private final PageRepository pageRepository;
    @Lazy
    private final IndexingService indexingService;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private final Set<String> visitedPages = new ConcurrentSkipListSet<>();
    private final SiteRepository siteRepository;

    public PageCrawler(Site site,LemmaRepository lemmaRepository,SiteRepository siteRepository,IndexRepository indexRepository, String url, Set<String> visitedUrls, PageRepository pageRepository,@Lazy IndexingService indexingService) {
        this.site = site;
        this.url = url;
        this.visitedUrls = visitedUrls;
        this.pageRepository = pageRepository;
        this.indexingService = indexingService;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;
        this.siteRepository = siteRepository;
    }

    @Override
    protected void compute() {
        if (!visitedPages.add(url) || shouldSkipUrl(url) || pageRepository.existsByPath(url.replace(site.getUrl(), ""))) {
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            long delay = 500 + (long) (Math.random() * 4500);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Поток прерван: {}", e.getMessage());
                return;
            }

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

            // Теперь вызываем processPageContent через indexingService
            indexingService.processPageContent(page);

            long endTime = System.currentTimeMillis();
            logger.info("✅ [{}] Проиндексировано за {} мс: {}", responseCode, (endTime - startTime), url);

            Elements links = document.select("a[href]");
            List<PageCrawler> subTasks = links.stream()
                    .map(link -> cleanUrl(link.absUrl("href")))
                    .filter(link -> link.startsWith(site.getUrl()) && !shouldSkipUrl(link))
                    .map(link -> new PageCrawler(
                            site,
                            lemmaRepository,
                            siteRepository,
                            indexRepository,
                            link,
                            visitedUrls,
                            pageRepository,
                            indexingService
                    ))
                    .toList();

            logger.info("🔗 Найдено ссылок: {}", subTasks.size());
            invokeAll(subTasks);

        } catch (IOException e) {
            handleException("❌ Ошибка при загрузке", e);
        }

        try {
            if (!shouldProcessUrl()) return;

            if (!checkAndLogStopCondition("Перед запросом")) return;

            Connection.Response response = fetchPageContent();
            if (response != null) {
                // Удален вызов handleResponse(response)
            }
        } catch (IOException e) {
            handleException(e);
        } finally {
            finalizeIndexing();
        }
    }

    private boolean shouldProcessUrl() {
        return checkAndLogStopCondition("Начало обработки") && markUrlAsVisited();
    }

    private void handleException(Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        handleError(new IOException("Ошибка при обработке страницы", e));
    }

    private boolean markUrlAsVisited() {
        synchronized (visitedUrls) {
            if (visitedUrls.contains(url)) {
                logger.debug("URL уже обработан: {}", url);
                return false;
            }
            visitedUrls.add(url);
        }
        return true;}

    private Connection.Response fetchPageContent() throws IOException {
        logger.info("Обработка URL: {}", url);
        Connection.Response response = Jsoup.connect(url)
                .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
                .referrer("http://www.google.com")
                .ignoreContentType(true)
                .execute();

        if (response.statusCode() >= 400) {
            logger.warn("Ошибка {} при индексации страницы {}", response.statusCode(), url);
            return null;
        }
        return response;
    }

    private void finalizeIndexing() {
        indexingService.checkAndUpdateStatus(site.getUrl());
        logger.info("Индексация завершена для URL: {}", url);
    }

    private void handleError(IOException e) {
        logger.warn("Ошибка обработки URL {}: {}", url, e.getMessage());
        Page page = new Page();
        page.setSite(site);
        page.setPath(url);
        page.setCode(0);
        page.setContent("Ошибка обработки: " + e.getMessage());
        pageRepository.save(page);
    }

    private boolean checkAndLogStopCondition(String stage) {
        if (!indexingService.isIndexingInProgress()) {
            logger.info("Индексация прервана на этапе {} для URL: {}", stage, url);
            return false;
        }
        return true;
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

    public static void startCrawling(Site site, String startUrl,
                                     LemmaRepository lemmaRepository,
                                     SiteRepository siteRepository,
                                     IndexRepository indexRepository,
                                     PageRepository pageRepository,
                                     IndexingService indexingService) {  // Добавляем параметр
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        try {
            forkJoinPool.invoke(new PageCrawler(
                    site, lemmaRepository, siteRepository, indexRepository,
                    startUrl, new HashSet<>(), pageRepository, indexingService // Передаём indexingService
            ));
        } finally {
            forkJoinPool.shutdown();
        }
    }
}
