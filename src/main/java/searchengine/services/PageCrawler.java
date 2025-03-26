package searchengine.services;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import java.net.URI;

import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import searchengine.model.*;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;
import searchengine.config.SitesList;
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
    private final IndexingService indexingService;
    private final String url;
    private final Set<String> visitedUrls;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;
    private final Set<String> visitedPages = new ConcurrentSkipListSet<>();
    private final SiteRepository siteRepository;
    private final SitesList sitesList; // Add this field to store the list of configured sites

    public PageCrawler(Site site, LemmaRepository lemmaRepository, SiteRepository siteRepository,
                       IndexRepository indexRepository, String url, Set<String> visitedUrls,
                       PageRepository pageRepository, IndexingService indexingService, SitesList sitesList) {
        this.site = site;
        this.url = url;
        this.visitedUrls = visitedUrls;
        this.pageRepository = pageRepository;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexingService = indexingService;
        this.siteRepository = siteRepository;
        this.sitesList = sitesList;  // Initialize the sitesList
    }

    @Override
    protected void compute() {
        // Skip already visited pages or those that should be skipped
        if (!visitedPages.add(url) || shouldSkipUrl(url) || pageRepository.existsByPath(url.replace(site.getUrl(), ""))) {
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            // Add random delay to prevent overloading the server (simulate human behavior)
            long delay = 4000 + (long) (Math.random() * 8000);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Поток прерван: {}", e.getMessage());
                return;
            }

            // Update the site's status time
            site.setStatusTime(LocalDateTime.now());
            siteRepository.save(site);

            logger.info("🌍 Загружаем страницу: {}", url);

            // Fetch and parse the document using Jsoup
            Document document = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0")
                    .referrer("http://www.google.com")
                    .ignoreContentType(true)
                    .get();

            // Get response code and content type
            String contentType = document.connection().response().contentType();
            int responseCode = document.connection().response().statusCode();

            // Create a new Page object for storing data
            Page page = new Page();
            page.setPath(url.replace(site.getUrl(), ""));
            page.setSite(site);
            page.setCode(responseCode);

            // Store content and index files/images based on content type
            if (contentType.startsWith("text/html")) {
                page.setContent(document.html());
                indexFilesAndImages(document);  // Process files/images if needed
            } else if (contentType.startsWith("image/") || contentType.startsWith("application/")) {
                page.setContent("FILE: " + url);  // Mark the URL as a file
            }

            // Save the page to the repository
            pageRepository.save(page);

            // Process the page content using the indexing service
            indexingService.processPageContent(page);

            long endTime = System.currentTimeMillis();
            logger.info("✅ [{}] Проиндексировано за {} мс: {}", responseCode, (endTime - startTime), url);

            // Process the links from the page (instead of manually extracting and processing links)
            processLinks(document);  // Call processLinks here to handle the link extraction and further task creation

        } catch (IOException e) {
            handleException("❌ Ошибка при загрузке", e);
        }

        try {
            // Ensure URL processing logic is handled properly
            if (!shouldProcessUrl()) return;

            if (!checkAndLogStopCondition("Перед запросом")) return;

            // Fetch page content and process the response
            Connection.Response response = fetchPageContent();
            if (response != null) {
                // Removed the unused handleResponse(response) call
            }
        } catch (IOException e) {
            handleException(e);
        } finally {
            finalizeIndexing();  // Ensure any cleanup or final processing is done
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


    public static void startCrawling(Site site, String startUrl,
                                     LemmaRepository lemmaRepository,
                                     SiteRepository siteRepository,
                                     IndexRepository indexRepository,
                                     PageRepository pageRepository,
                                     IndexingService indexingService,
                                     SitesList sitesList) {

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        try {
            // Invoke PageCrawler with necessary parameters, including SitesList and IndexingService
            forkJoinPool.invoke(new PageCrawler(
                    site,                          // Site object
                    lemmaRepository,               // LemmaRepository object
                    siteRepository,                // SiteRepository object
                    indexRepository,               // IndexRepository object
                    startUrl,                      // Starting URL for indexing
                    new HashSet<>(),               // Set of visited URLs
                    pageRepository,                // PageRepository object
                    indexingService,               // IndexingService object (for isInternalLink)
                    sitesList                      // SitesList object
            ));
        } finally {
            forkJoinPool.shutdown();
        }
    }

    private boolean shouldSkipUrl(String url) {
        if (!isUrlWithinConfiguredSites(url)) {
            logger.info("URL skipped (not part of configured sites): {}", url);
            return true;  // Skip URLs not belonging to the configured sites
        }
        return url.contains("/basket") || url.contains("/cart") || url.contains("/checkout");
    }


    private boolean isUrlWithinConfiguredSites(String url) {
        // Проверяем, начинается ли URL с URL конфигурированного сайта
        return sitesList.getSites().stream()
                .anyMatch(configSite -> url.startsWith(configSite.getUrl()));
    }

    private void processLinks(Document document) {
        Elements links = document.select("a[href]");
        List<PageCrawler> subtasks = new ArrayList<>();

        for (Element link : links) {
            if (!checkAndLogStopCondition("При обработке ссылок")) return;

            String childUrl = link.absUrl("href");

            // Проверяем, что ссылка принадлежит корневому сайту
            if (!childUrl.startsWith(site.getUrl())) {
                logger.debug("Ссылка {} находится за пределами корневого сайта. Пропускаем.", childUrl);
                continue;
            }

            // Пропускаем ссылки на другие сайты
            if (!isUrlWithinConfiguredSites(childUrl)) {
                logger.debug("Ссылка {} не принадлежит конфигурированному сайту. Пропускаем.", childUrl);
                continue;
            }

            // Обработка JavaScript ссылок
            if (childUrl.startsWith("javascript:")) {
                logger.info("Обнаружена JavaScript ссылка: {}", childUrl);
                saveJavaScriptLink(childUrl);
                continue;
            }

            // Обработка tel: ссылок
            if (childUrl.startsWith("tel:")) {
                logger.info("Обнаружена телефонная ссылка: {}", childUrl);
                savePhoneLink(childUrl);
                continue;
            }

            // Извлечение пути из URL
            String childPath = null;
            try {
                URI childUri = new URI(childUrl);
                childPath = childUri.getPath();
            } catch (Exception e) {
                logger.warn("Ошибка извлечения пути из URL: {}", childUrl);
            }

            // Добавление в очередь на обработку, если ссылка еще не была обработана
            if (childPath != null) {
                synchronized (visitedUrls) {
                    if (!visitedUrls.contains(childPath)) {
                        visitedUrls.add(childPath);
                        subtasks.add(new PageCrawler(
                                site,                          // Site object
                                lemmaRepository,               // LemmaRepository object
                                siteRepository,                // SiteRepository object
                                indexRepository,               // IndexRepository object
                                childUrl,                      // Starting URL for indexing
                                visitedUrls,                   // Set of visited URLs
                                pageRepository,                // PageRepository object
                                indexingService,               // IndexingService object
                                sitesList                      // SitesList object
                        ));
                        logger.debug("Добавлена ссылка в обработку: {}", childUrl);
                    } else {
                        logger.debug("Ссылка уже обработана: {}", childUrl);
                    }
                }
            }
        }

        // Запуск подзадач
        if (!subtasks.isEmpty()) {
            invokeAll(subtasks);
        }
    }


    private void savePhoneLink(String telUrl) {
        String phoneNumber = telUrl.substring(4); // Убираем "tel:"
        if (pageRepository.existsByPathAndSiteId(phoneNumber, site.getId())) {
            logger.info("Телефонный номер {} уже сохранён. Пропускаем.", phoneNumber);
            return;
        }

        Page page = new Page();
        page.setSite(site);
        page.setPath(phoneNumber);
        page.setCode(0); // Код 0 для телефонных ссылок
        page.setContent("Телефонный номер: " + phoneNumber);
        pageRepository.save(page);

        logger.info("Сохранён телефонный номер: {}", phoneNumber);
    }

    private void saveJavaScriptLink(String jsUrl) {
        if (pageRepository.existsByPathAndSiteId(jsUrl, site.getId())) {
            logger.info("JavaScript ссылка {} уже сохранена. Пропускаем.", jsUrl);
            return;
        }

        Page page = new Page();
        page.setSite(site);
        page.setPath(jsUrl); // Сохраняем полный jsUrl как path
        page.setCode(0); // Код 0 для JavaScript ссылок
        page.setContent("JavaScript ссылка: " + jsUrl);
        pageRepository.save(page);

        logger.info("Сохранена JavaScript ссылка: {}", jsUrl);
    }

}
