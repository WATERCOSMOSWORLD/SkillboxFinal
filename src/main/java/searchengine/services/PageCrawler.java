package searchengine.services;

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
    private final SitesList sitesList;
    private final int depth;  // <-- Добавляем глубину

    public PageCrawler(Site site, LemmaRepository lemmaRepository, SiteRepository siteRepository,
                       IndexRepository indexRepository, String url, Set<String> visitedUrls,
                       PageRepository pageRepository, IndexingService indexingService,
                       SitesList sitesList, int depth) {  // <-- Добавляем параметр depth
        this.site = site;
        this.url = url;
        this.visitedUrls = visitedUrls;
        this.pageRepository = pageRepository;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;
        this.indexingService = indexingService;
        this.siteRepository = siteRepository;
        this.sitesList = sitesList;
        this.depth = depth;  // <-- Сохраняем глубину
    }

    @Override
    protected void compute() {
        if (!indexingService.isIndexingInProgress() || depth > 3 || !visitedPages.add(url) || shouldSkipUrl(url) ||
                pageRepository.existsByPath(url.replace(site.getUrl(), ""))) {
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            // Добавляем задержку для предотвращения перегрузки сервера
            long delay = 4000 + (long) (Math.random() * 8000);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Поток прерван: {}", e.getMessage());
                return;
            }

            if (!indexingService.isIndexingInProgress()) {
                return;
            }

            site.setStatusTime(LocalDateTime.now());
            siteRepository.save(site);

            logger.info("🌍 Загружаем страницу (глубина {}): {}", depth, url);

            // Получаем и парсим страницу
            try {
                Document document = Jsoup.connect(url)
                        .userAgent("Mozilla/5.0")
                        .referrer("http://www.google.com")
                        .ignoreContentType(true)
                        .timeout(10000)
                        .get();

                if (!indexingService.isIndexingInProgress()) {
                    return;
                }

                // Получаем код ответа и тип контента
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
                indexingService.processPageContent(page);

                long endTime = System.currentTimeMillis();
                logger.info("✅ [{}] Проиндексировано за {} мс (глубина {}): {}",
                        responseCode, (endTime - startTime), depth, url);

                // 🔥 Запускаем обработку внутренних ссылок только если глубина < 3
                if (depth < 3 && indexingService.isIndexingInProgress()) {
                    processLinks(document, depth + 1);
                }

            } catch (IOException e) {
                logger.warn("⚠️ Тайм-аут при загрузке: {}. Пропускаем.", url);
            }

        } finally {
            finalizeIndexing();
        }
    }



    private void finalizeIndexing() {
        indexingService.checkAndUpdateStatus(site.getUrl());
        logger.info("Индексация завершена для URL: {}", url);
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
        if (shouldSkipUrl(url)) {
            logger.info("🚨 Медиа-ссылка {} пропущена (фильтр URL)", url);
            return;
        }

        Page mediaPage = new Page();
        mediaPage.setPath(url.replace(site.getUrl(), ""));
        mediaPage.setSite(site);
        mediaPage.setCode(200);
        mediaPage.setContent(type.toUpperCase() + ": " + url);
        pageRepository.save(mediaPage);

        logger.info("📂 Добавлен {}: {}", type, url);
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
                    indexingService,               // IndexingService object
                    sitesList,                      // SitesList object
                    0                               // <-- Начальная глубина (0)
            ));

        } finally {
            forkJoinPool.shutdown();
        }
    }

    private boolean shouldSkipUrl(String url) {
        if (!isUrlWithinConfiguredSites(url)) {
            logger.info("URL пропущен (не относится к конфигурированным сайтам): {}", url);
            return true;
        }

        // Фильтр рекламных ссылок (utm, ad, track, banner, promo и др.)
        if (url.matches(".*(utm_|clickid=|affid=|ref=|ad=|track=|banner=|promo=|partner=|campaign=|source=).*")) {
            logger.info("⛔ URL пропущен (рекламная ссылка): {}", url);
            return true;
        }

        // Фильтр рекламных сервисов (Яндекс Метрика, Google Analytics, DoubleClick и др.)
        List<String> blockedDomains = Arrays.asList(
                "mc.yandex.ru", "google-analytics.com", "doubleclick.net",
                "adservice.google.com", "googletagmanager.com", "vk.com/rtrg"
        );
        for (String domain : blockedDomains) {
            if (url.contains(domain)) {
                logger.info("🚫 URL пропущен (подозрительный рекламный домен): {}", url);
                return true;
            }
        }

        // Пропуск ненужных разделов сайтов (корзина, оформление заказа и т. д.)
        return url.contains("/basket") || url.contains("/cart") || url.contains("/checkout");
    }

    private boolean isUrlWithinConfiguredSites(String url) {
        // Проверяем, начинается ли URL с URL конфигурированного сайта
        return sitesList.getSites().stream()
                .anyMatch(configSite -> url.startsWith(configSite.getUrl()));
    }

    private void processLinks(Document document, int currentDepth) {
        if (currentDepth >= 3) {
            logger.debug("Достигнута максимальная глубина индексации ({}). Остановка обработки ссылок.", currentDepth);
            return;
        }

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
                                sitesList,                     // SitesList object
                                currentDepth + 1               // Передаём увеличенную глубину
                        ));
                        logger.debug("Добавлена ссылка в обработку (глубина {}): {}", currentDepth + 1, childUrl);
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
