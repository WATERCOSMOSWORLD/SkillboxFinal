package searchengine.services;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.transaction.annotation.Transactional;
import searchengine.model.*;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;
import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import java.io.IOException;
import java.net.URL;
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
        // Проверка на уже посещенную страницу или пропуск URL
        if (!visitedPages.add(url) || shouldSkipUrl(url) || pageRepository.existsByPath(url.replace(site.getUrl(), ""))) {
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            // Задержка между запросами (от 500 до 5000 миллисекунд)
            long delay = 500 + (long) (Math.random() * 4500);
            Thread.sleep(delay);

            // Обновление времени последнего статуса сайта
            site.setStatusTime(LocalDateTime.now());
            siteRepository.save(site);

            logger.info("🌍 Загружаем страницу: {}", url);

            // Загрузка документа через Jsoup
            Document document = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0")
                    .referrer("http://www.google.com")
                    .ignoreContentType(true)
                    .get();

            // Получаем информацию о типе контента и коде ответа
            String contentType = document.connection().response().contentType();
            int responseCode = document.connection().response().statusCode();

            // Создаем объект страницы для сохранения
            Page page = new Page();
            page.setPath(url.replace(site.getUrl(), ""));
            page.setSite(site);
            page.setCode(responseCode);

            // Если это HTML-страница, сохраняем ее содержимое и индексируем файлы и изображения
            if (contentType.startsWith("text/html")) {
                page.setContent(document.html());
                indexFilesAndImages(document);
            } else if (contentType.startsWith("image/") || contentType.startsWith("application/")) {
                page.setContent("FILE: " + url);
            }

            // Сохраняем страницу в репозитории
            pageRepository.save(page);

            // Обрабатываем содержимое страницы (например, анализ лемм)
            processPageContent(page);

            long endTime = System.currentTimeMillis();
            logger.info("✅ [{}] Проиндексировано за {} мс: {}", responseCode, (endTime - startTime), url);

            // Извлекаем все ссылки на странице и создаем подзадачи для их обработки
            Elements links = document.select("a[href]");
            List<PageCrawler> subTasks = links.stream()
                    .map(link -> cleanUrl(link.absUrl("href")))
                    .filter(link -> link.startsWith(site.getUrl()) && !shouldSkipUrl(link))
                    .map(link -> new PageCrawler(
                            site,                          // передаем site
                            lemmaRepository,               // передаем lemmaRepository
                            siteRepository,                // передаем siteRepository
                            indexRepository,               // передаем indexRepository
                            link,                          // передаем ссылку как url
                            visitedUrls,                   // передаем visitedUrls
                            pageRepository,                // передаем pageRepository
                            indexingService                // передаем indexingService
                    ))
                    .toList();

            logger.info("🔗 Найдено ссылок: {}", subTasks.size());
            invokeAll(subTasks); // Запускаем параллельные задачи

        } catch (IOException e) {
            handleException("❌ Ошибка при загрузке", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Если поток был прерван
            handleException("⏳ Поток прерван", e);
        }

        // Дополнительная часть кода для обработки страницы
        try {
            if (!shouldProcessUrl()) return;

            applyRequestDelay(); // Применение задержки перед запросом
            if (!checkAndLogStopCondition("Перед запросом")) return;

            Connection.Response response = fetchPageContent(); // Получаем содержимое страницы
            if (response != null) {
                handleResponse(response);  // Обработка ответа
            }
        } catch (IOException | InterruptedException e) {
            handleException(e);
        } finally {
            finalizeIndexing(); // Завершаем индексацию
        }
    }






    private boolean shouldProcessUrl() {
        return checkAndLogStopCondition("Начало обработки") && markUrlAsVisited();
    }

    private void processPageContent() throws IOException, InterruptedException {
        applyRequestDelay();
        if (!checkAndLogStopCondition("Перед запросом")) return;
        Connection.Response response = fetchPageContent();
        if (response != null) {
            processFetchedContent(response);
        }
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
        return true;
    }

    private void applyRequestDelay() throws InterruptedException {
        long delay = 500 + new Random().nextInt(4500);
        logger.debug("Задержка перед запросом: {} ms для URL: {}", delay, url);
        Thread.sleep(delay);
    }

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

    private void handleResponse(Connection.Response response) throws IOException {
        String contentType = response.contentType();
        int statusCode = response.statusCode();
        String path = new URL(url).getPath();

        // Проверяем, существует ли уже такая страница
        if (pageRepository.existsByPathAndSiteId(path, site.getId())) {
            logger.info("Страница уже существует и была пропущена: {}", url);
            return;
        }

        // Создаём объект Page
        Page page = new Page();
        page.setSite(site);
        page.setPath(path);
        page.setCode(statusCode);

        // Обрабатываем контент в зависимости от типа
        if (contentType != null && contentType.startsWith("image/")) {
            // Обработка изображения
            page.setContent("Image content: " + contentType);
            pageRepository.save(page);
            logger.info("Изображение успешно добавлено для URL: {}", url);
        } else if (contentType != null && contentType.contains("text/html")) {
            // Обработка HTML-страницы
            try {
                processPageContent();  // Вызов без параметров
                logger.info("HTML-страница успешно обработана и добавлена для URL: {}", url);
            } catch (InterruptedException e) {
                // Обработка исключения InterruptedException
                Thread.currentThread().interrupt();  // Восстановление состояния прерывания
                logger.error("Индексация страницы была прервана для URL: {}", url, e);
            }
        } else {
            // Обработка неизвестного типа контента
            page.setContent("Unhandled content type: " + contentType);
            pageRepository.save(page);
            logger.info("Неизвестный тип контента был обработан и добавлен для URL: {}", url);
        }
    }

    private String extractText(Document document) {
        return document.text();
    }


    private Map<String, Integer> lemmatizeText(String text) throws IOException {
        Map<String, Integer> lemmaFrequencies = new HashMap<>();

        LuceneMorphology russianMorph = new RussianLuceneMorphology();
        LuceneMorphology englishMorph = new EnglishLuceneMorphology();

        String[] words = text.toLowerCase().split("\\P{L}+");

        for (String word : words) {
            if (word.length() < 2) continue;

            List<String> normalForms;
            if (word.matches("[а-яё]+")) {
                normalForms = russianMorph.getNormalForms(word);
            } else if (word.matches("[a-z]+")) {
                normalForms = englishMorph.getNormalForms(word);
            } else {
                continue;
            }

            for (String lemma : normalForms) {
                lemmaFrequencies.put(lemma, lemmaFrequencies.getOrDefault(lemma, 0) + 1);
            }
        }

        return lemmaFrequencies;
    }


    private void processLinks(Document document) {
        Elements links = document.select("a[href]");
        List<PageCrawler> subtasks = new ArrayList<>();

        for (Element link : links) {
            if (!checkAndLogStopCondition("При обработке ссылок")) return;

            String childUrl = link.absUrl("href");

            if (!childUrl.startsWith(site.getUrl())) {
                logger.debug("Ссылка {} находится за пределами корневого сайта {}. Пропускаем.", childUrl, site.getUrl());
                continue;
            }

            if (childUrl.startsWith("javascript:")) {
                logger.info("Обнаружена JavaScript ссылка: {}", childUrl);
                saveJavaScriptLink(childUrl);
                continue;
            }

            if (childUrl.startsWith("tel:")) {
                logger.info("Обнаружена телефонная ссылка: {}", childUrl);
                savePhoneLink(childUrl);
                continue;
            }

            String childPath = null;
            try {
                childPath = new URL(childUrl).getPath();
            } catch (Exception e) {
                logger.warn("Ошибка извлечения пути из URL: {}", childUrl);
            }

            synchronized (visitedUrls) {
                if (childPath != null && !visitedUrls.contains(childPath)) {
                    visitedUrls.add(childPath);
                    subtasks.add(new PageCrawler(
                            site,                          // объект сайта
                            lemmaRepository,               // репозиторий лемм
                            siteRepository,                // репозиторий сайта (добавьте этот параметр)
                            indexRepository,               // репозиторий индексов
                            childUrl,                      // дочерний URL
                            visitedUrls,                   // множество посещенных URL
                            pageRepository,                // репозиторий страниц
                            indexingService                // сервис индексации
                    ));

                    logger.debug("Добавлена ссылка в обработку: {}", childUrl);
                } else {
                    logger.debug("Ссылка уже обработана: {}", childUrl);
                }
            }
        }
        invokeAll(subtasks);
    }



    private void savePhoneLink(String telUrl) {
        String phoneNumber = telUrl.substring(4);
        if (pageRepository.existsByPathAndSiteId(phoneNumber, site.getId())) {
            logger.info("Телефонный номер {} уже сохранён. Пропускаем.", phoneNumber);
            return;
        }

        Page page = new Page();
        page.setSite(site);
        page.setPath(phoneNumber);
        page.setCode(0);
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
        page.setPath(jsUrl);
        page.setCode(0);
        page.setContent("JavaScript ссылка: " + jsUrl);
        pageRepository.save(page);

        logger.info("Сохранена JavaScript ссылка: {}", jsUrl);
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


    private void processFetchedContent(Connection.Response response) throws IOException {
        String contentType = response.contentType();
        int statusCode = response.statusCode();
        String path = new URL(url).getPath();

        if (pageRepository.existsByPathAndSiteId(path, site.getId())) {
            logger.info("Страница {} уже существует. Пропускаем сохранение.", url);
            return;
        }

        Page page = new Page();
        page.setSite(site);
        page.setPath(path);
        page.setCode(statusCode);

        if (contentType != null && contentType.startsWith("image/")) {
            processImageContent(page, contentType);
        } else if (contentType != null && contentType.contains("text/html")) {
            Document document = response.parse();

        } else {
            processUnknownContent(page, contentType);
        }
    }


    private void processImageContent(Page page, String contentType) {
        page.setContent("Image content: " + contentType);
        pageRepository.save(page);
        logger.info("Изображение добавлено: {}", url);
    }



    private void processUnknownContent(Page page, String contentType) {
        page.setContent("Unhandled content type: " + contentType);
        pageRepository.save(page);
        logger.info("Контент с неизвестным типом добавлен: {}", url);
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
    public void processPageContent(Page page) {
        try {
            // Извлекаем текст из HTML страницы
            String text = extractTextFromHtml(page.getContent());

            // Лемматизация текста
            Map<String, Integer> lemmas = lemmatizeText(text);

            Set<String> processedLemmas = new HashSet<>();
            List<Lemma> lemmasToSave = new ArrayList<>();
            List<Index> indexesToSave = new ArrayList<>();

            // Обработка лемм
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

            // Сохранение лемм и индексов в репозитории
            lemmaRepository.saveAll(lemmasToSave);
            indexRepository.saveAll(indexesToSave);

        } catch (IOException e) {
            // Логируем ошибку, если IOException был брошен
            logger.error("Ошибка при обработке страницы: {}", page.getPath(), e);
            // Можете также повторно выбросить исключение, если хотите остановить выполнение
            throw new RuntimeException("Ошибка при извлечении текста с страницы", e);
        }
    }

    private String extractTextFromHtml(String html) {
        return Jsoup.parse(html).text();
    }

}
