package searchengine.services;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import searchengine.model.Page;
import searchengine.model.Site;
import searchengine.model.Lemma;
import searchengine.model.Index;
import searchengine.repository.PageRepository;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;
import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.RecursiveAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PageCrawler extends RecursiveAction {
    private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);
    private final Site site;
    private final String url;
    private final Set<String> visitedUrls;
    private final PageRepository pageRepository;
    private final IndexingService indexingService;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;


    public PageCrawler(Site site,LemmaRepository lemmaRepository,IndexRepository indexRepository, String url, Set<String> visitedUrls, PageRepository pageRepository, IndexingService indexingService) {
        this.site = site;
        this.url = url;
        this.visitedUrls = visitedUrls;
        this.pageRepository = pageRepository;
        this.indexingService = indexingService;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;

    }

    @Override
    protected void compute() {
        try {
            if (!shouldProcessUrl()) return;

            applyRequestDelay();
            if (!checkAndLogStopCondition("Перед запросом")) return;

            Connection.Response response = fetchPageContent();
            if (response != null) {
                processFetchedContent(response);
            }
        } catch (IOException | InterruptedException e) {
            handleException(e);
        } finally {
            finalizeIndexing();
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

        if (pageRepository.existsByPathAndSiteId(path, site.getId())) {
            logger.info("Страница {} уже существует. Пропускаем сохранение.", url);
            return;
        }

        Page page = new Page();
        page.setSite(site);
        page.setPath(path);
        page.setCode(statusCode);

        if (contentType != null && contentType.startsWith("image/")) {
            page.setContent("Image content: " + contentType);
            logger.info("Изображение добавлено: {}", url);
        } else if (contentType != null && contentType.contains("text/html")) {
            Document document = response.parse();
            String text = extractText(document);
            Map<String, Integer> lemmaFrequencies = lemmatizeText(text);

            page.setContent(text);
            pageRepository.save(page);

            saveLemmasAndIndexes(lemmaFrequencies, page);

            logger.info("HTML-страница добавлена: {}", url);
            processLinks(document);
        } else {
            page.setContent("Unhandled content type: " + contentType);
            logger.info("Контент с неизвестным типом добавлен: {}", url);
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



    private void saveLemmasAndIndexes(Map<String, Integer> lemmaFrequencies, Page page) {
        int newLemmas = 0;
        int updatedLemmas = 0;
        int savedIndexes = 0;

        StringBuilder lemmaLog = new StringBuilder("Найденные леммы: ");

        for (Map.Entry<String, Integer> entry : lemmaFrequencies.entrySet()) {
            String lemmaText = entry.getKey();
            int rank = entry.getValue();

            lemmaLog.append(lemmaText).append(" (").append(rank).append("), ");

            // Получаем список совпадений
            List<Lemma> lemmas = lemmaRepository.findByLemmaAndSite(lemmaText, page.getSite());

            Lemma lemma;
            if (!lemmas.isEmpty()) { // Проверяем, есть ли в списке элементы
                lemma = lemmas.get(0); // Берем первую найденную лемму
                lemma.setFrequency(lemma.getFrequency() + 1);
                updatedLemmas++;
            } else {
                lemma = new Lemma();
                lemma.setLemma(lemmaText);
                lemma.setSite(page.getSite());
                lemma.setFrequency(1);
                lemmaRepository.save(lemma);
                newLemmas++;
            }

            Index index = new Index();
            index.setPage(page);
            index.setLemma(lemma);
            index.setRank((float) rank);
            indexRepository.save(index);
            savedIndexes++;
        }

        logger.info(lemmaLog.toString());

        logger.info("Страница '{}' обработана. Новых лемм: {}, Обновленных лемм: {}, Связок (индексов): {}",
                page.getPath(), newLemmas, updatedLemmas, savedIndexes);
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
                    subtasks.add(new PageCrawler(site, lemmaRepository, indexRepository, childUrl, visitedUrls, pageRepository, indexingService));
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
            processHtmlContent(page, document);
        } else {
            processUnknownContent(page, contentType);
        }
    }


    private void processImageContent(Page page, String contentType) {
        page.setContent("Image content: " + contentType);
        pageRepository.save(page);
        logger.info("Изображение добавлено: {}", url);
    }

    private void processHtmlContent(Page page, Document document) throws IOException {
        String text = extractText(document);
        Map<String, Integer> lemmaFrequencies = lemmatizeText(text);

        page.setContent(text);
        pageRepository.save(page);

        saveLemmasAndIndexes(lemmaFrequencies, page);
        logger.info("HTML-страница добавлена: {}", url);

        processLinks(document);
    }

    private void processUnknownContent(Page page, String contentType) {
        page.setContent("Unhandled content type: " + contentType);
        pageRepository.save(page);
        logger.info("Контент с неизвестным типом добавлен: {}", url);
    }

}
