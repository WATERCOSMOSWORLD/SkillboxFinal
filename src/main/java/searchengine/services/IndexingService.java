package searchengine.services;

import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import org.springframework.transaction.annotation.Propagation;
import searchengine.config.SitesList;
import searchengine.model.*;
import searchengine.repository.PageRepository;
import searchengine.repository.SiteRepository;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import searchengine.repository.LemmaRepository;
import searchengine.repository.IndexRepository;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Lazy;

@Lazy
@Service

public class IndexingService {

    private static final Logger logger = LoggerFactory.getLogger(IndexingService.class);
    private final ConcurrentHashMap<String, Boolean> indexingTasks = new ConcurrentHashMap<>();
    private final SitesList sitesList;
    private final SiteRepository siteRepository;
    private final PageRepository pageRepository;
    private final LemmaRepository lemmaRepository;
    private final IndexRepository indexRepository;

    private volatile boolean indexingInProgress = false;
    private ExecutorService executorService;
    private ForkJoinPool forkJoinPool;

    public IndexingService(SitesList sitesList,LemmaRepository lemmaRepository, IndexRepository indexRepository, SiteRepository siteRepository,  PageRepository pageRepository ) {
        this.sitesList = sitesList;
        this.siteRepository = siteRepository;
        this.pageRepository = pageRepository;
        this.indexRepository = indexRepository;
        this.lemmaRepository = lemmaRepository;

    }

    public synchronized boolean isIndexingInProgress() {
        return indexingInProgress;
    }

    public synchronized void startFullIndexing() {
        if (indexingInProgress) {
            logger.warn("Попытка запустить индексацию, которая уже выполняется.");
            throw new IllegalStateException("Индексация уже запущена.");
        }
        indexingInProgress = true;
        logger.info("Индексация начата.");

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                performIndexing();
            } catch (Exception e) {
                logger.error("Ошибка во время индексации: ", e);
            } finally {
                indexingInProgress = false;
                logger.info("Индексация завершена.");
            }
        });
        executorService.shutdown();
    }

    public synchronized void stopIndexing() {
        if (!indexingInProgress) {
            logger.warn("Попытка остановить индексацию, которая не выполняется.");
            return;
        }
        logger.info("Остановка индексации по запросу пользователя.");
        indexingInProgress = false;

        if (executorService != null) {
            executorService.shutdownNow();
        }
        if (forkJoinPool != null) {
            forkJoinPool.shutdownNow();
        }

        updateSitesStatusToFailed("Индексация остановлена пользователем");
    }


    private void performIndexing() {
        List<searchengine.config.ConfigSite> sites = sitesList.getSites();
        if (sites == null || sites.isEmpty()) {
            logger.warn("Список сайтов для индексации пуст.");
            return;
        }

        executorService = Executors.newFixedThreadPool(sites.size());
        try {
            for (searchengine.config.ConfigSite site : sites) {
                // Ensure we only index sites from the configured list
                if (isConfiguredSite(site.getUrl())) {
                    executorService.submit(() -> {
                        logger.info("Индексация сайта: {} ({})", site.getName(), site.getUrl());
                        startIndexingForSite(site.getUrl()); // Add the site to active tasks
                        try {
                            deleteSiteData(site.getUrl());
                            searchengine.model.Site newSite = new searchengine.model.Site();
                            newSite.setName(site.getName());
                            newSite.setUrl(site.getUrl());
                            newSite.setStatus(IndexingStatus.INDEXING);
                            newSite.setStatusTime(LocalDateTime.now());
                            siteRepository.save(newSite);

                            // Crawl and index the pages for the site
                            // Make sure the SitesList instance is available
                            SitesList sitesList = getSitesList();  // Replace with actual way to get SitesList


                            PageCrawler.startCrawling(
                                    newSite,                      // The new site to index
                                    site.getUrl(),                 // URL to start from
                                    lemmaRepository,               // LemmaRepository object
                                    siteRepository,                // SiteRepository object
                                    indexRepository,               // IndexRepository object
                                    pageRepository,                // PageRepository object
                                    this,                          // The current instance of IndexingService
                                    sitesList                      // Pass SitesList as needed
                            );

                            if (indexingInProgress) {
                                updateSiteStatusToIndexed(newSite);
                            } else {
                                logger.warn("Индексация была прервана. Статус сайта {} не обновлен на INDEXED.", site.getName());
                            }
                        } catch (Exception e) {
                            handleIndexingError(site.getUrl(), e);
                        } finally {
                            stopIndexingForSite(site.getUrl()); // Remove the site from active tasks
                        }
                    });
                } else {
                    logger.warn("Сайт {} не найден в конфигурации для индексации, пропускаем.", site.getUrl());
                }
            }
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                    executorService.shutdownNow();
                    logger.error("Превышено время ожидания завершения индексации.");
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                logger.error("Индексация была прервана: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    public SitesList getSitesList() {
        return this.sitesList;
    }


    private boolean isConfiguredSite(String url) {
        List<searchengine.config.ConfigSite> sites = sitesList.getSites();
        for (searchengine.config.ConfigSite site : sites) {
            if (site.getUrl().equals(url)) {
                return true;
            }
        }
        return false;
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

    private void updateSiteStatusToIndexed(searchengine.model.Site site) {
        if (site.getStatus() == IndexingStatus.FAILED) {
            logger.warn("Сайт {} имеет ошибку, статус INDEXED не устанавливается.", site.getUrl());
            return;
        }

        site.setStatus(IndexingStatus.INDEXED);
        site.setStatusTime(LocalDateTime.now());
        siteRepository.save(site);
        logger.info("Сайт {} изменил статус на INDEXED.", site.getUrl());
    }


    private void handleIndexingError(String siteUrl, Exception e) {
        searchengine.model.Site site = siteRepository.findByUrl(siteUrl);
        if (site != null) {
            site.setStatus(IndexingStatus.FAILED);
            site.setLastError(e.getMessage());
            site.setStatusTime(LocalDateTime.now());
            siteRepository.save(site);
            logger.error("Ошибка при индексации сайта {}: {}", site.getUrl(), e.getMessage());
        }
    }

    private void updateSitesStatusToFailed(String errorMessage) {
        List<searchengine.model.Site> sites = siteRepository.findAllByStatus(IndexingStatus.INDEXING);
        for (searchengine.model.Site site : sites) {
            site.setStatus(IndexingStatus.FAILED);
            site.setLastError(errorMessage);
            site.setStatusTime(LocalDateTime.now());
            siteRepository.save(site);
            logger.info("Сайт {} изменил статус на FAILED: {}", site.getUrl(), errorMessage);
        }
    }

    private void startIndexingForSite(String url) {
        if (isConfiguredSite(url)) {
            indexingTasks.put(url, true);
            logger.info("Индексация начата для сайта: {}", url);
        } else {
            logger.warn("Сайт {} не найден в конфигурации, пропускаем индексацию.", url);
        }
    }


    private void stopIndexingForSite(String url) {
        indexingTasks.remove(url);
    }

    public boolean isSiteIndexing(String url) {
        return indexingTasks.getOrDefault(url, false);
    }

    public boolean checkAndUpdateStatus(String message) {
        if (!indexingInProgress) {
            logger.warn("Индексация остановлена: {}", message);
            return false;
        }
        return true;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processPageContent(Page page) {
        try {
            String text = extractTextFromHtml(page.getContent());
            Map<String, Integer> lemmas = lemmatizeText(text);

            // Используем Map, чтобы избежать повторных запросов в БД
            Map<String, Lemma> lemmaCache = new HashMap<>();
            List<Lemma> lemmasToSave = new ArrayList<>();
            List<Index> indexesToSave = new ArrayList<>();

            for (Map.Entry<String, Integer> entry : lemmas.entrySet()) {
                String lemmaText = entry.getKey();
                int count = entry.getValue();

                // Проверяем, есть ли лемма в кэше (из базы или уже добавленная в текущем процессе)
                Lemma lemma = lemmaCache.get(lemmaText);
                if (lemma != null) {
                    lemma.setFrequency(lemma.getFrequency() + count);
                    continue;
                }

                // Ищем лемму в базе данных
                Optional<Lemma> existingLemmaOpt = lemmaRepository.findByLemmaAndSite(lemmaText, page.getSite());
                if (existingLemmaOpt.isPresent()) {
                    lemma = existingLemmaOpt.get();
                    lemma.setFrequency(lemma.getFrequency() + count);
                } else {
                    // Создаём новую лемму
                    lemma = new Lemma(null, page.getSite(), lemmaText, count);
                    lemmasToSave.add(lemma);
                }
                lemmaCache.put(lemmaText, lemma);
            }

            // Добавляем индексы
            for (Map.Entry<String, Integer> entry : lemmas.entrySet()) {
                Lemma lemma = lemmaCache.get(entry.getKey());
                indexesToSave.add(new Index(null, page, lemma, (float) entry.getValue()));
            }

            // Сохраняем обновленные или новые леммы и индексы
            if (!lemmasToSave.isEmpty()) {
                lemmaRepository.saveAll(lemmasToSave);
            }
            if (!indexesToSave.isEmpty()) {
                indexRepository.saveAll(indexesToSave);
            }
        } catch (IOException e) {
            logger.error("❌ Ошибка при обработке страницы: {}", page.getPath(), e);
        } catch (Exception e) {
            logger.error("Ошибка при обработке лемм и индексов: {}", e.getMessage(), e);
        }
    }


    private String extractTextFromHtml(String html) {
        return Jsoup.parse(html).text();
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
}
