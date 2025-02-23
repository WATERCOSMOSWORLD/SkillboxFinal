package searchengine;

import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;

import java.io.IOException;
import java.util.*;


public class Lemmatizer {
    private static final Set<String> EXCLUDED_PARTS_OF_SPEECH = new HashSet<>(Arrays.asList(
            "PREP", "CONJ", "PRCL", "INTJ" // Предлоги, союзы, частицы, междометия
    ));

    private LuceneMorphology luceneMorphology;

    public Lemmatizer(String language) {
        try {
            // Выбираем лемматизатор в зависимости от языка
            if ("ru".equalsIgnoreCase(language)) {
                luceneMorphology = new RussianLuceneMorphology();
            } else if ("en".equalsIgnoreCase(language)) {
                luceneMorphology = new EnglishLuceneMorphology();
            } else {
                throw new IllegalArgumentException("Unsupported language: " + language);
            }
        } catch (IOException e) {
            // Обрабатываем исключения, связанные с загрузкой данных лемматизатора
            System.err.println("Error initializing lemmatizer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public Map<String, Integer> getLemmas(String text) {
        // Очищаем текст от всех небуквенных символов
        text = text.replaceAll("[^a-zA-Zа-яА-ЯёЁ]", " "); // Заменяем все не-буквенные символы на пробелы
        String[] words = text.split("\\s+"); // Разбиваем текст на слова
        Map<String, Integer> lemmaCount = new HashMap<>();

        for (String word : words) {
            if (word.isEmpty()) {
                continue; // Пропускаем пустые строки
            }

            // Приводим слово к нижнему регистру
            word = word.toLowerCase();

            // Лемматизация слова
            List<String> lemmas = luceneMorphology.getNormalForms(word);
            if (!lemmas.isEmpty()) {
                String lemma = lemmas.get(0); // Берем первую лемму (основную)

                // Получаем часть речи
                List<String> grammemes = luceneMorphology.getMorphInfo(word);
                for (String grammeme : grammemes) {
                    // Если это не исключение, добавляем лемму в карту
                    if (!isExcludedPartOfSpeech(grammeme)) {
                        lemmaCount.put(lemma, lemmaCount.getOrDefault(lemma, 0) + 1);
                    }
                }
            }
        }

        return lemmaCount;
    }

    private boolean isExcludedPartOfSpeech(String grammeme) {
        // Проверяем, является ли часть речи исключением
        for (String partOfSpeech : EXCLUDED_PARTS_OF_SPEECH) {
            if (grammeme.contains(partOfSpeech)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        // Тексты для обработки
        String russianText = "Повторное появление леопарда в Осетии позволяет предположить, что леопард постоянно обитает в некоторых районах Северного Кавказа.";
        String englishText = "The repeated appearance of the leopard in Ossetia suggests that the leopard constantly lives in some areas of the North Caucasus.";

        // Для русского текста
        Lemmatizer ruLemmatizer = new Lemmatizer("ru");
        Map<String, Integer> ruResult = ruLemmatizer.getLemmas(russianText);
        System.out.println("Russian Lemmas:");
        for (Map.Entry<String, Integer> entry : ruResult.entrySet()) {
            System.out.println(entry.getKey() + " — " + entry.getValue());
        }

        // Для английского текста
        Lemmatizer enLemmatizer = new Lemmatizer("en");
        Map<String, Integer> enResult = enLemmatizer.getLemmas(englishText);
        System.out.println("\nEnglish Lemmas:");
        for (Map.Entry<String, Integer> entry : enResult.entrySet()) {
            System.out.println(entry.getKey() + " — " + entry.getValue());
        }
    }
}
