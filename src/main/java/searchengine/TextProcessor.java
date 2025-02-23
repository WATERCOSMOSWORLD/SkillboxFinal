package searchengine;

import org.apache.lucene.morphology.LuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextProcessor {

    // Инициализация морфологических анализаторов
    private static LuceneMorphology russianMorph;
    private static LuceneMorphology englishMorph;

    static {
        try {
            // Инициализация морфологических анализаторов
            russianMorph = new RussianLuceneMorphology();
            englishMorph = new EnglishLuceneMorphology();
        } catch (IOException e) {
            // Обработка исключения, если не удалось загрузить морфологию
            System.err.println("Ошибка при инициализации морфологического анализатора: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Метод для очистки текста от служебных частей речи и подсчета лемм
    public static HashMap<String, Integer> processText(String text, String language) {
        HashMap<String, Integer> lemmaCount = new HashMap<>();

        // Убираем HTML-теги из текста
        text = removeHtmlTags(text);

        // Разделяем текст на слова по пробелам
        String[] words = text.split("\\s+");

        // Выбираем морфологический анализатор в зависимости от языка
        LuceneMorphology luceneMorph = getMorphology(language);

        if (luceneMorph == null) {
            return lemmaCount; // Если морфология не была инициализирована, возвращаем пустой результат
        }

        // Регулярное выражение для удаления знаков препинания
        Pattern pattern = Pattern.compile("[^a-zA-Zа-яА-ЯёЁ]");

        for (String word : words) {
            // Убираем знаки препинания из слов
            Matcher matcher = pattern.matcher(word);
            word = matcher.replaceAll("");

            // Приводим слово к нижнему регистру
            word = word.toLowerCase();

            // Пропускаем слова с апострофами в английском языке (например, it's, I'm)
            if (language.equals("en") && word.contains("'")) {
                continue;
            }

            try {
                // Получаем информацию о морфологии слова
                List<String> wordBaseForms = luceneMorph.getMorphInfo(word);

                // Проверяем, что слово не является служебной частью речи
                if (wordBaseForms.stream().noneMatch(info -> info.contains("СОЮЗ") ||
                        info.contains("МЕЖД") ||
                        info.contains("ПРЕДЛ") ||
                        info.contains("ЧАСТ") ||
                        info.contains("CONJ") ||  // Для английского языка
                        info.contains("PART"))) { // Для английского языка

                    // Получаем лемму (основную форму слова)
                    String lemma = luceneMorph.getNormalForms(word).get(0);

                    // Добавляем лемму в HashMap с подсчетом количества
                    lemmaCount.put(lemma, lemmaCount.getOrDefault(lemma, 0) + 1);
                }
            } catch (Exception e) {
                // Логируем ошибку, если не удалось обработать слово
                System.err.println("Ошибка при обработке слова: " + word);
                e.printStackTrace();
            }
        }
        return lemmaCount;
    }

    // Метод для удаления HTML-тегов
    public static String removeHtmlTags(String text) {
        // Используем библиотеку Jsoup для очистки HTML
        return Jsoup.parse(text).text();
    }

    // Метод для выбора морфологического анализатора в зависимости от языка
    public static LuceneMorphology getMorphology(String language) {
        switch (language.toLowerCase()) {
            case "ru":
                return russianMorph;
            case "en":
                return englishMorph;
            default:
                throw new IllegalArgumentException("Неизвестный язык: " + language);
        }
    }

    // Пример использования
    public static void main(String[] args) {
        // Пример текста с HTML-тегами для русского языка
        String htmlTextRu = "<html><body>Я люблю программировать! И <b>это</b> интересно. И это круто.</body></html>";
        System.out.println("Очищенный русский текст: " + removeHtmlTags(htmlTextRu));

        // Пример текста для обработки морфологии на русском языке
        String textRu = "Я люблю программировать и создавать приложения, и это интересно.";
        HashMap<String, Integer> lemmaCountsRu = processText(textRu, "ru");
        System.out.println("Леммы и их количество для русского текста: " + lemmaCountsRu);

        // Пример текста с HTML-тегами для английского языка
        String htmlTextEn = "<html><body>I love programming! And <b>this</b> is interesting. And it's cool.</body></html>";
        System.out.println("Очищенный английский текст: " + removeHtmlTags(htmlTextEn));

        // Пример текста для обработки морфологии на английском языке
        String textEn = "I love programming and creating applications, and it's interesting.";
        HashMap<String, Integer> lemmaCountsEn = processText(textEn, "en");
        System.out.println("Леммы и их количество для английского текста: " + lemmaCountsEn);
    }
}
