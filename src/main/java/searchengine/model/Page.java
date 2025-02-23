package searchengine.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

@Entity
@Table(
        name = "page",
        indexes = {@jakarta.persistence.Index(name = "idx_path", columnList = "path")}
)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Page {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id; // Изменено с int на Integer для поддержки null

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "site_id", nullable = false)
    private Site site;

    @Column(length = 500, nullable = false) // Increase length to accommodate longer paths
    private String path;


    @Column(nullable = false)
    private int code;

    @Column(columnDefinition = "MEDIUMTEXT", nullable = false)
    private String content;

    @Column(length = 500)
    private String contentType; // Столбец для хранения типа содержимого (например, "image/png")

    // Дополнительное поле для хранения заголовка страницы
    @Transient
    private String title;

    // Дополнительное поле для хранения текста страницы
    @Transient
    private String text;



    // Метод для извлечения текста из HTML контента
    public String getText() {
        if (content != null) {
            Document doc = Jsoup.parse(content);
            return doc.text(); // Извлекаем текст из HTML (без тегов)
        }
        return null;
    }
}
