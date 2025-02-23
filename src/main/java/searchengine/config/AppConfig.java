package searchengine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class AppConfig {

    // Создаем бин ExecutorService
    @Bean
    public ExecutorService executorService() {
        // Создаем пул потоков с заданным количеством потоков
        return Executors.newFixedThreadPool(4); // Используем 4 потока в пуле, можете настроить по вашему усмотрению
    }
}