package net.samitkumar.filereaderjava;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class FileReaderJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileReaderJavaApplication.class, args);
	}

	final ReactiveRedisTemplate<String, String> redisTemplate;

	@Bean
	RouterFunction<ServerResponse> routerFunction() {
		return RouterFunctions
				.route()
				.GET("/details", this::processFile)
				.POST("/message", request -> {
					return request
							.bodyToMono(String.class)
							.flatMap(s -> redisTemplate.convertAndSend("channel", s))
							.map(l -> Map.of("status", "SUCCESS", "l", l))
							.flatMap(ServerResponse.ok()::bodyValue);
				})
				.build();
	}

	@EventListener
	public void onApplicationEvent(ApplicationReadyEvent event) {
		redisTemplate.listenToChannel("channel")
				.doOnNext(message -> log.info("[*] Received Message: {}", message))
				.subscribe();
	}

	@SneakyThrows
	private Mono<ServerResponse> processFile(ServerRequest request) {
		String filename = "sample.txt";
		int lines = 0, words = 0, letters = 0;

		try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
			String line;
			while ((line = reader.readLine()) != null) {
				lines++;
				words += line.split("\\s+").length;
				letters += line.replaceAll("\\s", "").length();
			}
		} catch (IOException e) {
			log.error("Error reading file", e);
		}
		log.info("Read {} words , {} letters and {} lines from file {}", words, letters, lines, filename);

		var x = new FileReaderDetails(lines, words, letters);
		return Mono.fromCallable(() -> Map.of(
						"lines", x.lines(),
						"words", x.words(),
						"letters", x.letters()
						)
				)
				.flatMap(ServerResponse.ok()::bodyValue);
	}

}

record FileReaderDetails(int lines, int words, int letters){}

