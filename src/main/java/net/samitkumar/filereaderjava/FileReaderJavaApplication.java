package net.samitkumar.filereaderjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class FileReaderJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileReaderJavaApplication.class, args);
	}

	final ReactiveRedisTemplate<String, String> redisTemplate;
	final ObjectMapper objectMapper;
	@Value("${spring.application.file.lookup.path}")
	private String fileLookUpPath;

	@Bean
	Sinks.Many<String> sinks(){
		return Sinks.many().multicast().onBackpressureBuffer();
	}

	@Bean
	RouterFunction<ServerResponse> routerFunction() {
		return RouterFunctions
				.route()
				.GET("/ping", request -> ServerResponse.ok().bodyValue(Map.of("status", "UP")))
				.GET("/details", this::fileReader)
				.build();
	}

	private Mono<ServerResponse> fileReader(ServerRequest request) {
		var fileName = request.queryParam("filename").orElseThrow();
		return ServerResponse.ok().bodyValue(processFile(fileName));
	}

	@Bean
	public HandlerMapping handlerMapping(MyWebSocketHandler myWebSocketHandler) {
		Map<String, WebSocketHandler> map = new HashMap<>();
		map.put("/ws-java", myWebSocketHandler);
		int order = -1; // before annotated controllers
		return new SimpleUrlHandlerMapping(map, order);
	}

	@EventListener
	public void onApplicationEvent(ApplicationReadyEvent event) {
		redisTemplate
				.listenToChannel("channel")
				.doOnNext(processedMessage -> log.info("[*] Received Message: {}", processedMessage))
				.doOnNext(processedMessage -> Mono.fromRunnable(() -> sinks().tryEmitNext("Got the file Information, Processing It...")).subscribeOn(Schedulers.parallel()).subscribe() )
				.doOnNext(processedMessage -> processFileAndEmit(processedMessage.getMessage()))
				.subscribe();
	}

	@SneakyThrows
	private void processFileAndEmit(String fileName) {
		var fileReaderDetails = processFile(fileName);
		var result = objectMapper.writeValueAsString(fileReaderDetails);
		//Thread.sleep(2000);
		sinks().tryEmitNext(result);
	}

	public FileReaderDetails processFile(String fileName) {
		int lines = 0, words = 0, letters = 0;

		try (Stream<String> stream = Files.lines(Paths.get(fileLookUpPath, fileName), StandardCharsets.UTF_8)) {
			for (String line : (Iterable<String>) stream::iterator) {
				lines++;
				words += line.split("\\s+").length;
				letters += line.replaceAll("\\s", "").length();
			}
		} catch (IOException e) {
			log.error("Error reading file", e);
			sinks().tryEmitNext("Error reading file");
		}
		log.info("Read {} words, {} letters, and {} lines from file {}", words, letters, lines, fileName);
		return new FileReaderDetails(lines, words, letters);
	}
}

record FileReaderDetails(int lines, int words, int letters){}

@RequiredArgsConstructor
@Slf4j
@Component
class MyWebSocketHandler implements WebSocketHandler {

	final Sinks.Many<String> sinks;

	@Override
	public Mono<Void> handle(WebSocketSession session) {
		return session
				.send(sinks.asFlux().map(session::textMessage))
				.and(session.receive().map(webSocketMessage -> sinks.tryEmitNext(webSocketMessage.getPayloadAsText())))
				.then();
	}
}