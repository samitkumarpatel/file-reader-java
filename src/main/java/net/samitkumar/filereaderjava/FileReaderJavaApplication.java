package net.samitkumar.filereaderjava;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class FileReaderJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileReaderJavaApplication.class, args);
	}

	final ReactiveRedisTemplate<String, String> redisTemplate;

	@Value("${spring.application.file.upload.path}")
	private String fileUploadPath;

	@Bean
	Sinks.Many<String> sinks(){
		return Sinks.many().multicast().onBackpressureBuffer();
	}

	@Bean
	RouterFunction<ServerResponse> routerFunction() {
		return RouterFunctions
				.route()
				.POST("/message/to/channel", request -> {
					return request
							.bodyToMono(String.class)
							.flatMap(s -> redisTemplate.convertAndSend("channel", s))
							.map(l -> Map.of("status", "SUCCESS", "l", l))
							.flatMap(ServerResponse.ok()::bodyValue);
				})
				.POST("/file/upload", this::upload)
				.build();
	}

	@Bean
	public HandlerMapping handlerMapping(MyWebSocketHandler myWebSocketHandler) {
		Map<String, WebSocketHandler> map = new HashMap<>();
		map.put("/ws-java", myWebSocketHandler);
		int order = -1; // before annotated controllers
		return new SimpleUrlHandlerMapping(map, order);
	}

	private Mono<ServerResponse> upload(ServerRequest request) {
		return request
				.multipartData()
				.map(MultiValueMap::toSingleValueMap)
				.map(stringPartMap -> stringPartMap.get("file"))
				.cast(FilePart.class)
				.flatMap(this::fileUploadToDisk)
				.flatMap(filePart -> redisTemplate.convertAndSend("channel", Path.of(fileUploadPath).resolve(filePart.filename()).toString()))
				.then(ServerResponse.ok().bodyValue(Map.of("status", "SUCCESS")))
				.onErrorResume(ex -> ServerResponse
					.status(HttpStatus.INTERNAL_SERVER_ERROR)
						.bodyValue(Map.of("status", "ERROR", "message", ex.getMessage()))
		);
	}

	private Mono<FilePart> fileUploadToDisk(FilePart filePart) {
		return filePart.transferTo(Path.of(fileUploadPath).resolve(filePart.filename()))
				.thenReturn(filePart);
	}

	@EventListener
	public void onApplicationEvent(ApplicationReadyEvent event) {
		/*redisTemplate.listenToChannel("channel")
				.doOnNext(message -> log.info("[*] Received Message: {}", message))
				.doOnNext(message -> sinks().tryEmitNext("Got the file Information, Processing It..."))
				.flatMap(message -> processFile(message.getMessage()).map(fileReaderDetails -> sinks().tryEmitNext(fileReaderDetails)))
				.subscribe();*/

		redisTemplate
				.listenToChannel("channel")
				.doOnNext(processedMessage -> log.info("[*] Received Message: {}", processedMessage))
				.doOnNext(processedMessage -> sinks().tryEmitNext("Got the file Information, Processing It..."))
				.doOnNext(processedMessage -> processFile(processedMessage.getMessage()))
				.subscribeOn(Schedulers.parallel(), true)
				.subscribe();
	}

	@SneakyThrows
	private void processFile(String filePath) {
		int lines = 0, words = 0, letters = 0;

		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				lines++;
				words += line.split("\\s+").length;
				letters += line.replaceAll("\\s", "").length();
			}
		} catch (IOException e) {
			log.error("Error reading file", e);
		}
		log.info("Read {} words , {} letters and {} lines from file {}", words, letters, lines, filePath);

		var x = new FileReaderDetails(lines, words, letters);
		var result = Map
				.of(
						"lines", x.lines(),
						"words", x.words(),
						"letters", x.letters()
				).toString();
		//Thread.sleep(2000);
		sinks().tryEmitNext(result);
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