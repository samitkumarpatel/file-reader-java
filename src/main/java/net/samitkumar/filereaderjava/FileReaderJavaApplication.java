package net.samitkumar.filereaderjava;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class FileReaderJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileReaderJavaApplication.class, args);
	}

	final RabbitTemplate template;

	@Bean
	RouterFunction<ServerResponse> routerFunction() {
		return RouterFunctions
				.route()
				.GET("/details", this::processFile)
				.POST("/message", request -> {
					return request
							.bodyToMono(String.class)
							.doOnNext(s -> log.info("Message received: {}", s))
							.mapNotNull(message -> template.convertSendAndReceive("file-reader", "", message))
							.flatMap(ServerResponse.ok()::bodyValue);
				})
				.build();
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

@RabbitListener(queues = "file-reader-java")
@Component
class RabbitMqReceiver {

	@RabbitHandler
	public void receive(String in) {
		System.out.println(" [x] Received '" + in + "'");
	}
}
