package net.samitkumar.filereaderjava;

import lombok.SneakyThrows;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
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

@SpringBootApplication
public class FileReaderJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileReaderJavaApplication.class, args);
	}

	@Bean
	RouterFunction<ServerResponse> routerFunction() {
		return RouterFunctions
				.route()
				.GET("/details", this::processFile)
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
			e.printStackTrace();
		}

		System.out.println("Lines: " + lines);
		System.out.println("Words: " + words);
		System.out.println("Letters: " + letters);

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

@RabbitListener(queues = "hello")
@Component
class RabbitMqReceiver {

	@RabbitHandler
	public void receive(String in) {
		System.out.println(" [x] Received '" + in + "'");
	}
}
