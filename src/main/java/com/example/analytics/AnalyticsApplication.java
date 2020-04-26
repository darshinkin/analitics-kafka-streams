package com.example.analytics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class AnalyticsApplication {

    @Component
    public static class PageViewEventSource implements ApplicationRunner {

        private final MessageChannel pageViewOut;

        public PageViewEventSource(AnalyticsBinding binding) {
            this.pageViewOut = binding.pageViewsOut();
        }

        @Override
        public void run(ApplicationArguments args) throws Exception {
            List<String> names = Arrays.asList("User: Developer", "User: analist", "DevOps", "Tester");
            List<String> pages = Arrays.asList("blog", "sitemap", "about");
            Runnable runnable = () -> {
                String rPage = pages.get(new Random().nextInt(pages.size()));
                String rName = names.get(new Random().nextInt(names.size()));
                PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);
                Message<PageViewEvent> message = MessageBuilder
                        .withPayload(pageViewEvent)
                        .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
                        .build();

                try {
                    this.pageViewOut.send(message);
                    log.info("sent " + message.toString());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            };
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
        }
    }

    @Component
    public static class PageViewEventProcessor {

        @StreamListener
        @SendTo(AnalyticsBinding.PAGE_COUNT_OUT)
        public KStream<String, Long> process(@Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, PageViewEvent> events) {
            return events
                    .filter((key, pageViewEvent) -> pageViewEvent.getDuration() > 10)
                    .map((key, pageViewEvent) -> new KeyValue<>(pageViewEvent.getPage(), "0"))
                    .groupByKey()
                    .count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV))
                    .toStream();
        }
    }

    @Component
    public static class PageCountSink {

        @StreamListener
        public void process(@Input(AnalyticsBinding.PAGE_COUNT_IN) KTable<String, Long> counts) {
            counts
                    .toStream()
                    .foreach((key, value) -> log.info(key + "=" + value));
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}

interface AnalyticsBinding {

    String PAGE_VIEWS_OUT = "pvout";
    String PAGE_VIEWS_IN = "pvin";
    String PAGE_COUNT_MV = "pcmv";
    String PAGE_COUNT_OUT = "pcout";
    String PAGE_COUNT_IN = "pcin";

    // page views
    @Input(PAGE_VIEWS_IN)
    KStream<String, PageViewEvent> pageViewIn();

    @Output(PAGE_VIEWS_OUT)
    MessageChannel pageViewsOut();

    // page counts
    @Output(PAGE_COUNT_OUT)
    KStream<String, Long> pageCountOUt();

    @Input(PAGE_COUNT_IN)
    KTable<String, Long> pageCountIn();

}

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
    private String userId, page;
    private long duration;
}
