package io.opentracing.contrib.redis.spring.processor;

import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan("io.opentracing.contrib.redis.spring.processor")
public class RedisConnectionFactoryBeanPostProcessorConfiguration {
  @Bean
  public Tracer tracer() {
    return new MockTracer();
  }
}
