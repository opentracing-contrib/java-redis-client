package io.opentracing.contrib.redis.spring.processor;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RedisConnectionFactoryBeanPostProcessorConfiguration.class)
@EnableAutoConfiguration
public class RedisConnectionFactoryBeanPostProcessorTest {

  @Autowired
  private RedisConnectionFactoryBeanPostProcessor redisConnectionFactoryBeanPostProcessor;

  @Autowired
  private RedisConnectionFactory redisConnectionFactory;

  @Test
  public void test() {
    assertNotNull(redisConnectionFactory);

  }
}
