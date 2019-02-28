package org.gangel.kstreams.kstreamservice;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@EnableCaching
public class CacheConfiguration {

  @Bean(destroyMethod = "shutdown")
  public HazelcastInstance createStorageNode(Config config) throws Exception {
    return Hazelcast.newHazelcastInstance(config);
  }

  @Bean
  public CacheManager cacheManager(HazelcastInstance instance) {
    return new com.hazelcast.spring.cache.HazelcastCacheManager(instance);
  }

  @Bean
  public Config hazelCastConfig(){
    Config config = new Config();
    config.setInstanceName("hazelcast-instance")
        .addMapConfig(
            new MapConfig()
                .setName("states")
                .setMaxSizeConfig(new MaxSizeConfig(200, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE))
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setTimeToLiveSeconds(3600));
    return config;
  }

}
