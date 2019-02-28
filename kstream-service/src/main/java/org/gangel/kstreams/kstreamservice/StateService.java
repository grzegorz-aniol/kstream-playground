package org.gangel.kstreams.kstreamservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * Cache of recent devices indication
 *
 * Instead of working directly with Hazelcast instance API this bean uses standard Spring Cache annotations
 */
@Service
@CacheConfig(cacheNames="states")
public class StateService {

  private Logger log = LoggerFactory.getLogger(StateService.class);

  @Cacheable
  public Boolean getState(String key) {
    log.debug("CACHE: reading not cached status for key={}", key);
    return null; // return null as default, non-cached value
  }

  @CachePut(key="#key")
  public Boolean updateState(String key, Boolean value) {
    // this method do not produce new value in fact,
    // it just put argument 'value' into the cache by returning it
    log.debug("CACHE: updating status, key={}, value={}", key, value);
    return value;
  }

}
