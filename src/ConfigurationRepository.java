
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class ConfigurationRepository {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationRepository.class);

    @PersistenceContext
    private EntityManager em;

    @Inject
    @ConfigurationSchema
    private Instance<String> schema;

    @Inject
    @ConfigurationEnvironment
    private Instance<String> environment;

    private Map<String, String> cache = new ConcurrentHashMap<>();

    @PostConstruct
    void postConstruct() {
        logger.info("ConfigurationRepository initialized. Configuration schema is set to '{}', env prefix to '{}'", getSchema(), getEnv());
    }

    String find(String key) {
        String value = findInCache(key);
        if (value != null) {
            return value;
        }

        value = findInDatabase(key);

        saveOrUpdateToCache(key, value);
        return value;
    }

    void deleteFromCache(String key) {
        this.cache.remove(key);
    }

    Map<String, String> findAll() {
        Map<String, String> map = findAllInDatabase();
        map.entrySet().forEach(entry -> {
            String cachedValue = findInCache(entry.getKey());
            if (cachedValue != null) {
                entry.setValue(cachedValue);
            } else {
                saveOrUpdateToCache(entry.getKey(), entry.getValue());
            }
        });
        return map;
    }

    @Transactional(Transactional.TxType.REQUIRED)
    void saveOrUpdate(String key, String value) {
        saveOrUpdateToDatabase(key, value);
        saveOrUpdateToCache(key, value);
    }

    protected String getSchema() {
        if (null != schema) {
            return schema.get();
        }
        return "";
    }

    protected String getSchemaPrefix() {
        if (!StringUtils.isEmpty(getSchema())) {
            return getSchema() + ".";
        }
        return "";
    }

    protected String getEnv() {
        if (null != environment) {
            return environment.get();
        }
        return "";
    }

    protected String getEnvPrefix() {
        if (!StringUtils.isEmpty(getEnv())) {
            return getEnv().concat("_");
        }

        return null;
    }

    private String findInCache(String key) {
        return cache.get(key);
    }

    private String findInDatabase(String key) {
        Query query = em.createNativeQuery("select value from " + getSchemaPrefix() + "configuration conf where conf.key = :key");
        query.setParameter("key", key);
        try {
            String result = (String) query.getSingleResult();
            if (null != result) {
                return new StrSubstitutor(getEnvironmentEntities()).replace(result);
            }

            return result;
        } catch (NoResultException e) {
            logger.debug("No result found for key={}", key);
            return null;
        }
    }

    private Map<String, String> findAllInDatabase() {
        Query query = em.createNativeQuery("select key, value from " + getSchemaPrefix() + "configuration conf");
        List list = query.getResultList();
        Map<String, String> map = new ConcurrentHashMap<>(list.size());
        for (Object o : list) {
            Object[] arr = (Object[]) o;

            StrSubstitutor substitutor = new StrSubstitutor(getEnvironmentEntities());
            if (!StringUtils.isEmpty(arr[1].toString())) {
                map.put(arr[0].toString(), substitutor.replace(arr[1].toString()));
            } else {
                map.put(arr[0].toString(), arr[1].toString());
            }

        }
        return map;
    }

    private void saveOrUpdateToCache(String key, String value) {
        if (!StringUtils.isEmpty(key) && !StringUtils.isEmpty(value)) {
            this.cache.put(key, value);
        }
    }

    private void saveOrUpdateToDatabase(String key, String value) {
        String updateQuery = "update " + getSchemaPrefix() + "configuration set value=? where key=?";

        int updateCount = em.createNativeQuery(updateQuery)
                .setParameter(1, value)
                .setParameter(2, key)
                .executeUpdate();

        if (updateCount == 0) {
            String insertQuery = "insert into " + getSchemaPrefix() + "configuration values(?,?)";

            em.createNativeQuery(insertQuery)
                    .setParameter(1, key)
                    .setParameter(2, value)
                    .executeUpdate();
        }
    }

    private Map<String, String> environmentEntities = null;

    private Map<String, String> getEnvironmentEntities() {
        if (environmentEntities == null) {
            environmentEntities = new ConcurrentHashMap<>();
            if (!StringUtils.isEmpty(getEnvPrefix())) {
                for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                    if (entry.getKey().startsWith(getEnvPrefix())) {
                        environmentEntities.put(entry.getKey().substring(getEnvPrefix().length()), entry.getValue());
                    }
                }
            }
        }
        return environmentEntities;
    }
}
