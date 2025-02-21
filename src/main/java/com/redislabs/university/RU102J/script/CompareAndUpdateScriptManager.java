package com.redislabs.university.RU102J.script;

import com.redislabs.university.RU102J.exceptions.ScriptReadingException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/* Encapsulates a server-side Lua script to compare
 * a value stored in a hash field and update if
 * greater than or less than the provided value,
 * as requested.
 */
public class CompareAndUpdateScriptManager {

    private final String sha;
    private final static String COMPARE_AND_UPDATE_SCRIPT_PATH = "./src/main/resources/lua/compareAndUpdateScript.lua";

    public CompareAndUpdateScriptManager(JedisPool jedisPool) throws ScriptReadingException {
        try (Jedis jedis = jedisPool.getResource()) {
            String script = this.readLuaFile();
            this.sha = jedis.scriptLoad(script);
        }
        catch (IOException e) {
            throw new ScriptReadingException("Could not find resource:", e);
        }
    }

    private String readLuaFile() throws IOException {
        // Read all bytes and convert to string
        byte[] bytes = Files.readAllBytes(Paths.get(CompareAndUpdateScriptManager.COMPARE_AND_UPDATE_SCRIPT_PATH));
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public void updateIfGreater(Transaction jedis, String key, String field,
                               Double value) {
        update(jedis, key, field, value, ScriptOperation.GREATERTHAN);
    }

    public void updateIfLess(Transaction jedis, String key, String field,
                               Double value) {
        update(jedis, key, field, value, ScriptOperation.LESSTHAN);
    }

    private void update(Transaction jedis, String key, String field, Double value,
                           ScriptOperation op) {
        if (sha != null) {
            List<String> keys = Collections.singletonList(key);
            List<String> args = Arrays.asList(field, String.valueOf(value),
                    op.getSymbol());
            jedis.evalsha(sha, keys, args);
        }
    }
}
