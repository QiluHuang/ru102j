package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Coordinate;
import com.redislabs.university.RU102J.api.GeoQuery;
import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.*;

import java.util.*;
import java.util.stream.Collectors;

public class SiteGeoDaoRedisImpl implements SiteGeoDao {
    private JedisPool jedisPool;
    final static private Double capacityThreshold = 0.2;

    public SiteGeoDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Site findById(long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields =
                    jedis.hgetAll(RedisSchema.getSiteHashKey(id));
            if (fields == null || fields.isEmpty()) {
                return null;
            }
            return new Site(fields);
        }
    }

    @Override
    public Set<Site> findAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.zrange(RedisSchema.getSiteGeoKey(), 0, -1);
            List<Response<Map<String, String>>> sitesResponseList = new ArrayList<>();

            // Collect all response using pipeline
            Pipeline pipeline = jedis.pipelined();
            for (String key : keys) {
                sitesResponseList.add(pipeline.hgetAll(key));
            }
            pipeline.sync();

            // Filter and transform responses to Sites
            return sitesResponseList.stream()
                    .map(Response::get)             // Get the Map<String, String> from Response
                    .filter(map -> !map.isEmpty())  // Filter out empty maps
                    .map(Site::new)                 // Transform map to Site object
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public Set<Site> findByGeo(GeoQuery query) {
        if (query.onlyExcessCapacity()) {
            return findSitesByGeoWithCapacity(query);
        } else {
            return findSitesByGeo(query);
        }
    }

    // Challenge #5
//     private Set<Site> findSitesByGeoWithCapacity(GeoQuery query) {
//         return Collections.emptySet();
//     }
    // Comment out the above, and uncomment what's below
    private Set<Site> findSitesByGeoWithCapacity(GeoQuery query) {
        Set<Site> results = new HashSet<>();
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

         try (Jedis jedis = jedisPool.getResource()) {
             // Get the sites matching the geo query, store them in a list
             List<GeoRadiusResponse> radiusResponses =
                     jedis.georadius(RedisSchema.getSiteGeoKey(), coord.getLng(),
                             coord.getLat(), radius, radiusUnit);

             Set<Site> sites = radiusResponses.stream()
                     .map(response -> jedis.hgetAll(response.getMemberByString()))
                     .filter(Objects::nonNull)
                     .map(Site::new).collect(Collectors.toSet());

             // Get the score of a site using Redis pipeline
             Pipeline pipeline = jedis.pipelined();
             Map<Long, Response<Double>> scores = sites.stream()
                     .collect(Collectors.toMap(
                             Site::getId,
                             site -> pipeline.zscore(
                                     RedisSchema.getCapacityRankingKey(),
                                     String.valueOf(site.getId())
                             )
                     ));
             pipeline.sync();

             for (Site site : sites) {
                 if (scores.get(site.getId()).get() >= capacityThreshold) {
                     results.add(site);
                 }
             }
         }

         return results;
    }

    private Set<Site> findSitesByGeo(GeoQuery query) {
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

        // TODO: fix the issue that if the user put an "int-val" string which is converted to Double but failed the search
        try (Jedis jedis = jedisPool.getResource()) {
            List<GeoRadiusResponse> radiusResponses =
                    jedis.georadius(RedisSchema.getSiteGeoKey(), coord.getLng(),
                            coord.getLat(), radius, radiusUnit);

            return radiusResponses.stream()
                    .map(response -> jedis.hgetAll(response.getMemberByString()))
                    .filter(Objects::nonNull)
                    .map(Site::new).collect(Collectors.toSet());
        }
    }

    @Override
    public void insert(Site site) {
         try (Jedis jedis = jedisPool.getResource()) {
             String key = RedisSchema.getSiteHashKey(site.getId());
             jedis.hmset(key, site.toMap());

             if (site.getCoordinate() == null) {
                 throw new IllegalArgumentException("Coordinate required for Geo " +
                         "insert.");
             }
             Double longitude = site.getCoordinate().getGeoCoordinate().getLongitude();
             Double latitude = site.getCoordinate().getGeoCoordinate().getLatitude();
             jedis.geoadd(RedisSchema.getSiteGeoKey(), longitude, latitude,
                     key);
         }
    }
}
