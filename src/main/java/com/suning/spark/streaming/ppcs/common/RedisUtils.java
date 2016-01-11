package com.suning.spark.streaming.ppcs.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;

import com.suning.framework.sedis.ShardedJedisAction;
import com.suning.framework.sedis.SmartShardedJedisPipeline;
import com.suning.framework.sedis.impl.ShardedJedisClientImpl;

public class RedisUtils {
    protected static ShardedJedisClientImpl shardedClient;
    private static final Logger LOG = LoggerFactory.getLogger(RedisUtils.class);
    static {
    	shardedClient = new ShardedJedisClientImpl("redis.conf");
    }
/*
    *//**
     * 
     * 功能描述: <br>
     * 通过匹配，获取key值。
     * 
     * @param pattern
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     *//*
    public Set<String> keys(final String pattern) {
    	return shardedClient.execute(new ShardedJedisAction<Set<String>>() {
			public Set<String> doAction(ShardedJedis jedis) {
				MultiKeyCommands shardedJedis = (MultiKeyCommands) jedis;
				return shardedJedis.keys(pattern);
			}
		});
    }
*/
    /**
     * 将序列化对象值value关联到key， 如果key已经持有其他值，SET就覆写旧值，无视类型 时间复杂度O(1)
     * 
     * @param key
     * @param value
     * @return
     */
    public static String set(final String key, final String value, final int seconds) {
       try{ 
        return shardedClient.execute(new ShardedJedisAction<String>() {
            public String doAction(ShardedJedis shardedJedis) {
            	String result = shardedJedis.set(key, value);
            	shardedJedis.expire(key, seconds);
                return result;
            }
        });
       }catch(Exception ex){
            LOG.info("RedisUtils set error " + ex.getMessage());
            return null;
        }
    }
    
    /**
     * 将序列化对象值value关联到key， 如果key已经持有其他值，SET就覆写旧值，无视类型 时间复杂度O(1)
     * 
     * @param key
     * @param value
     * @return
     */
    public static  String set(final String key, final String value) {
        return shardedClient.execute(new ShardedJedisAction<String>() {
            public String doAction(ShardedJedis shardedJedis) {
                String result = shardedJedis.set(key, value);
                return result;
            }
        });
    }

    public static String mset(final int seconds, final String... keysvalues) {
        return shardedClient.execute(new ShardedJedisAction<String>() {
            public String doAction(ShardedJedis jedis) {
                MultiKeyCommands shardedJedis = (MultiKeyCommands) jedis;
                String result = shardedJedis.mset(keysvalues);
                for (int i = 0; i<keysvalues.length; i++) {
                	if (i % 2 == 0) {
                		jedis.expire(keysvalues[0], seconds);
                	}
                }
                
                return result;
            }
        });
    }

    public static List<String> mget(final String... keysvalues) {
        try{
            return shardedClient.execute(new ShardedJedisAction<List<String>>() {
                public List<String> doAction(ShardedJedis jedis) {
                    MultiKeyCommands shardedJedis = (MultiKeyCommands) jedis;
                           return shardedJedis.mget(keysvalues);
                }
            });
        }catch(Exception ex){
            LOG.info("RedisUtils mget error " + ex.getMessage());
            return null;
        }
    }
    
    /**
     * 返回key所关联的序列化对象。如果key不存在则返回null。 </br>时间复杂度O(1)
     * 
     * @param key
     * @return
     */
    public static String get(final String key) {
        try{
            return shardedClient.execute(new ShardedJedisAction<String>() {
                public String doAction(ShardedJedis shardedJedis) {
                    return shardedJedis.get(key);
                }
            });
        }catch(Exception ex){
            LOG.info("RedisUtils get error " + ex.getMessage());
            return null;
        }
    }

    /**
     * 将哈希表key中的域field的值设为value。如果key不存在，一个新的哈希表被创建并进行HSET操作。如果域field已经存在于哈希表中，旧值将被覆盖。</br> 时间复杂度O(1)
     * 
     * @param key field value
     * @return 如果field是哈希表中的一个新建域，并且值设置成功，返回1。 如果哈希表中域field已经存在且旧值已被新值覆盖，返回0。
     */
    public static Long hset(final String key, final String field, final String value, final int seconds) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
            	long result = shardedJedis.hset(key, field, value);
            	shardedJedis.expire(key, seconds);
                return result;
            }
        });
    }

    /**
     * 返回哈希表key中给定域field的值。</br> 时间复杂度O(1)
     * 
     * @param key field
     * @return 给定域的值。 当给定域不存在或是给定key不存在时，返回null。
     */
    public static String hget(final String key, final String field) {
        return shardedClient.execute(new ShardedJedisAction<String>() {
            public String doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hget(key, field);
            }
        });
    }

    /**
     * 
     * 功能描述: <br>
     * 返回hash
     *
     * @param key
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public static Map<String, String> hgetAll(final String key) {
        return shardedClient.execute(new ShardedJedisAction<Map<String, String>>() {
            public Map<String, String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hgetAll(key);
            }
        });
    }

    /**
     * 同时将多个域-值对设置到哈希表key中。如果key不存在，一个空哈希表被创建并执行HMSET操作。</br> 时间复杂度O(N)，N为域-值对的数量。
     * 
     * @param key hash
     * @return
     */
    public static String hmset(final String key, final Map<String, String> hash, final int seconds) {
        return shardedClient.execute(new ShardedJedisAction<String>() {
            public String doAction(ShardedJedis shardedJedis) {
            	String result = shardedJedis.hmset(key, hash);
            	shardedJedis.expire(key, seconds);
                return result;
            }
        });
    }

    /**
     * 删除哈希表key中的指定域，不存在的域将被忽略。 </br>时间复杂度O(1)
     * 
     * @param key field
     * @return
     */
    public static Long hdel(final String key, final String field) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.hdel(key, field);
            }
        });
    }

    /**
     * 取key值对应的集合段的数据集<\br> 时间复杂度O(S+N)
     * 
     * @param key
     * @param startIndex
     * @param endIndex
     * @return List
     */
    public static List<String> lrange(final String key, final int startIndex, final int endIndex) {
        return shardedClient.execute(new ShardedJedisAction<List<String>>() {
            public List<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.lrange(key, startIndex, endIndex);
            }
        });
    }

    /**
     * 向名称为key的zset中添加元素member，score用于排序。 如果该元素已经存在，则根据score更新该元素的顺序<\br>O(log(N))
     * 
     * @param key
     * @param score
     * @param member
     * @return 如果元素被添加，返回值为1；如果元素已经是有序集合中的一员并且scroe被更新，那么返回值为0。
     */
    public static Long zadd(final String key, final double score, final String member) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zadd(key, score, member);

            }
        });
    }

    /**
     * 删除名称为key的zset中的元素member 若删除为对象时，对象必须与member全值匹配后才生效
     * 
     * @param key
     * @param member
     * @return
     */
    public static Long zrem(final String key, final String member) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrem(key, member);
            }
        });
    }

    /**
     * 返回名称为key的zset中的index从start到end的所有元素<\br>时间复杂度O(log(N)+M)
     * 
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static Set<String> zrange(final String key, final int start, final int end) {
        return shardedClient.execute(new ShardedJedisAction<Set<String>>() {
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrange(key, start, end);
            }
        });
    }

    /**
     * 返回名称为key的zset中倒序的index从start到end的所有元素<\br>时间复杂度O(log(N)+M)
     * 
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static Set<String> zrevrange(final String key, final int start, final int end) {
        return shardedClient.execute(new ShardedJedisAction<Set<String>>() {
            public Set<String> doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zrevrange(key, start, end);
            }
        });
    }

    /**
     * 返回名称为key的zset中member的score值<\br>时间复杂度O(1)
     * 
     * @param key
     * @param
     * @param
     * @return
     */
    public static Double zscore(final String key, final String member) {
        return shardedClient.execute(new ShardedJedisAction<Double>() {
            public Double doAction(ShardedJedis shardedJedis) {
                return shardedJedis.zscore(key, member);
            }
        });
    }

    /**
     * 移除给定的key。如果key不存在，则忽略该命令。 <\br>时间复杂度O(1)
     * 
     * @param key
     * @return
     */
    public static Long del(final String key) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.del(key);
            }
        });
    }

    /**
     * 为给定key设置生存时间，当key过期时，它会被自动删除<\br> 时间复杂度O(1)
     * 
     * @param key
     * @param seconds 秒
     * @return 1：成功； 0：key不存在或不能为key设置生存时间时
     */
    public static Long expire(final String key, final int seconds) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.expire(key, seconds);
            }
        });
    }

    /**
     * 为给定key设置失效时刻，当key过期时，它会被自动删除<\br> 时间复杂度O(1)
     * 
     * @param key
     * @param unixTime Unix时间戳，单位为秒，使用时考虑误差
     * @return 1：成功； 0：key不存在或不能为key设置失效时刻
     */
    public static Long expireAt(final String key, final long unixTime) {
        return shardedClient.execute(new ShardedJedisAction<Long>() {
            public Long doAction(ShardedJedis shardedJedis) {
                return shardedJedis.expireAt(key, unixTime);
            }
        });
    }

  /*  public String flushDB() {
         return redisClient.execute(new JedisAction<String>() {
         public String doAction(Jedis Jedis) {
         return Jedis.flushDB();
         }
         });
     }*/

    /**
     * 
     * 功能描述: <br>
     * destroy
     * 
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public void destroy() {
    	shardedClient.destroy();
    }
    
    /**
     * 
     * 功能描述: <br>
     * 向名称为key的set元素添加value
     *
     * @param seconds
     * @param key
     * @param values
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public static Long ssadd(final int seconds, final String key,final String... values) {
       try{
            return shardedClient.execute(new ShardedJedisAction<Long>() {
                public Long doAction(ShardedJedis jedis) {
                    Long count = jedis.sadd(key, values);
                    jedis.expire(key,seconds);
                    
                    return count;
                }
            });
        }catch(Exception ex){
            LOG.info("RedisUtils ssadd error " + ex.getMessage());
            return null;
        }
    }
    
    public static Set<String> smembers(final String key) {
        try{
            return shardedClient.execute(new ShardedJedisAction<Set<String>>() {
                public Set<String> doAction(ShardedJedis shardedJedis) {
                    return shardedJedis.smembers(key);
                }
            });
        }catch(Exception ex){
            LOG.info("RedisUtils smembers error " + ex.getMessage());
            return null;
        }
    }
    
    public static List<String> pipeGet(final List<String> list) {
        return shardedClient.execute(new ShardedJedisAction<List<String>>() {
        public List<String> doAction(ShardedJedis shardedJedis) {
                        SmartShardedJedisPipeline pipeline = (SmartShardedJedisPipeline) shardedJedis
                                .pipelined();
                        List<String> result = new ArrayList<String>();
                        List<Response<String>> responseLists = new ArrayList<Response<String>>();
                       // pipeline.sync(); 
                        Response<String> r = null;
                        for (String string : list) {
                            r = pipeline.get(string);
                            responseLists.add(r);
                          //  list21.add(r.get());
                        }
                        pipeline.sync(); 
                     
                        for(Response<String> resonse:responseLists){
                            result.add(resonse.get());
                        }
//                        System.out.println(r.get());
                        return result;
                    }
                });
            }
    
    public static void main(String[] args) {
    	RedisUtils redis = new RedisUtils();
       // redis.set("ppcs_oms_catentry3Id_60788","244006");
    	System.out.println(redis.get("ppcs_oms_catentry3Id_6134497724"));

    	  // System.out.println(redis.get("test1"));
    /*	   List<String> list  = new ArrayList<String>();
    	   list.add("test1");
    	   list.add("test2");
    	   List<String> result  = redis.pipeGet(list);
    	   for(String str:result){
    	       System.out.println(str);
    	   }*/
    	// redis.set("tad_sub_general_109749660","109749601");
    	// System.out.println(redis.get("tad_general_sub_109749601"));
    	 // redis.ssadd(1000, "App_" + recmdKey + "_" + mainGdsId, i + ":" + simGdsId);
   	 /* Set<String> keySets = redis.smembers("screen_9999999999"); 
    	// System.out.println(redis.get("tad_orebuyPro_Cache_B0DA0BDD-EDBA-4E38-8A80-043F451CEB07"));
    	//  Set<String> keySets = redis.smembers("App_6104674297_127733057"); 
    	  System.out.println(keySets.size());
    	   for(String str:keySets){
    	       System.out.println(str);
    	   }
    	  for (String key : keySets) {
              if (!StringUtils.isBlank(key)) {
                  //LOG.info("FilterOrderBolt key " + key);
                 System.out.println(key);
              }
              //LOG.info("0 dspOutPut " + dspOutput.toString());
          }*/
//    	redis.set("tad_vendor_gds_102587097","123127#@#R10000001#@#007100064");
    //	redis.set("tad_vendor_gds_102587032","123124#@#R10000001#@#007100061");
//    	System.out.println(redis.get("tad_vendor_gds_102587091"));
/*    	redis.set("tad_gds_102587097","2#@#102#@#2");
    	redis.del("tad_orebuyPro_Cache_111111");*/
    	//System.out.println(redis.get("tad_orebuyPro_Cache_" + "323232322323232"));
    	
    /*	redis.ssadd(10, "testmem", "777");
    	redis.ssadd(10, "testmem", "123456|323|32");
    	
    	Set<String> testSet = redis.smembers("testmem");
    	for(String tmp:testSet){
    	    System.out.println(tmp);
    	}
    	*/
    	/*long before = System.currentTimeMillis();
    	for(int i =0;i<10000;i++){
    	    redis.set("test_" + i, "0.12255161561616165165161",600000);
    	}
    	for(int i =0;i<10000;i++){
            String tmp = redis.get("test_" + i);
        }
    	long end = System.currentTimeMillis();
    	System.out.println(end-before);*/
/*    	System.out.println(redis.get("tad_goods_group_102587034"));
    	System.out.println(redis.get("tad_goods_group_102587037"));
    	System.out.println(redis.get("tad_goods_group_102587038"));*/
    	//System.out.println(redis.get("tad_sub_general_" + "102587138"));
    	
    /*	redis.set("tad_sub_general_102587032","102587091");
    	   redis.set("tad_sub_general_102587033","102587116");
    	    redis.set("tad_sub_general_102587034","102587132");
    	    redis.set("tad_sub_general_102587035","102587150");
    	    redis.set("tad_sub_general_102587036","102587172");
    	    redis.set("tad_sub_general_102587037","102587181");*/

    /*	String[] s = {"tad_goods_group_123816401","tad_goods_group_108231869"};
    	List<String> arr = redis.mget(s);
    	System.out.println(arr);*/
    	 /*  String[] skuGrpArray = new String[4];
    	   List<String> arr = redis.mget(skuGrpArray);
           System.out.println(arr);*/
    //	redis.set("tad_goods_group_107055506", "R1207002");
    	//   redis.set("tad_goods_group_124727577", "R9001849");
     
   //  System.out.println(redis.get("tad_goods_group_102587174"));
    //	System.out.println(redis.get("tad_general_sub_107055506"));
    	//System.out.println(redis.get("tad_sub_general_107055506"));
    	//redis.set("tad_gds_105332283","2#@#101#@#1");
    	//redis.set("tad_gds_102587091","1#@#101#@#1");
//System.out.println(redis.get("tad_gds_102587091"));
/*redis.set("tad_gds_102587032","1#@#101#@#1");
redis.set("tad_gds_102587093","2#@#102#@#2");
redis.set("tad_gds_102587095","3#@#103#@#3");
redis.set("tad_gds_102587097","4#@#104#@#4");
redis.set("tad_gds_102587099","5#@#105#@#5");
redis.set("tad_gds_102587101","6#@#106#@#6");
redis.set("tad_gds_102587103","7#@#107#@#7");
redis.set("tad_gds_102587105","8#@#108#@#8");
redis.set("tad_gds_102587107","9#@#109#@#9");
redis.set("tad_gds_102587109","10#@#110#@#10");
redis.set("tad_gds_102587110","11#@#111#@#11");
redis.set("tad_gds_102587112","12#@#112#@#12");
redis.set("tad_gds_102587114","13#@#113#@#13");
redis.set("tad_gds_102587116","14#@#114#@#14");
redis.set("tad_gds_102587118","15#@#115#@#15");
redis.set("tad_gds_102587121","16#@#116#@#16");
redis.set("tad_gds_102587123","17#@#117#@#17");
redis.set("tad_gds_102587129","18#@#118#@#18");
redis.set("tad_gds_102587130","19#@#119#@#19");
redis.set("tad_gds_102587132","20#@#120#@#20");
redis.set("tad_gds_102587134","21#@#121#@#21");
redis.set("tad_gds_102587136","22#@#122#@#22");
redis.set("tad_gds_102587138","23#@#123#@#23");
redis.set("tad_gds_102587141","24#@#124#@#24");
redis.set("tad_gds_102587143","25#@#125#@#25");
redis.set("tad_gds_102587145","26#@#126#@#26");
redis.set("tad_gds_102587147","27#@#127#@#27");
redis.set("tad_gds_102587149","28#@#128#@#28");
redis.set("tad_gds_102587150","29#@#129#@#29");
redis.set("tad_gds_102587152","30#@#130#@#30");
redis.set("tad_gds_102587154","31#@#131#@#31");
redis.set("tad_gds_102587156","32#@#132#@#32");
redis.set("tad_gds_102587158","33#@#133#@#33");
redis.set("tad_gds_102587161","34#@#134#@#34");
redis.set("tad_gds_102587163","35#@#135#@#35");
redis.set("tad_gds_102587165","36#@#136#@#36");
redis.set("tad_gds_102587167","37#@#137#@#37");
redis.set("tad_gds_102587169","38#@#138#@#38");
redis.set("tad_gds_102587170","39#@#139#@#39");
redis.set("tad_gds_102587172","40#@#140#@#40");
redis.set("tad_gds_102587174","41#@#141#@#41");
redis.set("tad_gds_102587176","42#@#142#@#42");
redis.set("tad_gds_102587178","43#@#143#@#43");
redis.set("tad_gds_102587181","44#@#144#@#44");
redis.set("tad_gds_102587183","45#@#145#@#45");
redis.set("tad_gds_102587185","46#@#146#@#46");
redis.set("tad_gds_102587187","47#@#147#@#47");
redis.set("tad_gds_102587189","48#@#148#@#48");
redis.set("tad_gds_102587190","49#@#149#@#49");
redis.set("tad_gds_102587192","50#@#150#@#50");
redis.set("tad_vendor_gds_102587032","123124#@#R10000001#@#007100061");
redis.set("tad_vendor_gds_102587093","123125#@#R10000001#@#007100062");
redis.set("tad_vendor_gds_102587095","123126#@#R10000001#@#007100063");
redis.set("tad_vendor_gds_102587097","123127#@#R10000001#@#007100064");
redis.set("tad_vendor_gds_102587099","123128#@#R10000001#@#007100065");
redis.set("tad_vendor_gds_102587101","123129#@#R10000001#@#007100066");
redis.set("tad_vendor_gds_102587103","123130#@#R10000001#@#007100067");
redis.set("tad_vendor_gds_102587105","123131#@#R10000001#@#007100068");
redis.set("tad_vendor_gds_102587107","123132#@#R10000001#@#007100069");
redis.set("tad_vendor_gds_102587109","123133#@#R10000001#@#007100070");
redis.set("tad_vendor_gds_102587110","123134#@#R10000001#@#007100071");
redis.set("tad_vendor_gds_102587112","123135#@#R10000001#@#007100072");
redis.set("tad_vendor_gds_102587114","123136#@#R10000001#@#007100073");
redis.set("tad_vendor_gds_102587116","123137#@#R10000001#@#007100074");
redis.set("tad_vendor_gds_102587118","123138#@#R10000001#@#007100075");
redis.set("tad_vendor_gds_102587121","123139#@#R10000001#@#007100076");
redis.set("tad_vendor_gds_102587123","123140#@#R10000001#@#007100077");
redis.set("tad_vendor_gds_102587129","123141#@#R10000002#@#007100078");
redis.set("tad_vendor_gds_102587130","123142#@#R10000002#@#007100079");
redis.set("tad_vendor_gds_102587132","123143#@#R10000002#@#007100080");
redis.set("tad_vendor_gds_102587134","123144#@#R10000002#@#007100081");
redis.set("tad_vendor_gds_102587136","123145#@#R10000002#@#007100082");
redis.set("tad_vendor_gds_102587138","123146#@#R10000002#@#007100083");
redis.set("tad_vendor_gds_102587141","123147#@#R10000002#@#007100084");
redis.set("tad_vendor_gds_102587143","123148#@#R10000002#@#007100085");
redis.set("tad_vendor_gds_102587145","123149#@#R10000002#@#007100086");
redis.set("tad_vendor_gds_102587147","123150#@#R10000002#@#007100087");
redis.set("tad_vendor_gds_102587149","123151#@#R10000002#@#007100088");
redis.set("tad_vendor_gds_102587150","123152#@#R10000002#@#007100089");
redis.set("tad_vendor_gds_102587152","123153#@#R10000002#@#007100090");
redis.set("tad_vendor_gds_102587154","123154#@#R10000002#@#007100091");
redis.set("tad_vendor_gds_102587156","123155#@#R10000002#@#007100092");
redis.set("tad_vendor_gds_102587158","123156#@#R10000002#@#007100093");
redis.set("tad_vendor_gds_102587161","123157#@#R10000002#@#007100094");
redis.set("tad_vendor_gds_102587163","123158#@#R10000002#@#007100095");
redis.set("tad_vendor_gds_102587165","123159#@#R10000002#@#007100096");
redis.set("tad_vendor_gds_102587167","123160#@#R10000002#@#007100097");
redis.set("tad_vendor_gds_102587169","123161#@#R10000002#@#007100098");
redis.set("tad_vendor_gds_102587170","123162#@#R10000002#@#007100099");
redis.set("tad_vendor_gds_102587172","123163#@#R10000002#@#007100100");
redis.set("tad_vendor_gds_102587174","123164#@#R10000002#@#007100101");
redis.set("tad_vendor_gds_102587176","123165#@#R10000002#@#007100102");
redis.set("tad_vendor_gds_102587178","123166#@#R10000003#@#007100103");
redis.set("tad_vendor_gds_102587181","123167#@#R10000003#@#007100104");
redis.set("tad_vendor_gds_102587183","123168#@#R10000003#@#007100105");
redis.set("tad_vendor_gds_102587185","123169#@#R10000003#@#007100106");
redis.set("tad_vendor_gds_102587187","123170#@#R10000003#@#007100107");
redis.set("tad_vendor_gds_102587189","123171#@#R10000003#@#007100108");
redis.set("tad_vendor_gds_102587190","123172#@#R10000003#@#007100109");
redis.set("tad_vendor_gds_102587192","123173#@#R10000003#@#007100110");
    	System.out.println(redis.get("tad_general_sub_102587097"));
    	System.out.println(redis.get("tad_general_sub_102587116"));
    	System.out.println(redis.get("tad_general_sub_102587134"));
    	System.out.println(redis.get("tad_general_sub_102587097"));
    	System.out.println(redis.get("tad_general_sub_102587097"));
    	
redis.set("tad_goods_group_102587091","R10000001");
redis.set("tad_goods_group_102587093","R10000001");
redis.set("tad_goods_group_102587095","R10000001");
redis.set("tad_goods_group_102587097","R10000001");
redis.set("tad_goods_group_102587099","R10000001");
redis.set("tad_goods_group_102587101","R10000001");
redis.set("tad_goods_group_102587103","R10000001");
redis.set("tad_goods_group_102587105","R10000001");
redis.set("tad_goods_group_102587107","R10000001");
redis.set("tad_goods_group_102587109","R10000001");
redis.set("tad_goods_group_102587110","R10000001");
redis.set("tad_goods_group_102587112","R10000001");
redis.set("tad_goods_group_102587114","R10000001");
redis.set("tad_goods_group_102587116","R10000001");
redis.set("tad_goods_group_102587118","R10000001");
redis.set("tad_goods_group_102587121","R10000001");
redis.set("tad_goods_group_102587123","R10000001");
redis.set("tad_goods_group_102587129","R10000002");
redis.set("tad_goods_group_102587130","R10000002");
redis.set("tad_goods_group_102587132","R10000002");
redis.set("tad_goods_group_102587134","R10000002");
redis.set("tad_goods_group_102587136","R10000002");
redis.set("tad_goods_group_102587138","R10000002");
redis.set("tad_goods_group_102587141","R10000002");
redis.set("tad_goods_group_102587143","R10000002");
redis.set("tad_goods_group_102587145","R10000002");
redis.set("tad_goods_group_102587147","R10000002");
redis.set("tad_goods_group_102587149","R10000002");
redis.set("tad_goods_group_102587150","R10000002");
redis.set("tad_goods_group_102587152","R10000002");
redis.set("tad_goods_group_102587154","R10000002");
redis.set("tad_goods_group_102587156","R10000002");
redis.set("tad_goods_group_102587158","R10000002");
redis.set("tad_goods_group_102587161","R10000002");
redis.set("tad_goods_group_102587163","R10000002");
redis.set("tad_goods_group_102587165","R10000002");
redis.set("tad_goods_group_102587167","R10000002");
redis.set("tad_goods_group_102587169","R10000002");
redis.set("tad_goods_group_102587170","R10000002");
redis.set("tad_goods_group_102587172","R10000002");
redis.set("tad_goods_group_102587174","R10000002");
redis.set("tad_goods_group_102587176","R10000002");
redis.set("tad_goods_group_102587178","R10000003");
redis.set("tad_goods_group_102587181","R10000003");
redis.set("tad_goods_group_102587183","R10000003");
redis.set("tad_goods_group_102587185","R10000003");
redis.set("tad_goods_group_102587187","R10000003");
redis.set("tad_goods_group_102587189","R10000003");
redis.set("tad_goods_group_102587190","R10000003");
redis.set("tad_goods_group_102587192","R10000003");

redis.set("tad_general_sub_102587091","102587032");
redis.set("tad_general_sub_102587093","102587032");
redis.set("tad_general_sub_102587095","102587032");
redis.set("tad_general_sub_102587097","102587032");
redis.set("tad_general_sub_102587099","102587032");
redis.set("tad_general_sub_102587101","102587032");
redis.set("tad_general_sub_102587103","102587032");
redis.set("tad_general_sub_102587105","102587032");
redis.set("tad_general_sub_102587107","102587032");
redis.set("tad_general_sub_102587109","102587032");
redis.set("tad_general_sub_102587110","102587032");
redis.set("tad_general_sub_102587112","102587033");
redis.set("tad_general_sub_102587114","102587033");
redis.set("tad_general_sub_102587116","102587033");
redis.set("tad_general_sub_102587118","102587033");
redis.set("tad_general_sub_102587121","102587033");
redis.set("tad_general_sub_102587123","102587033");
redis.set("tad_general_sub_102587129","102587034");
redis.set("tad_general_sub_102587130","102587034");
redis.set("tad_general_sub_102587132","102587034");
redis.set("tad_general_sub_102587134","102587034");
redis.set("tad_general_sub_102587136","102587034");
redis.set("tad_general_sub_102587138","102587034");
redis.set("tad_general_sub_102587141","102587034");
redis.set("tad_general_sub_102587143","102587034");
redis.set("tad_general_sub_102587145","102587034");
redis.set("tad_general_sub_102587147","102587034");
redis.set("tad_general_sub_102587149","102587035");
redis.set("tad_general_sub_102587150","102587035");
redis.set("tad_general_sub_102587152","102587035");
redis.set("tad_general_sub_102587154","102587035");
redis.set("tad_general_sub_102587156","102587035");
redis.set("tad_general_sub_102587158","102587035");
redis.set("tad_general_sub_102587161","102587035");
redis.set("tad_general_sub_102587163","102587036");
redis.set("tad_general_sub_102587165","102587036");
redis.set("tad_general_sub_102587167","102587036");
redis.set("tad_general_sub_102587169","102587036");
redis.set("tad_general_sub_102587170","102587036");
redis.set("tad_general_sub_102587172","102587036");
redis.set("tad_general_sub_102587174","102587036");
redis.set("tad_general_sub_102587176","102587036");
redis.set("tad_general_sub_102587178","102587037");
redis.set("tad_general_sub_102587181","102587037");
redis.set("tad_general_sub_102587183","102587037");
redis.set("tad_general_sub_102587185","102587037");
redis.set("tad_general_sub_102587187","102587037");
redis.set("tad_general_sub_102587189","102587037");
redis.set("tad_general_sub_102587190","102587037");
redis.set("tad_general_sub_102587192","102587037");*/
    	//redis.set("tad_general_sub_102587032","102587091");
    	
    	
//    	System.out.println(redis.get("tad_goods_group_102587037"));
    	
    /*	redis.set("232432432432423432&2323", "test");
    	
    	System.out.println("----------" + redis.get("23243243243242343"));
    	   System.out.println("----------" + redis.get("232432432432423432&2323"));
    	   
    	   try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
           System.out.println("----------" + redis.get("232432432432423432&2323"));*/
    /*	Map<String,String> map = new HashMap<String,String>();
    	map.put("212","23423424");
    	redis.hmset("test", map, 10000);
    	
    	System.out.println("----------" + redis.hget("test", "212"));*/
//    	Set<String> keys = redis.keys("*");
//    	int setSize = keys.size();
//    	System.out.println(setSize);
//    	for (String key : keys) {
//    		System.out.println(key);
//    	}
    /*	Map<String, String> gds = redis.hgetAll("dfp:gds:107833750");
    	System.out.println(gds.size());
    	for (String s : gds.keySet()) {
    		System.out.println(s + "----------" + gds.get(s));
    	}*/
//    	if (setSize % 2 != 0) {
//    		setSize = setSize + 1;
//    	}
//    	int count = 0;
//    	for (String k : keys) {
//    		redis.del(k);
//    		count ++;
//    		if (count % 10000 == 0) {
//    			System.out.println("del " + count);
//    		}
//    		if (count == 300000) {
//    			System.out.println("del " + count);
//    			break;
//    		}
//    	}
	}
    
}