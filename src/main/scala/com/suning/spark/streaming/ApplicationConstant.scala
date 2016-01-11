package com.suning.spark.streaming

import scala.beans.BeanProperty

/**
 * Created by 14070345 on 2015/12/7 0007.
 */
object ApplicationConstant {
  @BeanProperty
  var zkQuorum="10.27.25.80:2181,10.27.25.81:2181,10.27.25.82:2181"
  @BeanProperty
  var brokers="10.27.25.164:9092,10.27.25.165:9093,10.27.25.166:9094"
  @BeanProperty
  var group ="spark_BrandCat_111"
  @BeanProperty
  var topics="spark-short-action-output"
  @BeanProperty
 // var OMStopics="dfp-oms-history"
  var OMStopics="spark-short-action-output"
  @BeanProperty
  var omsordergroup ="spark_omsorder_0"

  @BeanProperty
  var resulttopics="spark-short-action-output-result"
  @BeanProperty
  var numThreads="2"
  @BeanProperty
  var checkpointDirectory="/user/bdapp/sparkstreamingcheckpoint/BrandCat"
  val cookieId="cookieId"
  var memberId="memberId"
  var imei="imei"
  val simGdsId="simGdsId"
  val score="score"
  var userType="userType"
  val l4_group="l4GdsGroupDd"
  val l3_group="l3GdsGroupDd"
  val l3Catgroup="l3CatGroup"
  val sublist="sublist"
  val l3GropuTable="l3GropuTable"
  val aConstant=0.012
  val memInCardNo="memInCardNo"
  val generalGdsCode="generalGdsCode"
  val general_cd="general_cd"
  val visitor_id="visitor_id"
  /*
    use bi_dm;
drop table bi_dm.catgroup3Table;

set mapred.reduce.tasks=24;
create table bi_dm.catgroup3Table(gds_cd String,l4_gds_group_cd String,l1_gds_group_cd String,brand_cd String ,catgroup3_id String);
use bi_dm;
insert into table bi_dm.catgroup3Table select ta.gds_cd,
        ta.l4_gds_group_cd,
        tb.l1_gds_group_cd,
        ta.brand_cd,
        td.catgroup3_id
  from bi_dm.dm_gds_inf_td ta
inner join bi_dm.dm_gds_group_inf_td tb
    on ta.l4_gds_group_cd = tb.l4_gds_group_cd
inner join bi_td.tdpa_ca_catentry_info tc
on tc.gds_id <> '-'
   and ta.gds_cd = tc.gds_id
inner join (select catentry_id,
                       max(catgroup3_id) as catgroup3_id
                 from bi_td.tdpa_b2c_catentry_grp_horz_td
                where catgroup3_name is not null
                group by catentry_id) td
    on tc.catentry_id = td.catentry_id;
   */
 // val cacheL3GroupTableSql="cache table catgroup3Table as select * from bi_dm.catgroup3Table"
  val cacheL3GroupTableSql="select gds_cd,catgroup3_id,l1_gds_group_cd,brand_cd from bi_dm.catgroup3Table sort by gds_cd"
  val catgroup3_idSql="select" +
                       " temptable.cookeId,temptable.simGdsId,catgroup3Table.catgroup3_id , " +   //key
                       " temptable.score,temptable.l4GdsGroupDd " +   //value
                       "from catgroup3Table  join temptable " +
                       "on  catgroup3Table.l4_gds_group_cd=temptable.l4GdsGroupDd limit 1"
  val l4_gds_group_cd="l4_gds_group_cd"
  val l3_gds_group_cd="l3_gds_group_cd"
  val brand_cd="brand_cd"
  val catgroup3_id="catgroup3_id"

  val catgroup3_memberIdSql = "select member_id,catentry3_id from BI_DM.TDM_DSP_MEM_CATENTRY_ED sort by member_id"

  var brandCat3Format = "{\"brand_id\":\"%s\",\"catgroup3_id\":\"%s\",\"user_brandcat_score\":\"%s\",\"l1_gds_group_cd\":\"%s\",\"l4_gds_group_cd\":\"%s\"}"
  var recbrandCat3Format = "{\"recKey\":\"%s\",\"recLists\":[%s]}"
}
