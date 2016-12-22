package com.finupgroup.cif.spark


import com.finupgroup.cif.utils.{DateUtils, MapTools, SegmentTools}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * spark-submit --master spark://cloud138:7077 --total-executor-cores 50 --executor-memory 15g \
--driver-class-path ojdbc14.jar --jars ojdbc14.jar --class com.tescomm.secpf.summary.HUserAndCellExpD \
secpf_2.10-1.0.jar /data/stg_dp secpf 2016-05-01\ 11:06:11
 * g_user_exp_d.sql h_basic_user_exp_d.sql h_basic_cell_exp_d.sql
 * Created by wangxy on 16-6-21.
 */

object HUserAndCellExpD {

  object seg{
    //    stg_dp.s_basic_net_event
    def unapply(in: String): Option[Array[String]] = {
      val strArr = in.split("\001", -1)
      val nameArr = Array("cur_date", "prov_id","city_id", "area_id", "lac", "cell_id","user_id","net_type", "exp_id",
        "imei", "mcc", "mnc")
      val indexArr = SegmentTools.getSegIndex("stg_dp.s_basic_net_event", nameArr)
      val res = indexArr.map{x=> strArr(x)}
      Some(res)
    }
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      System.out.println("usage: <in-path> <time> <out-path>")
      System.exit(1)
    }

    val Array(ipath, opath, itime) = args
    val yearmonth = itime.slice(0,6)
    val dym = itime.slice(0,4) + "-" + itime.slice(4,6)

    val inpath = Array(ipath,"s_basic_net_event","1","*"+yearmonth+"*","*").mkString("/")
    val outpath1 = Array(opath,"h_basic_user_exp",yearmonth).mkString("/")
    val outpath2 = Array(opath,"h_basic_cell_exp",yearmonth).mkString("/")

    val conf = new SparkConf().setAppName("exp_d")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(inpath)

    val uptime = DateUtils.getNowTime

    // 表 dim_dp.base_area_info
    val base_area_info = MapTools.getTabMap(sc, "baseAreaInfo")
    val areaInfo = sc.broadcast(base_area_info)

    // 取运营商名字
    val idname = MapTools.getcarrierName()
    val idnameMap = sc.broadcast(idname)

    // 表　sho_dp.h_cell_info
    val rdd1 = MapTools.getTabRdd(sc, "hCellInfo")

    val gRdd = rdd.map{
      case seg(Array(cur_date, prov_id, city_id, area_id, lac, cell_id, user_id, net_type, exp_id, imei, mcc, mnc)) =>
        // 需要关注的异常id
        val expids = List("2", "106", "107", "108", "110", "16", "105", "9", "14", "82", "116", "117", "111", "17",
          "18", "112")

        // time_id　取的是数据时间
        val timeid = DateUtils.getTimeFromUnix(cur_date.toLong, "yyyy-MM-dd")

        // 网络制式
        val  nettypes = List("1","5","7")
        // if中的条件　相当于sql中的where和join中的on
        if(nettypes.contains(net_type) && expids.contains(exp_id) && timeid.slice(0,7) == dym){

          val prov_name = areaInfo.value.getOrElse(prov_id, "")
          val city_name = areaInfo.value.getOrElse(city_id, "")
          val area_name = areaInfo.value.getOrElse(area_id, "")
//          val carrier_name = mdict.value.getOrElse(carrier_id2, "")
          val Array(carrier_id, carrier_name) = idnameMap.value.getOrElse(mcc+","+mnc,",").split(",", -1)
          val key = Array(prov_id, city_id, lac, cell_id).mkString(",")
          val value = Array(prov_id, city_id, area_id, lac, cell_id, prov_name, city_name, area_name, user_id,
            imei, carrier_id, carrier_name, exp_id, uptime, timeid)

          (key, value)
        }else{
          ("", Array(""))
        }
    }.filter(_._1 != "")

    // 根据　prov_id, city_id, lac, cell_id　关联cell_name　并以天、人、区域等汇总异常时间数量
    val res1 = gRdd.leftOuterJoin(rdd1).map{
      case (bkey, (Array(prov_id, city_id, area_id, lac, cell_id, prov_name, city_name, area_name, user_id,
      imei, carrier_id, carrier_name, exp_id, upd_date, timeid), Some(cell_name))) =>
        val nkey = (prov_id, city_id, area_id, lac, cell_id, cell_name, prov_name, city_name, area_name, user_id,
          imei, carrier_id, carrier_name, exp_id, upd_date, timeid)
        val value = 1
        (nkey, value)
      case (key, (Array(prov_id, city_id, area_id, lac, cell_id, prov_name, city_name, area_name, user_id,
      imei, carrier_id, carrier_name, exp_id, upd_date, timeid), None)) =>
        val key = (prov_id, city_id, area_id, lac, cell_id, "", prov_name, city_name, area_name, user_id,
          imei, carrier_id, carrier_name, exp_id, upd_date, timeid)
        val value = 1
        (key, value)
    }.reduceByKey(_+_).cache()

    // 产生h_basic_user_exp
    res1.map{
      case ((prov_id, city_id, area_id, lac, cell_id, cell_name, prov_name, city_name, area_name, user_id,
      imei, carrier_id, carrier_name, exp_id, upd_date, timeid), cnt) =>
        Array(prov_id, city_id, area_id, lac, cell_id, cell_name, prov_name, city_name, area_name, user_id, imei,
          carrier_id, carrier_name, exp_id, cnt, upd_date, "1", timeid).mkString(",")
    }.saveAsTextFile(outpath1)

    // 计算属于每个异常事件的个数分别为多少　产生h_basic_cell_exp
    res1.map{
      case ((prov_id, city_id, area_id, lac, cell_id, cell_name, prov_name, city_name, area_name, user_id,
      imei, carrier_id, carrier_name, exp_id, upd_date, timeid), cnt) =>
        val ids = List("2","9","14","16","17","18","82","105","106","107","108","110","111","112","116","117")
        val v = Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
        val index = ids.indexOf(exp_id)
        if(index != -1)
          v.update(index, cnt)
        val key = (prov_id, city_id, area_id, prov_name, city_name, area_name, lac, cell_id, cell_name, carrier_id,
          carrier_name, upd_date, timeid)
        (key, v)
    }.reduceByKey((x, y) => (x, y).zipped.map(_+_)).map{
      case ((prov_id, city_id, area_id, prov_name, city_name, area_name, lac, cell_id, cell_name, carrier_id,
      carrier_name, upd_date, timeid), v) =>
        val str1 = Array(prov_id, city_id, area_id, prov_name, city_name, area_name, lac, cell_id, cell_name,
          carrier_id, carrier_name).mkString(",")
        val str2 = v.mkString(",")
        s"$str1,$str2,$upd_date,1,$timeid"
    }.saveAsTextFile(outpath2)

    sc.stop()

  }

}
