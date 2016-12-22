package com.finupgroup.cif.utils



/**
 *　
 * Created by wangxy on 16-6-30.
 */
object SegmentTools {

  val tabInfo = ConfigUtils.getConfig("/config/tables.properties")

  /**
   * 根据传入的表名，获取传入字段相应的序号
   * @param tabName　表明
   * @param nameArr　字段名数组（要和配置文件中的名一致，否则对应的字段序号会是-1）
   * @return
   */
  def getSegIndex(tabName: String, nameArr: Array[String]): Array[Int] = {
    tabInfo.get(tabName) match{
      case Some(tabSegNames) =>
        val segSeq = tabSegNames.split(",", -1)
        nameArr.map{x=>segSeq.indexOf(x)}
      case None =>
        println("Table isn't in tables.properies !!!")
        Array[Int]()
    }
  }

  /**
    * rp
    * 根据传入的表名，获取传入字段相应的序号
    * @param tabName　表明
    * @param nameArr　字段名数组（要和配置文件中的名一致，否则对应的字段序号会是-1）
    * @return (字段名_下标编号)
    */
  def getNameIndex(tabName: String, nameArr: Array[String])  = {
    tabInfo.get(tabName) match{
      case Some(tabSegNames) =>
        val segSeq = tabSegNames.split(",", -1)
        nameArr.map{
          x=>
            (x,segSeq.indexOf(x))
        }.toMap

      case None =>
        println("Table isn't in tables.properies !!!")
        Map(""->1)
    }
  }


  def main(args: Array[String]): Unit = {

    val a = Array("start_date", "prov_id", "city_id", "area_id", "user_id", "net_type", "imei", "lac",
      "cell_id", "map_lati", "map_longi", "mcc", "mnc")
    println(getSegIndex("stg_dp.s_basic_para_detail", a).mkString(","))

  }

}
