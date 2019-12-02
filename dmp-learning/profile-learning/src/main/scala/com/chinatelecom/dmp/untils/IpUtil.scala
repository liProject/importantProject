package com.chinatelecom.dmp.untils

import ch.hsr.geohash.GeoHash
import com.chinatelecom.dmp.bean.IpRegion
import com.maxmind.geoip.{Location, LookupService}
import org.lionsoul.ip2region.{DataBlock, DbSearcher}

class IpUtil {

  def transfromRegion(ip: String,dbSearcher: DbSearcher,lookupService: LookupService) = {
    val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
    val region: String = dataBlock.getRegion
    val Array(_, _, province, city, _) = region.split("\\|")
    val location: Location = lookupService.getLocation(ip)
    val latitude: Float = location.latitude
    val longitude: Float = location.longitude
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 8)
    IpRegion(ip,longitude, latitude,province, city,  geoHash)
  }

}

