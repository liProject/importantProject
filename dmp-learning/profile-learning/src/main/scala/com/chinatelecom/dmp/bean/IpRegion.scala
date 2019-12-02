package com.chinatelecom.dmp.bean

// 封装IP地址解析的值
case class IpRegion(
					   ip: String, // IP地址信息
					   longitude: Double, latitude: Double, // 经纬度信息
					   province: String, city: String, // 省份和城市信息
					   geoHash: String // geoHash值
				   )