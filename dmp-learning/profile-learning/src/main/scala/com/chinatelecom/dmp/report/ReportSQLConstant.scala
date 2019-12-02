package com.chinatelecom.dmp.report

object ReportSQLConstant {
  def reportAdsKpiWithSQL(viewName: String, groupField: Seq[String]) = {

    val groupStr: String = groupField.mkString(",")
    s"""
       |WITH t AS (
       |SELECT
       |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
       |  ${groupStr},
       |  SUM(CASE
       |    WHEN (requestmode = 1 and processnode >= 1) THEN 1
       |    ELSE 0
       |    END) AS orginal_req_cnt,
       |  SUM(CASE
       |    WHEN (requestmode = 1 and processnode >= 2) THEN 1
       |    ELSE 0
       |    END) AS valid_req_cnt,
       |  SUM(CASE
       |    WHEN (requestmode = 1 and processnode = 3) THEN 1
       |    ELSE 0
       |    END) AS ad_req_cnt,
       |  SUM(CASE
       |    WHEN (adplatformproviderid >= 100000 and
       |         iseffective = 1 and
       |         isbilling = 1 and
       |         isbid = 1 and
       |         adorderid != 0) THEN 1
       |    ELSE 0
       |    END) AS join_rtx_cnt,
       | SUM(CASE
       |    WHEN (adplatformproviderid >= 100000 and
       |         iseffective = 1 and
       |         isbilling = 1 and
       |         iswin = 1 and
       |         adorderid != 0) THEN 1
       |    ELSE 0
       |    END) AS success_rtx_cnt,
       |  SUM(CASE
       |    WHEN (requestmode = 2 and iseffective = 1) THEN 1
       |    ELSE 0
       |    END) AS ad_show_cnt,
       |  SUM(CASE
       |    WHEN (requestmode = 3 and iseffective = 1 and adorderid != 0) THEN 1
       |    ELSE 0
       |    END) AS ad_click_cnt,
       |  SUM(CASE
       |    WHEN (requestmode = 2 and
       |         iseffective = 1 and
       |         isbilling=1 and
       |         isbid = 1 and
       |         iswin = 1) THEN 1
       |    ELSE 0
       |    END) AS media_show_cnt,
       |  SUM(CASE
       |    WHEN (requestmode = 3 and
       |         iseffective = 1 and
       |         isbilling=1 and
       |         isbid = 1 and
       |         iswin = 1) THEN 1
       |    ELSE 0
       |    END) AS media_click_cnt,
       |  SUM(CASE
       |    WHEN (adplatformproviderid >= 100000 and
       |         iseffective = 1 and
       |         isbilling=1 and
       |         iswin = 1 and
       |         adorderid > 200000 and
       |         adcreativeid > 200000) THEN floor(winprice /1000)
       |    ELSE 0
       |    END) AS dsp_pay_money,
       |  SUM(CASE
       |    WHEN (adplatformproviderid >= 100000 and
       |         iseffective = 1 and
       |         isbilling=1 and
       |         isbid = 1 and
       |         iswin = 1 and
       |         adorderid > 200000 and
       |         adcreativeid > 200000) THEN floor(adpayment /1000)
       |    ELSE 0
       |    END) AS dsp_cost_money
       |FROM
       |  ${viewName}
       |Group BY ${groupStr}
       |)
       |SELECT
       |  report_date, ${groupStr}, orginal_req_cnt, valid_req_cnt,
       |  ad_req_cnt, join_rtx_cnt, success_rtx_cnt, ad_show_cnt,
       |  ad_click_cnt, media_show_cnt, media_click_cnt,
       |  dsp_pay_money, dsp_cost_money,
       |  round(success_rtx_cnt / join_rtx_cnt, 2) AS success_rtx_rate,
       |  round(ad_click_cnt / ad_show_cnt, 2) AS ad_click_rate,
       |  round(media_click_cnt / media_show_cnt, 2) AS media_click_rate
       |FROM
       |  t
       |WHERE
       |  join_rtx_cnt != 0 AND success_rtx_cnt != 0
       |  AND
       |  ad_show_cnt != 0 AND ad_click_cnt != 0
       |  AND
       |  media_show_cnt != 0 AND media_click_cnt != 0
    """.stripMargin
  }
}
