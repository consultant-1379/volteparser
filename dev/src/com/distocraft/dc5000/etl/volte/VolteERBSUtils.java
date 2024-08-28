package com.distocraft.dc5000.etl.volte;

public class VolteERBSUtils {

	public static String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
					"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
					"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
					"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
	
	public static String Where_ERBS = "where ERBS in ( $ERBSLIST ) AND OSS_ID = '$OSS'";
	public static String And_Where_ERBS = " and ERBS in ( $ERBSLIST ) AND OSS_ID = '$OSS'";
	
	public static String Partition_Query = "select tablename from dwhpartition where CURRENT DATE between starttime and endtime AND storageid in ( $TABLENAMES )";
	public static String ERBS_Data_Tables = "'DC_E_ERBS_EUTRANCELLFDD:RAW' , 'DC_E_ERBS_EUTRANCELLTDD:RAW' , 'DC_E_ERBSG2_EUTRANCELLFDD:RAW' , 'DC_E_ERBSG2_EUTRANCELLTDD:RAW'";
	public static String ERBS_Vector_Data_Tables = "'DC_E_ERBS_EUTRANCELLFDD_V:RAW' , 'DC_E_ERBS_EUTRANCELLTDD_V:RAW' , 'DC_E_ERBSG2_EUTRANCELLFDD_V:RAW' , 'DC_E_ERBSG2_EUTRANCELLTDD_V:RAW'";
	
	public static String SQL_Skeleton_Ranking ="select top(100) KPI_VALUE, DATETIME_ID, ERBS, OSS_ID from( " + 
			"	      $Query" + 
			") as KPI order by KPI_VALUE DESC";	
	
	public static String SQL_Skeleton_KPIA ="select ERBS, DATETIME_ID, OSS_ID, avg(KPI_VALUE) as KPI_VALUE " + 
			"from (       " + 
			"    $Query" + 
			")as KPI " + 
			"group by ERBS, DATETIME_ID, OSS_ID " + 
			"order by ERBS";
	
	public static String SQL_Query_KPI_24 ="SELECT ERBS, DATETIME_ID, OSS_ID, " + 
			"100 * pmVoipQualityRbUlOk/(pmVoipQualityRbUlOk+pmVoipQualityRbUlNok) as KPI_VALUE " +  
			"FROM $tablename  " + 
			"WHERE UTC_DATETIME_ID = '$datestamp' $ERBSCLAUSE";
	
	public static String SQL_Skeleton_KPI_29 ="select OSS_ID, ERBS, DATETIME_ID, 100 * avg(KPI_VALUE) as KPI_VALUE from( " + 
			"	select  " + 
			"	FACTOR_1.OSS_ID as OSS_ID,  " + 
			"	FACTOR_1.ERBS as ERBS,  " + 
			"	FACTOR_1.DATETIME_ID as DATETIME_ID,  " +
			"	FACTOR_1.cell as cell, " + 
			"	factor_1 * factor_2 * factor_3 as KPI_VALUE " + 
			"	from( " + 
			"		select OSS_ID, DATETIME_ID, ERBS, cell, factor_1       " + 
			"		from( " + 
			"			$FACTOR_1                 " + 
			"		) as KPI " + 
			"		where UTC_DATETIME_ID = '$datestamp' AND OSS_ID = '$OSS' " + 
			"	) as FACTOR_1, " + 
			"	( " + 
			"		select OSS_ID, DATETIME_ID, ERBS, cell, factor_2       " + 
			"		from( " + 
			"			$FACTOR_2  " + 
			"		) as KPI " + 
			"		where UTC_DATETIME_ID = '$datestamp' AND OSS_ID = '$OSS' " + 
			"	) as FACTOR_2, " + 
			"	( " + 
			"		select OSS_ID, ERBS, DATETIME_ID, cell, factor_3       " + 
			"		from( " + 
			"			$FACTOR_3" + 
			"		) as KPI " + 
			"		where UTC_DATETIME_ID = '$datestamp' AND OSS_ID = '$OSS' " + 
			"	) as FACTOR_3 " + 
			"	where FACTOR_1.ERBS=FACTOR_2.ERBS and FACTOR_2.ERBS=FACTOR_3.ERBS  " + 
			"	and  FACTOR_1.cell=FACTOR_2.cell and FACTOR_2.cell=FACTOR_3.cell  " + 
			") as KPI " + 
			"group by OSS_ID, ERBS, DATETIME_ID";	
	
	public static String SQL_Query_KPI_29_FACTOR1 ="select DATETIME_ID, UTC_DATETIME_ID, OSS_ID, ERBS, $cell as cell, " +
			"isnull(pmRrcConnEstabSuccMod + pmRrcConnEstabSuccMta, pmRrcConnEstabSuccMod, pmRrcConnEstabSuccMta)/isnull(pmRrcConnEstabAttMod + pmRrcConnEstabAttMta - isnull(pmRrcConnEstabAttReattMod, 0) - isnull(pmRrcConnEstabAttReattMta, 0) - isnull(pmRrcConnEstabFailMmeOvlMod, 0),"
			+ "pmRrcConnEstabAttMod - isnull(pmRrcConnEstabAttReattMod, 0) - isnull(pmRrcConnEstabAttReattMta, 0) - isnull(pmRrcConnEstabFailMmeOvlMod, 0),"
			+ "pmRrcConnEstabAttMta - isnull(pmRrcConnEstabAttReattMod, 0) - isnull(pmRrcConnEstabAttReattMta, 0) - isnull(pmRrcConnEstabFailMmeOvlMod, 0)) as factor_1 " +  
			"from $tablename " +
			"$ERBSCLAUSE";
	
	public static String SQL_Query_KPI_29_FACTOR2 ="select DATETIME_ID, UTC_DATETIME_ID, OSS_ID, ERBS, $cell as cell, " +
			"isnull(pmS1SigConnEstabSuccMod + pmS1SigConnEstabSuccMta, pmS1SigConnEstabSuccMod, pmS1SigConnEstabSuccMta)/isnull(pmS1SigConnEstabAttMod + pmS1SigConnEstabAttMta, pmS1SigConnEstabAttMod, pmS1SigConnEstabAttMta) as factor_2 " +  
			"from $tablename " +
			"$ERBSCLAUSE";
	
	public static String SQL_Query_KPI_29_FACTOR3 ="select DATETIME_ID, UTC_DATETIME_ID, OSS_ID, ERBS, $cell as cell, (pmErabEstabSuccInitQci/pmErabEstabAttInitQci) as factor_3 " + 
			"from $tablename where DCVECTOR_INDEX=1 $ERBSCLAUSE";
	
	public static String SQL_Query_KPI_30 ="SELECT ERBS, DATETIME_ID, OSS_ID, " + 
			"100*(pmErabEstabSuccAddedQci)/(pmErabEstabAttAddedQci - isnull(pmErabEstabAttAddedHoOngoingQci,0)) as KPI_VALUE " +  
			"FROM $tablename " + 
			"WHERE UTC_DATETIME_ID = '$datestamp' " + 
			"and DCVECTOR_INDEX=1 $ERBSCLAUSE";
	
	public static String SQL_Skeleton_KPI_31 ="select ERBS, OSS_ID, DATETIME_ID, avg(KPI_VALUE) as KPI_VALUE " + 
			"	from( " + 
			"	      $Query" + 
			"	)as KPI " + 
			"	group by ERBS, DATETIME_ID, OSS_ID";	
	
	public static String SQL_Query_KPI_31 ="SELECT ERBS, OSS_ID, DATETIME_ID, " + 
			"100*(isnull(pmErabRelAbnormalEnbActQci+pmErabRelAbnormalMmeActQci,pmErabRelAbnormalEnbActQci,pmErabRelAbnormalMmeActQci)/isnull(pmErabRelAbnormalEnbQci+pmErabRelNormalEnbQci+pmErabRelMmeQci, pmErabRelAbnormalEnbQci+pmErabRelNormalEnbQci, pmErabRelNormalEnbQci+pmErabRelMmeQci, pmErabRelAbnormalEnbQci+pmErabRelMmeQci,pmErabRelAbnormalEnbQci,pmErabRelNormalEnbQci,pmErabRelMmeQci)) as KPI_VALUE " +  
			"FROM $tablename " + 
			"WHERE UTC_DATETIME_ID = '$datestamp' " +
			"and OSS_ID = '$OSS' " +
			"and DCVECTOR_INDEX=1 $ERBSCLAUSE";
	
	public static String SQL_Skeleton_KPIB = "select NUMERATOR.ERBS, NUMERATOR.DATETIME_ID, NUMERATOR.OSS_ID, 100*AVG(numerator/denominator) as KPI_VALUE " + 
			"from( " + 
			"	$NUMERATOR" + 
			") as NUMERATOR, " + 
			"(        " + 
			"	$DENOMINATOR" + 
			") as DENOMINATOR " + 
			"where NUMERATOR.ERBS=DENOMINATOR.ERBS  " + 
			"and NUMERATOR.OSS_ID=DENOMINATOR.OSS_ID " + 
			"and NUMERATOR.cell=DENOMINATOR.cell " + 
			"and NUMERATOR.DATETIME_ID=DENOMINATOR.DATETIME_ID " + 
			"and NUMERATOR.UTC_DATETIME_ID = '$datestamp' " + 
			"and NUMERATOR.OSS_ID = '$OSS' " + 
			"group by NUMERATOR.ERBS, NUMERATOR.OSS_ID, NUMERATOR.DATETIME_ID";

	public static String SQL_Query_KPI_32_NUMERATOR = "select ERBS, DATETIME_ID, UTC_DATETIME_ID, OSS_ID, $cell as cell, sum(pmPdcpInactSecDlVolteDistr) as numerator " + 
			"from $tablename " + 
			"$ERBSCLAUSE and DCVECTOR_INDEX IN (1,2) " + 
			"group by ERBS, cell, DATETIME_ID, OSS_ID, UTC_DATETIME_ID";
	
	public static String SQL_Query_KPI_32_DENOMINATOR = "select ERBS, DATETIME_ID, UTC_DATETIME_ID, OSS_ID, $cell as cell, sum(pmPdcpInactSecDlVolteDistr) as denominator " + 
			"from $tablename  " + 
			"$ERBSCLAUSE" + 
			"group by ERBS, cell, DATETIME_ID, OSS_ID, UTC_DATETIME_ID";
	
	public static String SQL_Query_KPI_33_NUMERATOR = "select ERBS, DATETIME_ID, UTC_DATETIME_ID, OSS_ID, $cell as cell, sum(pmPdcpInactSecUlVolteDistr) as numerator " + 
			"from $tablename " + 
			"$ERBSCLAUSE and DCVECTOR_INDEX IN (1,2)" + 
			"group by ERBS, cell, DATETIME_ID, OSS_ID, UTC_DATETIME_ID";
	
	public static String SQL_Query_KPI_33_DENOMINATOR = "select ERBS, DATETIME_ID, UTC_DATETIME_ID, OSS_ID, $cell as cell, sum(pmPdcpInactSecUlVolteDistr) as denominator " + 
			"from $tablename " + 
			"$ERBSCLAUSE" + 
			"group by ERBS, cell, DATETIME_ID, OSS_ID, UTC_DATETIME_ID";
	
}
