def make_sql_string (BA,ubi_list):
    """returns SQL strings needed to execute the Queries"""
    # make TSO code strings
    if BA == 'ALL':
        TSO_str = "blk.TSO_CODE in('TBA','TCH','TSG','TPL','TKA','TKO','TPG','TST','TSK','TSN','TOC','TCC')"

    else:
        TSO_str = "blk.TSO_CODE = '{ba}'".format(ba = BA)


    # Split the UBIs list into chunks (SQL doesn't support more than 1000 entries)
    # create chunks of size 999
    n = 999
    array = [ubi_list[i:i + n] for i in range(0, len(ubi_list), n)]

    #Construct SQL string
    initial_string = "AND ("
    middle_string  = ''

    for i, value in enumerate (array):
        joined = '(' + ','.join("'" + j + "'"  for j in value) + ')'
        add_string = 'blk.UBI in ' + str(joined)

        if i < len(array)-1:
            add_string = add_string + ' OR '

        middle_string += add_string

    ubi_str = initial_string + middle_string +  ')'

    str_list = [TSO_str, ubi_str]

    return str_list



def build_sql_query (str_list):
    """Returns a dictionnary containing the SQL queries to be executed by the tool"""
    query_dict = {}

    query_dict['PR_DIPTI_$spent_noDvsDone']  ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE, ROUND (blk.CRUISE_VOL,2) AS CRUISE_VOLUME,
               cut.SPATIAL_FLAG AS SHAPE
        FROM (FORESTVIEW.V_BLOCK_SPATIAL cut INNER JOIN FORESTVIEW.V_BLOCK blk ON blk.CUTB_SEQ_NBR = cut.CUTB_SEQ_NBR)
                                         INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act ON act.CUTB_SEQ_NBR = cut.CUTB_SEQ_NBR
        WHERE {tso}
        	 AND (act.ACTIVITY_TYPE = 'Development Started' AND (act.ACTI_STATUS_IND <> 'D' OR act.ACTI_STATUS_IND is null))
             AND (act.CUTB_SEQ_NBR NOT IN (SELECT act.CUTB_SEQ_NBR
                                        FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act

    								   WHERE act.ACTIVITY_TYPE = 'Work in Progress' AND act.ACTI_STATUS_IND = 'D'
                                        OR act.ACTIVITY_TYPE = 'Write-off' AND act.ACTI_STATUS_IND = 'D'))
             {ubi}
        	 AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND (blk.CUTB_BLOCK_STATE <> 'X' OR blk.CUTB_BLOCK_STATE is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
  """ .format(ubi = str_list[1] , tso = str_list[0])

    query_dict['PR_DIPTI_$spent_noVolume']  ="""
        SELECT DISTINCT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID, blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, ROUND (blk.CRUISE_VOL,2) AS CRUISE_VOLUME

        FROM  FORESTVIEW.V_BLOCK blk
              INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act ON act.CUTB_SEQ_NBR = blk.CUTB_SEQ_NBR
        WHERE {tso}
        	 AND (blk.CRUISE_VOL is null OR blk.CRUISE_VOL <=1)
             {ubi}
        	 AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND (blk.CUTB_BLOCK_STATE <> 'X' OR blk.CUTB_BLOCK_STATE is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
  """ .format(ubi = str_list[1] , tso = str_list[0])

    query_dict['PR_DIPTI_dvsDone_noVolume']  ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE, ROUND (blk.CRUISE_VOL,2) AS CRUISE_VOLUME
        FROM FORESTVIEW.V_BLOCK blk
             INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
                     ON blk.CUTB_SEQ_NBR = act.CUTB_SEQ_NBR
        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Development Started' AND act.ACTI_STATUS_IND = 'D')
             AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND (blk.CRUISE_VOL is null OR blk.CRUISE_VOL <=1)
             AND (blk.CUTB_BLOCK_STATE <> 'X' OR blk.CUTB_BLOCK_STATE is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
      """.format(tso = str_list[0])

    query_dict['PR_DIPTI_dvsDone_noSpatial'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID, blk.BLOCK_NBR,blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE, cut.SPATIAL_FLAG AS SHAPE

        FROM (FORESTVIEW.V_BLOCK_SPATIAL cut INNER JOIN FORESTVIEW.V_BLOCK blk ON blk.CUTB_SEQ_NBR = cut.CUTB_SEQ_NBR)
                                         INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act ON act.CUTB_SEQ_NBR = cut.CUTB_SEQ_NBR
        WHERE {tso}
              AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
              AND cut.SPATIAL_FLAG ='NO'
              AND (act.ACTIVITY_TYPE = 'Development Started' AND act.ACTI_STATUS_IND = 'D')
        	  AND (act.CUTB_SEQ_NBR NOT IN (SELECT act.CUTB_SEQ_NBR
                                        FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act

    								    WHERE act.ACTIVITY_TYPE = 'Write-off'))
              AND (blk.CUTB_BLOCK_STATE <> 'X' OR blk.CUTB_BLOCK_STATE is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
      """.format(tso = str_list[0])

    query_dict['PR_DIPTI_dvsDone_noUBI'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR,blk.UBI, blk.TENURE,
        	   blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE
        FROM FORESTVIEW.V_BLOCK blk
        	 INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
        			 ON blk.CUTB_SEQ_NBR = act.CUTB_SEQ_NBR
        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Development Started' AND act.ACTI_STATUS_IND = 'D')
        	 AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND blk.UBI is null
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
      """.format(tso = str_list[0])

    query_dict['PR_DIPTI_dvsDone_noWoff-P'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy-mm-dd') AS DVS_D_DATE,
               EXTRACT (YEAR FROM ADD_MONTHS (act.ACTIVITY_DATE, 9)) AS DVS_D_FISCAL,
               null AS WOFF_P_DATE,
               null AS WOFF_P_FISCAL

        FROM FORESTVIEW.V_BLOCK blk
        	 INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
        			 ON blk.CUTB_SEQ_NBR = act.CUTB_SEQ_NBR

        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Development Started' AND act.ACTI_STATUS_IND = 'D')
             AND (blk.LICENCE_ID LIKE '%WO%' OR blk.LICENCE_ID LIKE '%Write%' OR blk.LICENCE_ID LIKE '%write%' OR blk.LICENCE_ID LIKE '%WRITE%')
             AND (act.CUTB_SEQ_NBR NOT IN (SELECT act.CUTB_SEQ_NBR
                                    FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act

								    WHERE (act.ACTIVITY_TYPE = 'Write-off' AND ACTI_STATUS_IND = 'P')
                                    OR (act.ACTIVITY_TYPE = 'Write-off' AND ACTI_STATUS_IND = 'D')
                                    ))


             AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND blk.CUTB_BLOCK_STATE <> 'X'
             AND blk.CUTB_BLOCK_STATE IS NOT null

       UNION ALL

        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy-mm-dd') AS DVS_D_DATE,
               EXTRACT (YEAR FROM ADD_MONTHS (act.ACTIVITY_DATE, 9)) AS DVS_D_FISCAL,
               TO_CHAR(woff.ACTIVITY_DATE, 'yyyy-mm-dd') AS WOFF_P_DATE,
               EXTRACT (YEAR FROM ADD_MONTHS (woff.ACTIVITY_DATE, 9)) AS WOFF_P_FISCAL

        FROM ((SELECT act2.ACTIVITY_DATE, act2.CUTB_SEQ_NBR FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act2
                  WHERE ACTIVITY_TYPE = 'Write-off' AND ACTI_STATUS_IND = 'P') woff
              LEFT JOIN FORESTVIEW.V_BLOCK blk ON blk.CUTB_SEQ_NBR = woff.CUTB_SEQ_NBR)
              INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act ON act.CUTB_SEQ_NBR = woff.CUTB_SEQ_NBR

        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Development Started' AND act.ACTI_STATUS_IND = 'D')
             AND (blk.LICENCE_ID LIKE '%WO%' OR blk.LICENCE_ID LIKE '%Write%' OR blk.LICENCE_ID LIKE '%write%' OR blk.LICENCE_ID LIKE '%WRITE%')
             AND woff.ACTIVITY_DATE < ADD_MONTHS(SYSDATE, 3)
             AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND blk.CUTB_BLOCK_STATE <> 'X'
             AND blk.CUTB_BLOCK_STATE IS NOT null
    """.format(tso = str_list[0])

    query_dict['PR_RTSTI_dvcDone_rcdrPlanned'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR,blk.UBI, blk.TENURE,
        	   blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE
        FROM FORESTVIEW.V_BLOCK blk
        	 INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
        			 ON blk.CUTB_SEQ_NBR = act.CUTB_SEQ_NBR
        WHERE {tso}
        AND (act.ACTIVITY_TYPE = 'Development Completed' AND act.ACTI_STATUS_IND = 'D')
        AND (act.CUTB_SEQ_NBR IN (SELECT act.CUTB_SEQ_NBR
                                  FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
                                  WHERE act.ACTIVITY_TYPE = 'Referral-Complete-Dev-Ready' AND act.ACTI_STATUS_IND = 'P'))
        	 AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
      """.format(tso = str_list[0])

    query_dict['PR_RCDRTI_rcdrDone_noSpatial'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID, blk.BLOCK_NBR,blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE, cut.SPATIAL_FLAG AS SHAPE

        FROM (FORESTVIEW.V_BLOCK_SPATIAL cut INNER JOIN FORESTVIEW.V_BLOCK blk ON blk.CUTB_SEQ_NBR = cut.CUTB_SEQ_NBR)
                                         INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act ON act.CUTB_SEQ_NBR = cut.CUTB_SEQ_NBR
        WHERE {tso}
              AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
              AND cut.SPATIAL_FLAG ='NO'
              AND (act.ACTIVITY_TYPE = 'Referral-Complete-Dev-Ready' AND act.ACTI_STATUS_IND = 'D'
                   AND act.ACTIVITY_DATE > to_timestamp('01-01-2012 00:00:00', 'dd-mm-yyyy hh24:mi:ss'))
              AND blk.BLOCK_ID NOT LIKE '%XXX%'
              AND blk.BLOCK_ID NOT LIKE '%ZZZ%'
        	  AND (act.CUTB_SEQ_NBR NOT IN (SELECT act.CUTB_SEQ_NBR
                                        FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act

    								    WHERE act.ACTIVITY_TYPE = 'Write-off'))
              AND (blk.CUTB_BLOCK_STATE NOT IN ('X', 'FG') OR blk.CUTB_BLOCK_STATE is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
      """.format(tso = str_list[0])

    query_dict['PR_RCDRTI_rcdrDone_noVolume'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy/MM/dd') ACTIVITY_DATE, ROUND (blk.CRUISE_VOL,2) AS CRUISE_VOLUME
        FROM FORESTVIEW.V_BLOCK blk
             INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
                     ON blk.CUTB_SEQ_NBR = act.CUTB_SEQ_NBR
        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Referral-Complete-Dev-Ready' AND act.ACTI_STATUS_IND = 'D'
                   AND act.ACTIVITY_DATE > to_timestamp('01-01-2012 00:00:00', 'dd-mm-yyyy hh24:mi:ss'))
             AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND (blk.CRUISE_VOL is null OR blk.CRUISE_VOL <=1)
             AND (blk.CUTB_BLOCK_STATE NOT IN ('X', 'FG', 'GU') OR blk.CUTB_BLOCK_STATE is null)
        ORDER BY blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID
      """.format(tso = str_list[0])

    query_dict['PR_RCDRTI_rcdrDone_noWoff-P'] ="""
        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy-mm-dd') AS RCDR_DATE,
               EXTRACT (YEAR FROM ADD_MONTHS (act.ACTIVITY_DATE, 9)) AS RCDR_FISCAL,
               null AS WOFF_P_DATE,
               null AS WOFF_P_FISCAL

        FROM FORESTVIEW.V_BLOCK blk
        	 INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act
        			 ON blk.CUTB_SEQ_NBR = act.CUTB_SEQ_NBR

        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Referral-Complete-Dev-Ready' AND act.ACTI_STATUS_IND = 'D')
             AND (blk.LICENCE_ID LIKE '%WO%' OR blk.LICENCE_ID LIKE '%Write%' OR blk.LICENCE_ID LIKE '%write%' OR blk.LICENCE_ID LIKE '%WRITE%')
             AND (act.CUTB_SEQ_NBR NOT IN (SELECT act.CUTB_SEQ_NBR
                                    FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act

								    WHERE (act.ACTIVITY_TYPE = 'Write-off' AND ACTI_STATUS_IND = 'P')
                                    OR (act.ACTIVITY_TYPE = 'Write-off' AND ACTI_STATUS_IND = 'D')
                                    ))


             AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND blk.CUTB_BLOCK_STATE <> 'X'
             AND blk.CUTB_BLOCK_STATE IS NOT null

       UNION ALL

        SELECT blk.TSO_CODE, blk.NAV_NAME, blk.LICENCE_ID, blk.BLOCK_ID,blk.BLOCK_NBR, blk.UBI, blk.TENURE,
               blk.CUTB_BLOCK_STATE BLOCK_STATE, act.ACTIVITY_TYPE, act.ACTI_STATUS_IND ACTIVITY_STATUS,
               TO_CHAR(act.ACTIVITY_DATE, 'yyyy-mm-dd') AS RCDR_DATE,
               EXTRACT (YEAR FROM ADD_MONTHS (act.ACTIVITY_DATE, 9)) AS RCDR_FISCAL,
               TO_CHAR(woff.ACTIVITY_DATE, 'yyyy-mm-dd') AS WOFF_P_DATE,
               EXTRACT (YEAR FROM ADD_MONTHS (woff.ACTIVITY_DATE, 9)) AS WOFF_P_FISCAL

        FROM ((SELECT act2.ACTIVITY_DATE, act2.CUTB_SEQ_NBR FROM FORESTVIEW.V_BLOCK_ACTIVITY_ALL act2
                 WHERE ACTIVITY_TYPE = 'Write-off' AND ACTI_STATUS_IND = 'P') woff
              LEFT JOIN FORESTVIEW.V_BLOCK blk ON blk.CUTB_SEQ_NBR = woff.CUTB_SEQ_NBR)
              INNER JOIN FORESTVIEW.V_BLOCK_ACTIVITY_ALL act ON act.CUTB_SEQ_NBR = woff.CUTB_SEQ_NBR

        WHERE {tso}
             AND (act.ACTIVITY_TYPE = 'Referral-Complete-Dev-Ready' AND act.ACTI_STATUS_IND = 'D')
             AND (blk.LICENCE_ID LIKE '%WO%' OR blk.LICENCE_ID LIKE '%Write%' OR blk.LICENCE_ID LIKE '%write%' OR blk.LICENCE_ID LIKE '%WRITE%')
             AND woff.ACTIVITY_DATE < ADD_MONTHS(SYSDATE, 3)
             AND (blk.TENURE = 'B20' OR  blk.TENURE  is null)
             AND blk.CUTB_BLOCK_STATE <> 'X'
             AND blk.CUTB_BLOCK_STATE IS NOT null
    """.format(tso = str_list[0])

    return query_dict
