#!/bin/ksh
#set -x
####################################################################################################
# timtest.ksh  script to test run SP_L_AIR_TICKETS()                                               #
#                                                                                                  #
# Parameter 1   :   filename                                                                       #
#                                                                                                  #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 11/11/2019    TWilson           Original                                                         #
#                                                                                                  #
####################################################################################################
. /Oracle/SECTOR/bin/set_ora_env.ksh
#
PATH=$PATH:$ORACLE_HOME/bin:/opt/bin:/usr/ccs/bin
export PATH
LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/java/lib ; export LD_LIBRARY_PATH
#
# set up directory variables
#
. /Oracle/SECTOR/scripts/stella/set_paths.ksh

filename=${1}

echo "Load file ${filename}"

sqlldr ${dbuser}/${dbpass} control=${ctl_path}/air.ctl data=${data_path}/spec_airfiles/${filename} log=${load_log_path}/${filename}.log

echo "Truncate table INTEGRATION_ERRORS"

echo "TRUNCATE TABLE STELLA.INTEGRATION_ERRORS;"|sqlplus -s $dbuser/$dbpass

echo "Truncate table L_AIR_EMD"

echo "TRUNCATE TABLE STELLA.L_AIR_EMD;"|sqlplus -s $dbuser/$dbpass

echo "Run SP_L_AIR_TICKETS() for file ${filename}"

#echo "EXEC STELLA.SP_L_AIR_TICKETS('${filename}');" | sqlplus -S -L ${dbuser}/${dbpass}

sqlplus -S -L ${dbuser}/${dbpass} << eof_marker
  set serveroutput on size 1000000

  set linesize 200

  BEGIN
    STELLA.SP_L_AIR_TICKETS('${filename}');
  END;

/

EXIT
eof_marker

# Or instead of the BEGIN...END.../ then use EXEC STELLA.SP_L_AIR_TICKETS('${filename}');

