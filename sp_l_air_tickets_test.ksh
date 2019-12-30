#!/bin/ksh
#set -x
####################################################################################################
# sp_l_air_tickets_test.ksh  script to run SP_L_AIR_TICKETS_TEST()                                 #
#                                                                                                  #
# Parameter 1   :   filename (file must be in ${data_path}/airfiles directory)                     #
#                                                                                                  #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 23/12/2019    TWilson           Original                                                         #
# 30/12/2019    TWilson           Revise for SP_L_AIR_TICKETS() now having a testflag parameter    #
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

sqlldr ${dbuser}/${dbpass} control=${ctl_path}/air.ctl data=${data_path}/airfiles/${filename} log=${load_log_path}/${filename}_test.log 

echo "Truncate table INTEGRATION_ERRORS"

echo "TRUNCATE TABLE STELLA.INTEGRATION_ERRORS;"|sqlplus -s $dbuser/$dbpass

echo "Truncate table L_AIR_EMD"

echo "TRUNCATE TABLE STELLA.L_AIR_EMD;"|sqlplus -s $dbuser/$dbpass

echo "Run SP_L_AIR_TICKETS() for file ${filename}"

sqlplus -S -L ${dbuser}/${dbpass} << eof_marker
  set serveroutput on size 1000000

  set linesize 200

  BEGIN  
    STELLA.SP_L_AIR_TICKETS('${filename}','TEST');
  END;

/

EXIT
eof_marker

#
