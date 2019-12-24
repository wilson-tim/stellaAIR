CREATE OR REPLACE PROCEDURE STELLA.sp_l_air_tickets (fileToProcess VARCHAR2) AUTHID CURRENT_USER AS 
-- DECLARE /*****  D E C L A R E  S E C T I O N  *****/
--===========================================================
-- sp_l_air_tickets - which takes the contents of AIR files
-- and populates the ticket table
--
--===========================================================
-- Change History
--===========================================================
-- Date          Ver Who Comment
-- 25-Oct-19     1.0 SMartin  Original Version
-- 11-Nov-19     1.1 TWilson  Continued Steve's development
-- 27-Nov-19     1.2 TWilson  T-,FM,FT,FO,FP,RM processing
-- 28-Nov-19     1.3 TWilson  insert_ticket processing
-- 03-Dec-19     1.4 TWilson  EMD data processing
-- 05-Dec-19     1.5 TWilson  EMD data processing debugging
--
-- @"I:\MI\Data Warehouse\Tim\MI-2672\SP_L_AIR_TICKETS_1.1.prc";
--
-- $ ./timtest.ksh spec0497000.AIR
--
-- Example of AIR file with EMD data
-- $ ./timtest.ksh spec0497014.AIR
--
--===========================================================
-- EXCEPTION DEFINITIONS
--
primary_key_error EXCEPTION;
PRAGMA EXCEPTION_INIT(primary_key_error, -00001);
foreign_key_error EXCEPTION;
PRAGMA EXCEPTION_INIT(foreign_key_error, -02291);
already_loaded    EXCEPTION;
no_booking_ref    EXCEPTION;
ticket_error      EXCEPTION;
consistency_error EXCEPTION;
emd_error         EXCEPTION;
--
--
v_commit_at CONSTANT PLS_INTEGER := 1000;
--
-- File, passenger and ticket data variables
v_ticket_no               ticket.ticket_no%TYPE;
v_airline_num             ticket.airline_num%TYPE;
v_iata_num                ticket.iata_no%TYPE;
v_entry_date              ticket.entry_date%TYPE:=TRUNC(SYSDATE);
v_e_ticket_ind            ticket.e_ticket_ind%TYPE:='Y';
v_source_ind              ticket.source_ind%TYPE:='AL';
v_entry_user_id           ticket.entry_user_id%TYPE:='AL';
v_ticket_issue_date       ticket.ticket_issue_date%TYPE;
v_passenger_name          ticket.passenger_name%TYPE;
v_passenger_type          ticket.passenger_type%TYPE;
v_num_pax                 ticket.num_pax%TYPE:=0;
v_published_fare_amt      ticket.published_fare_amt%TYPE:=0;
v_selling_fare_amt        ticket.selling_fare_amt%TYPE:=0;
v_remaining_tax_amt       ticket.remaining_tax_amt%TYPE:=0;
v_commission_pct          ticket.commission_pct%TYPE:=0;
v_commission_amt          ticket.commission_amt%TYPE:=0;
v_collection_amt          ticket.published_fare_amt%TYPE:=0;
v_ccy_code                ticket.ccy_code%TYPE;
v_pseudocitycode          ticket.pseudo_city_code%TYPE;
v_departure_date          ticket.departure_date%TYPE;
v_season                  VARCHAR(3);
v_pnr_no                  pnr.pnr_no%TYPE;
v_pnr_date                DATE;
v_gb_tax_amt              ticket.gb_tax_amt%TYPE:=0;
v_ub_tax_amt              ticket.ub_tax_amt%TYPE:=0;
v_ticket_type             ticket.ticket_type%TYPE;
v_ticket_agent            ticket.ticketing_agent_initials%TYPE;
v_remaining_taxes         ticket.remaining_taxes%TYPE;
v_doc_type_code           ticket.doc_type_code%TYPE:='AMA';
v_tour_code               ticket.tour_code%TYPE;
v_fare_basis_code         ticket.fare_basis_code%TYPE;
v_conjunction_ticket_ind  ticket.conjunction_ticket_ind%TYPE:='N';
v_airline_code            airline.airline_code%TYPE;
v_other_taxes             ticket.remaining_taxes%TYPE;
v_total_additional_tax    ticket.remaining_tax_amt%TYPE:=0;
v_group                   VARCHAR2(10):='';
v_booking_ref             VARCHAR2(10):='1';
v_branch_code             VARCHAR2(10):='AAIR';
--
-- EMD data variables
e_record_no               l_air_emd.record_no%TYPE:=0;
e_doc_id                  l_air_emd.doc_id%TYPE;
e_selling_fare_amt        l_air_emd.selling_fare_amt%TYPE;
e_remaining_tax_amt       l_air_emd.remaining_tax_amt%TYPE;
e_airline_num             l_air_emd.airline_num%TYPE;
e_ticket_no               l_air_emd.ticket_no%TYPE;
e_group                   l_air_emd.group_code%TYPE;
e_booking_ref             l_air_emd.booking_ref%TYPE;
--
-- Work Variables
v_null                    CHAR(1);
v_count                   CHAR(1);
v_insert_ok               CHAR(1);
v_update_ok               CHAR(1);
v_fk_error                CHAR(1);
v_foreign_key_error       CHAR(1);
v_primary_key_error       CHAR(1);
v_result                  VARCHAR2(200):='';
--
v_code                    NUMBER(5);
v_error_message           VARCHAR2(512);
v_stage                   VARCHAR2(100);
v_void                    BOOLEAN;
v_first_depdate           BOOLEAN;
v_exchange_found          BOOLEAN;
v_rfd_found               BOOLEAN;
v_tax_found               BOOLEAN;
v_unpaid_tax_found        BOOLEAN;
v_endfile_found           NUMBER;
v_insert_passenger        BOOLEAN;
v_passenger_no            VARCHAR2(10);
v_emd_found               NUMBER;
v_tax_amt                 VARCHAR2(10);
v_chunk                   VARCHAR2(200);
v_poscnt                  NUMBER;
v_pos1                    NUMBER;
v_pos2                    NUMBER;
v_pos12                   NUMBER;
v_pos12n                  NUMBER;
v_pos13                   NUMBER;
v_gbpos                   NUMBER;
v_ubpos                   NUMBER;
v_last_ticket_no          ticket.ticket_no%TYPE;
v_num_conj_tkts           NUMBER;
v_max_conj_ticket_no      ticket.ticket_no%TYPE;
v_num_exchconj_tkts       NUMBER;
v_exch_ticket_no          ticket.ticket_no%TYPE;
v_max_exchconj_ticket_no  ticket.ticket_no%TYPE;
v_fo_record               VARCHAR2(100);
v_fprefs                  VARCHAR2(100);
v_rmrefs                  VARCHAR2(100);
v_inside_rm               BOOLEAN:=False;
v_insertorupdate          VARCHAR2(1);
v_num_tkts                NUMBER;
v_ins_ticket_no           ticket.ticket_no%TYPE;
v_linked_ticket_no        ticket.ticket_no%TYPE;
v_num_inserted_rows       NUMBER:=0;
v_num_inserted_tkts       NUMBER:=0;
v_num_emd                 NUMBER:=0;
v_temp_string             VARCHAR(100);
v_bkgref_consistent       BOOLEAN:=False;
v_bkgref_check            VARCHAR2(10):='';
--
v_ins_published_fare_amt  ticket.published_fare_amt%TYPE:=0;
v_ins_selling_fare_amt    ticket.selling_fare_amt%TYPE:=0;
v_ins_gb_tax_amt          ticket.gb_tax_amt%TYPE:=0;
v_ins_ub_tax_amt          ticket.ub_tax_amt%TYPE:=0;
v_ins_remaining_tax_amt   ticket.remaining_taxes%TYPE:=0;
v_ins_other_taxes         ticket.remaining_taxes%TYPE;
v_ins_commission_amt      ticket.commission_amt%TYPE:=0;
v_ins_commission_pct      ticket.commission_pct%TYPE:=0;
v_ins_exch_ticket_no      ticket.ticket_no%TYPE;
--
CURSOR C001 IS
  SELECT data_text       
  FROM l_air
  ORDER BY sequence_no;
  --
BEGIN
  /**** < OPEN Cursor Block > ****/
  --
   v_primary_key_error    := 'N';
   v_foreign_key_error    := 'N';
   v_void                 := False;
   v_first_depdate        := False;
   v_exchange_found       := False;
   v_rfd_found            := False;      
   v_tax_found            := False;     
   v_unpaid_tax_found     := False;     
   v_endfile_found        := 0;
   v_insert_passenger     := False;
   v_emd_found            := 0;

   v_tax_amt := '';

   -- Prepare for EMD data   
   EXECUTE IMMEDIATE 'TRUNCATE TABLE L_AIR_EMD';

   BEGIN
      SELECT 1
      INTO   v_emd_found
      FROM   l_air 
      WHERE  (SUBSTR(data_text,1,3) = 'EMD' 
      OR     SUBSTR(data_text,1,4) = 'TMCD')
      AND ROWNUM = 1
      ;
   EXCEPTION
   WHEN NO_DATA_FOUND THEN
        NULL;
   END;  

-- Check that file is correctly terminated
   BEGIN
      SELECT 1
      INTO   v_endfile_found
      FROM   l_air 
      WHERE  SUBSTR(data_text,1,3) = 'END' 
      OR     SUBSTR(data_text,1,4) = 'ENDX';
   EXCEPTION
   WHEN NO_DATA_FOUND THEN
        core_dataw.sp_errors('AIR_TICKET','FILE',-20000, 'File: ' || fileToProcess || ', missing END of file indicator');
        RAISE;
   END;  

-- First pass (initial checks)
   FOR c1_rec IN c001 LOOP

-- Is this an exchange ticket?
     IF SUBSTR(c1_rec.data_text,1,2) = 'FO' THEN
          v_exchange_found := True;
     ELSE 
          v_exchange_found := False;
     END IF;

/*
* This code is added here so that when reading through fare
* records (K-R, K-Y etc , check can be done to match on
* additional fare Form of Payment eg FPO/NONREF
* AGT+/CASH/GBP731.00 or FPO/NONREF
* AGT+/CASH/GBP100.00;S2-5;P1
* Ver 1.1 : FPO line received as FPO/NONREF AGT+/NONREF AGT/GBP50.00  , code could not read \\uFFFD50 as no /CASH in it.
* Fixed by searching for /GBP rather CASH/
*/
    IF  SUBSTR(c1_rec.data_text,1,2) = 'FPO' AND v_exchange_found THEN
        -- Form of payment data for original issue / exchange
        BEGIN
            v_collection_amt := CASE WHEN INSTR(c1_rec.data_text,'/GBP') > 0 
                                      AND INSTR(c1_rec.data_text,';')    > 0 THEN
                                          TO_NUMBER(SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,'/GBP')+4,
                                                    (INSTR(c1_rec.data_text,';') -1) -
                                                     INSTR(c1_rec.data_text,'/GBP')+4
                                                    ))
                                     WHEN INSTR(c1_rec.data_text,'/GBP') > 0 
                                      AND INSTR(c1_rec.data_text,';')    = 0 THEN
                                          TO_NUMBER(SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,'/GBP')+4
                                                    ))               
                                     ELSE 0               
                                END ;
        EXCEPTION
        WHEN OTHERS THEN 
            core_dataw.sp_errors('AIR_TICKET','FPO',SQLCODE,'Collection amount ' || SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,'/GBP')+4)|| SQLERRM);
            v_collection_amt := 0;
        END;  
    END IF;

-- Is this a refund ticket?
    IF  SUBSTR(c1_rec.data_text,1,3) = 'RFD' THEN
        v_rfd_found := True;
    ELSE 
        v_rfd_found := False;
    END IF;

  END LOOP;

-- Second pass (file header data)
dbms_output.put_line('Collection amt - ' || TO_CHAR(v_collection_amt));
  FOR c1_rec IN c001 LOOP
     IF  SUBSTR(c1_rec.data_text,1,4) = 'AIR-' THEN
        -- File version and type
dbms_output.put_line(c1_rec.data_text);
        IF SUBSTR(c1_rec.data_text,12,2)= 'MA' THEN
dbms_output.put_line('Void Ticket - True'); 
           v_void := True; --insert blanks for this type of ticket
        ELSE
dbms_output.put_line('Void Ticket - False'); 
           v_void := False;  
        END IF;              

     ELSIF SUBSTR(c1_rec.data_text,1,5) = 'MUC1A' THEN
dbms_output.put_line(c1_rec.data_text);
        v_pnr_no := SUBSTR(c1_rec.data_text,7,6);
dbms_output.put_line('PNR - ' || TO_CHAR(v_pnr_no));        
        v_iata_num := SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,';',1,9)+1,8);
dbms_output.put_line('IATA - ' || v_iata_num);
        BEGIN
            v_pseudocitycode := CASE WHEN INSTR(c1_rec.data_text,'LONFC') >0 THEN
                                          SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,'LONFC')+5,4)
                                     WHEN INSTR(c1_rec.data_text,'LONSH') >0 THEN
                                          SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,'LONSH')+5,4)
                                     WHEN INSTR(c1_rec.data_text,'LONVM') >0 THEN
                                          SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,'LONVM')+5,4)
                                     ELSE NULL
                                     END;         
        EXCEPTION 
        WHEN OTHERS THEN 
            core_dataw.sp_errors('AIR_TICKET','MUC1A',SQLCODE,'Pseudo city code ' || SUBSTR(c1_rec.data_text,1,50) || ' - ' || SQLERRM);
            RAISE;
        END;
dbms_output.put_line('Pseudo City - ' || v_pseudocitycode);

     ELSIF NOT v_void THEN
        IF SUBSTR(c1_rec.data_text,1,2) = 'C-' THEN
            -- Carrier details
dbms_output.put_line(c1_rec.data_text);
            v_ticket_agent := SUBSTR(c1_rec.data_text,22,2);
dbms_output.put_line('TicketAgent ' || v_ticket_agent);

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'D-' THEN
            -- Dates for PNR creation, modification, and A.I.R. creation
dbms_output.put_line(c1_rec.data_text);
            BEGIN
                v_ticket_issue_date := TO_DATE(
                                           SUBSTR(c1_rec.data_text,17,6)
                                          ,'RRMMDD');
            EXCEPTION
            WHEN OTHERS THEN
                core_dataw.sp_errors('AIR_TICKET','D-',SQLCODE,'Ticket issue date ' || SUBSTR(c1_rec.data_text,17,6) || ' - ' || SQLERRM);
                RAISE;
            END;
dbms_output.put_line('Ticket Date - ' || TO_CHAR(v_ticket_issue_date,'DD-MON-YYYY'));
            BEGIN
                v_pnr_date          := TO_DATE(
                                           SUBSTR(c1_rec.data_text,3,6)
                                          ,'RRMMDD');  
            EXCEPTION
            WHEN OTHERS THEN
                core_dataw.sp_errors('AIR_TICKET','D-',SQLCODE,'PNR date ' || SUBSTR(c1_rec.data_text,3,6) || ' - ' || SQLERRM);
                RAISE;
            END;
dbms_output.put_line('PNR Creation Date - ' || TO_CHAR(v_ticket_issue_date,'DD-MON-YYYY'));

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'H-' AND NOT v_first_depdate THEN
            -- Ticketed segments: flight, surface, open, rail
dbms_output.put_line(c1_rec.data_text);
            BEGIN
-- DDMON format
            v_departure_date := TO_DATE(SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,';',1,5)+16,5)||TO_CHAR(v_ticket_issue_date,'YYYY'),
                                    'DDMONYYYY');
dbms_output.put_line('Departure Date     - ' || TO_CHAR(v_departure_date,'DD-MON-YYYY'));

-- If ticket issue date month is > departure date month then departure is next year of issue date so increment year
                IF TO_NUMBER(TO_CHAR(v_ticket_issue_date,'MM')) > 0
                    AND TO_NUMBER(TO_CHAR(v_ticket_issue_date,'MM')) > TO_NUMBER(TO_CHAR(v_departure_date,'MM')) THEN
                    v_departure_date := TO_DATE(SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,';',1,5)+16,5)||
                                             TO_CHAR(TO_NUMBER(TO_CHAR(v_ticket_issue_date,'YYYY')) + 1),
                                             'DDMONYYYY');

                END IF;
            EXCEPTION
            WHEN OTHERS THEN 
                core_dataw.sp_errors('AIR_TICKET','H-',SQLCODE, 'Departure date ' || SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,';',1,5)+16,5) || ' - ' || SQLERRM);
                RAISE;
            END;

            v_first_depdate := True;           
dbms_output.put_line('Departure Date Adj - ' || TO_CHAR(v_departure_date,'DD-MON-YYYY'));

            BEGIN
                v_season := CASE WHEN TO_NUMBER(TO_CHAR(v_departure_date,'MM')) < 4
                                        THEN 'W' || TO_CHAR(TO_NUMBER(TO_CHAR(v_departure_date,'YY')) - 1) -- winter of previous year
                                    WHEN TO_NUMBER(TO_CHAR(v_departure_date,'MM')) >= 4 
                                        AND TO_NUMBER(TO_CHAR(v_departure_date,'MM')) <= 9
                                        THEN 'S' || TO_CHAR(v_departure_date,'YY') -- summer of current year
                                    WHEN TO_NUMBER(TO_CHAR(v_departure_date,'MM')) > 9
                                        THEN 'W' || TO_CHAR(v_departure_date,'YY') -- winter of current year
                                    END;
            EXCEPTION 
            WHEN OTHERS THEN 
                core_dataw.sp_errors('AIR_TICKET','H-',SQLCODE,'Season ' || v_season || ' (Departure date ' || v_departure_date || ') - ' || SQLERRM);
                RAISE;
            END;
dbms_output.put_line('Season - ' || v_season);

/*
 * K- record published fare In case of 1) Published
 * fare than K-F amt = P.F = S.F calculate
 * commission 2) Nett Remitt than K-F amt = P.F. and
 * KN-F amt = S.F 3) BT/IT K.I or K-B amt = S.F and
 * P.F = 0 , commission = 0 4) CAT 35 (only BT/IT)
 * KN-I or KN-B amt = S.F , P.F = 0 , This type of
 * fare follows BT/IT provided it does not include
 * net remitt. For exchane tickets for Published
 * Fare K-F comes as k-R (reissue) K-I comes as K-Y
 * KN-F as KN-R K-B as K-W KN-I as KN-Y and KS-I as
 * KS-Y
 *
*/
/*
 * Per the Amadeus Reference Guide:
 *  
 * K and KFT lines: Base fare, total fare and taxes.
 * KN and KNT lines: Net fare and taxes.
 * KS and KST lines: Selling fare and taxes.
 * KRF line: Refundable tax data.
*/
        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'K-' OR SUBSTR(c1_rec.data_text,1,3) = 'KN-' OR SUBSTR(c1_rec.data_text,1,3) = 'KS-' THEN
            -- Base, total, net and selling fare data
dbms_output.put_line(c1_rec.data_text);  
            IF SUBSTR(c1_rec.data_text,1,2) = 'K-' AND INSTR(c1_rec.data_text,';',1,12)+1 > 12 THEN
                v_ccy_code := SUBSTR(c1_rec.data_text,INSTR(c1_rec.data_text,';',1,12)+1,3);
            END IF;
dbms_output.put_line('Currency Code - ' || v_ccy_code);  
            IF SUBSTR(c1_rec.data_text,1,3) = 'K-F' THEN
                v_published_fare_amt := FN_POPULATEFARE(SUBSTR(c1_rec.data_text,1,3),c1_rec.data_text);  
                v_selling_fare_amt := v_published_fare_amt;
            ELSIF SUBSTR(c1_rec.data_text,1,4) = 'KN-F' THEN
                v_selling_fare_amt :=  FN_POPULATEFARE(SUBSTR(c1_rec.data_text,1,4),c1_rec.data_text);
            ELSIF SUBSTR(c1_rec.data_text,1,3) = 'K-I' OR SUBSTR(c1_rec.data_text,1,3) = 'K-B' THEN
                v_selling_fare_amt := FN_POPULATEFARE(SUBSTR(c1_rec.data_text,1,3),c1_rec.data_text);  
                v_published_fare_amt := 0;     
            ELSIF SUBSTR(c1_rec.data_text,1,4) = 'KN-I' OR SUBSTR(c1_rec.data_text,1,4) = 'KN-B' THEN
                v_selling_fare_amt := FN_POPULATEFARE(SUBSTR(c1_rec.data_text,1,4),c1_rec.data_text);  
                v_published_fare_amt := 0;     
/*
 * EXCHANGE / REISSUE TICKET FARE , WHEN THERE
 * IS A ADDITIONAL COLLECTION
 * IMPORTANT NOTES :
 * Additional collection in FPO line and K- line
 * are always represents same amount
 * Additional collection in fare = K- fare amt (
 * at the end of line ) - (sum of all tax
 * changes in KFT- line )
 * In other words , additional collection in
 * fare line (K-) might represents only tax
 * changes
 * so find out if there is any tax changes in
 * KFT- line than deduct that from fare , which
 * is actual fare changes
*/
            ELSIF SUBSTR(c1_rec.data_text,1,3) = 'K-R' THEN
                BEGIN
                   v_published_fare_amt :=  TO_NUMBER(
                                               SUBSTR(c1_rec.data_text,
                                               INSTR(c1_rec.data_text,';',1,12)+1,
                                               (INSTR(c1_rec.data_text,';',1,13)-INSTR(c1_rec.data_text,';',1,12))-2
                                                     )); 
                EXCEPTION
                WHEN OTHERS THEN 
                    core_dataw.sp_errors('AIR_TICKET','K-R',SQLCODE,SUBSTR(c1_rec.data_text,
                                                                    INSTR(c1_rec.data_text,';',1,12)+1,
                                                                 (INSTR(c1_rec.data_text,';',1,13)-INSTR(c1_rec.data_text,';',1,12))-2)
                                               || SQLERRM);
                    v_published_fare_amt := NULL;                              
                END;
                IF  v_published_fare_amt IS NOT NULL
                AND v_published_fare_amt != v_collection_amt  THEN
                    v_published_fare_amt := 0;
                END IF;    
                v_selling_fare_amt := v_published_fare_amt;
            --
            ELSIF SUBSTR(c1_rec.data_text,1,4) = 'KN-R' THEN
                BEGIN
                   v_selling_fare_amt :=  TO_NUMBER(
                                               SUBSTR(c1_rec.data_text,
                                               INSTR(c1_rec.data_text,';',1,12)+1,
                                               (INSTR(c1_rec.data_text,';',1,13)-INSTR(c1_rec.data_text,';',1,12))-2
                                                     )); 
                EXCEPTION
                WHEN OTHERS THEN 
                    core_dataw.sp_errors('AIR_TICKET','KN-R',SQLCODE,SUBSTR(c1_rec.data_text,
                                                                    INSTR(c1_rec.data_text,';',1,12)+1,
                                                                 (INSTR(c1_rec.data_text,';',1,13)-INSTR(c1_rec.data_text,';',1,12))-2)
                                               || SQLERRM);
                    v_selling_fare_amt := NULL;                              
                END;
                IF  v_selling_fare_amt IS NOT NULL
                AND v_selling_fare_amt != v_collection_amt  THEN
                    v_selling_fare_amt := 0;
                END IF;    
            --
            ELSIF (SUBSTR(c1_rec.data_text,1,3) IN ('K-Y','K-W')
                OR  SUBSTR(c1_rec.data_text,1,4) IN ('KN-Y','KN-W', 'KS-Y','KS-W')) THEN       
                v_published_fare_amt := 0;
                BEGIN
                    v_pos12 := INSTR(c1_rec.data_text,';',1,12);
                    v_pos12n := REGEXP_INSTR(c1_rec.data_text, '[0-9]', v_pos12, 1);
                    v_pos13 := INSTR(c1_rec.data_text,';',1,13);
                    v_selling_fare_amt :=  TO_NUMBER(SUBSTR(c1_rec.data_text,    -- from the first numeric after 12th ';' to the position before 13th ';'
                                               v_pos12n, v_pos13 - v_pos12n)); 
                EXCEPTION
                WHEN OTHERS THEN 
                    core_dataw.sp_errors('AIR_TICKET','K-Y,W;KN-Y,W;KS-Y,W',SQLCODE,
                        SUBSTR(c1_rec.data_text,    -- from the first numeric after 12th ';' to the position before 13th ';'
                                               v_pos12n, v_pos13 - v_pos12n)
                                               || SQLERRM);
                    v_selling_fare_amt := NULL;                              
                END;        
                IF  v_selling_fare_amt IS NOT NULL
                AND v_selling_fare_amt != v_collection_amt  THEN
                    v_selling_fare_amt := 0;
                END IF; 

            END IF;
dbms_output.put_line('Selling fare amt - ' || v_selling_fare_amt);  
dbms_output.put_line('Published fare amt - ' || v_published_fare_amt);  

        ELSIF  SUBSTR(c1_rec.data_text,1,3) = 'KFT' OR SUBSTR(c1_rec.data_text,1,3) = 'KNT' THEN
dbms_output.put_line(c1_rec.data_text);  
            IF NOT v_exchange_found THEN
                IF NOT v_tax_found THEN
dbms_output.put_line('Exchange and Tax both not found');
                    v_tax_found := True;
                    v_poscnt := REGEXP_COUNT(c1_rec.data_text, ';');
dbms_output.put_line('v_poscnt - ' || TO_CHAR(v_poscnt));

                    FOR i IN 1..v_poscnt - 1 LOOP

                        v_pos1 := INSTR(c1_rec.data_text, ';', 1, i);
                        v_pos2 := INSTR(c1_rec.data_text, ';', 1, i + 1);

dbms_output.put_line('i - ' || TO_CHAR(i) || ' v_pos1 - ' || TO_CHAR(v_pos1) || ' v_pos2 - ' || TO_CHAR(v_pos2));    
                        v_chunk := SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 -1);
dbms_output.put_line('v_chunk - ' || v_chunk);    
                        IF LENGTH(v_chunk) > 0 THEN
                            v_gbpos := INSTR(v_chunk, 'GB ', 1, 1);
                            v_ubpos := INSTR(v_chunk, 'UB ', 1, 1);
                            v_tax_amt := TRIM(SUBSTR(v_chunk, 5, LENGTH(v_chunk)-9));
                            v_tax_amt := REPLACE(v_tax_amt, 'EXEMPT', '0.00');
dbms_output.put_line('v_tax_amt - ' || v_tax_amt);    

                            IF v_gbpos > 0 THEN
                                v_gb_tax_amt := TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_gb_tax_amt - ' || v_tax_amt);    
                            END IF;

                            IF v_ubpos > 0 THEN
                                v_ub_tax_amt := TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_ub_tax_amt - ' || v_tax_amt);    
                            END IF;

                            IF v_gbpos <= 0 AND v_ubpos <= 0 THEN
                                v_remaining_tax_amt := v_remaining_tax_amt + TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_remaining_tax_amt - ' || v_remaining_tax_amt);
                                v_other_taxes := v_other_taxes || v_chunk || ';';
dbms_output.put_line('v_other_taxes - ' || v_other_taxes);
                            END IF;

                        END IF;

                    END LOOP;

dbms_output.put_line('v_gb_tax_amt - ' || v_gb_tax_amt);    
dbms_output.put_line('v_ub_tax_amt - ' || v_ub_tax_amt);    
dbms_output.put_line('v_other_taxes - ' || v_other_taxes);
dbms_output.put_line('v_remaining_tax_amt - ' || v_remaining_tax_amt);

                END IF;
            ELSE
                IF NOT v_unpaid_tax_found THEN
                    v_poscnt := REGEXP_COUNT(c1_rec.data_text, ';');
dbms_output.put_line('v_poscnt - ' || TO_CHAR(v_poscnt));

                    FOR i IN 1..v_poscnt - 1 LOOP

                        v_pos1 := INSTR(c1_rec.data_text, ';', 1, i);
                        v_pos2 := INSTR(c1_rec.data_text, ';', 1, i + 1);

dbms_output.put_line('i - ' || TO_CHAR(i) || ' v_pos1 - ' || TO_CHAR(v_pos1) || ' v_pos2 - ' || TO_CHAR(v_pos2));    
                        v_chunk := SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 -1);
dbms_output.put_line('v_chunk - ' || v_chunk);    
                        IF LENGTH(v_chunk) > 0 AND SUBSTR(v_chunk, 1, 1) != 'O' THEN
							v_unpaid_tax_found := True;

                            v_gbpos := INSTR(v_chunk, 'GB ', 1, 1);
                            v_ubpos := INSTR(v_chunk, 'UB ', 1, 1);
                            v_tax_amt := TRIM(SUBSTR(v_chunk, 5, LENGTH(v_chunk)-9));
                            v_tax_amt := REPLACE(v_tax_amt, 'EXEMPT', '0.00');
dbms_output.put_line('v_tax_amt - ' || v_tax_amt);    

							v_total_additional_tax := v_total_additional_tax + TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_total_additional_tax - ' || v_total_additional_tax);    

                            IF v_gbpos > 0 THEN
                                v_gb_tax_amt := TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_tax_amt - ' || v_tax_amt);    
                            END IF;

                            IF v_ubpos > 0 THEN
                                v_ub_tax_amt := TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_tax_amt - ' || v_tax_amt);    
                            END IF;

                            IF v_gbpos <= 0 AND v_ubpos <= 0 THEN
                                v_remaining_tax_amt := v_remaining_tax_amt + TO_NUMBER(v_tax_amt);
dbms_output.put_line('v_remaining_tax_amt - ' || v_remaining_tax_amt);
                                v_other_taxes := v_other_taxes || v_chunk || ';';
dbms_output.put_line('v_other_taxes - ' || v_other_taxes);
                            END IF;

                        END IF;

                    END LOOP;

dbms_output.put_line('v_gb_tax_amt - ' || v_gb_tax_amt);    
dbms_output.put_line('v_ub_tax_amt - ' || v_ub_tax_amt);    
dbms_output.put_line('v_other_taxes - ' || v_other_taxes);
dbms_output.put_line('v_remaining_tax_amt - ' || v_remaining_tax_amt);

                    IF v_published_fare_amt > 0 THEN
                        v_published_fare_amt := v_published_fare_amt - v_total_additional_tax;
dbms_output.put_line('Published fare amt - ' || v_published_fare_amt);  
                    END IF;

                    IF v_selling_fare_amt > 0 THEN
                        v_selling_fare_amt := v_selling_fare_amt - v_total_additional_tax;
dbms_output.put_line('Selling fare amt - ' || v_selling_fare_amt);  
                    END IF;

                END IF;

            END IF;

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'M-' THEN   
            -- Fare basis code
dbms_output.put_line(c1_rec.data_text);
            BEGIN
                IF INSTR(c1_rec.data_text,';') > 0 THEN
                    v_fare_basis_code := TRIM(SUBSTR(c1_rec.data_text,3,INSTR(c1_rec.data_text,';')-3));
                ELSE
                    v_fare_basis_code := TRIM(SUBSTR(c1_rec.data_text,3,LENGTH(c1_rec.data_text)-2));
                END IF;
            EXCEPTION
            WHEN OTHERS THEN
                core_dataw.sp_errors('AIR_TICKET','M-',SQLCODE,'Fare basis code ' || c1_rec.data_text || ' - ' || SQLERRM);
                RAISE;
            END;
dbms_output.put_line('Fare Basis - ' || v_fare_basis_code);

        ELSIF SUBSTR(c1_rec.data_text,1,3) = 'EMD' THEN
            -- Electronic miscellaneous document
            -- EMD071;007CX;1606;CATHAY PACIFIC;14NOV;D1;1;CX;;;;;TO-CATHAY PACIFIC...etc.
dbms_output.put_line(c1_rec.data_text);
            v_pos1 := INSTR(c1_rec.data_text, ';', 1, 5);
            v_pos2 := INSTR(c1_rec.data_text, ';', 1, 6);
            
            IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
                e_doc_id := SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 -1);
dbms_output.put_line('EMD doc id ' || e_doc_id);

                v_pos1 := INSTR(c1_rec.data_text, ';', 1, 28);
                v_pos2 := INSTR(c1_rec.data_text, ';', 1, 29);
                v_temp_string := '';
                
                IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
                    v_temp_string := SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 - 1);
                    v_temp_string := REGEXP_REPLACE(v_temp_string, '[A-Z[:space:]]','');
                    e_selling_fare_amt := TO_NUMBER(v_temp_string);
                ELSE
                    e_selling_fare_amt := 0;
                END IF;
                e_selling_fare_amt := NVL(e_selling_fare_amt, 0);
dbms_output.put_line('EMD selling fare ' || e_selling_fare_amt);
                
                v_pos1 := INSTR(c1_rec.data_text, ';', 1, 33);
                v_pos2 := INSTR(c1_rec.data_text, ';', 1, 34);
                v_temp_string := '';
                
                IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
                    v_temp_string := SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 - 1);
                    v_temp_string := REPLACE(v_temp_string, 'T-','');
                    v_temp_string := REGEXP_REPLACE(v_temp_string, '[A-Z[:space:]]','');
                    e_remaining_tax_amt := TO_NUMBER(v_temp_string);
                ELSE
                    e_remaining_tax_amt := 0;
                END IF;
                e_remaining_tax_amt := NVL(e_remaining_tax_amt, 0);
dbms_output.put_line('EMD remaining tax ' || e_remaining_tax_amt);

                BEGIN
                    v_num_emd := v_num_emd + 1;
                    
                    INSERT INTO L_AIR_EMD
                        (
                            record_no,
                            doc_id,
                            selling_fare_amt,
                            remaining_tax_amt
                        )
                    VALUES
                        (
                            v_num_emd,
                            e_doc_id,
                            e_selling_fare_amt,
                            e_remaining_tax_amt
                        )
                    ;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_error_message := 'ERROR inserting EMD data into table L_AIR_EMD';
                        RAISE emd_error;
                END;
                COMMIT;
                
            END IF;

        END IF;  -- ELSIF NOT v_void THEN

     END IF;

   END LOOP; 

-- Third pass (passengers, tickets, booking details, etc.)
   FOR c1_rec IN c001 LOOP
        IF (SUBSTR(c1_rec.data_text,1,2) = 'I-' OR SUBSTR(c1_rec.data_text,1,3) = 'END') THEN
            -- Passenger data to be inserted
            IF v_insert_passenger THEN
dbms_output.put_line('********************************************************************************');
dbms_output.put_line('Complete processing passenger ' || v_passenger_no || ' data');
/* Only need this section if performing validation checks below
                -- There may be EMD data present but not necessarily relating to the current passenger
                IF v_emd_found > 0 AND v_num_emd > 0 THEN
                    -- Use data from first EMD record for validation checks
                    IF v_ticket_no IS NULL THEN
                        SELECT ticket_no
                        INTO   v_ticket_no
                        FROM   l_air_emd
                        WHERE  record_no = 1;
                    END IF;
                    IF v_airline_num IS NULL THEN
                        SELECT airline_num
                        INTO   v_airline_num
                        FROM   l_air_emd
                        WHERE  record_no = 1;
                    END IF;
                    IF v_group IS NULL THEN
                        SELECT group_code
                        INTO   v_group
                        FROM   l_air_emd
                        WHERE  record_no = 1;
                    END IF;
                    IF v_booking_ref IS NULL THEN
                        SELECT booking_ref
                        INTO   v_booking_ref
                        FROM   l_air_emd
                        WHERE  record_no = 1;
                    END IF;
                END IF;
*/
                v_booking_ref := NVL(v_booking_ref, '0');
/*
                is ticket no numeric?

                is exchange ticket no numeric?

                is airline no numeric?

                is booking ref numeric?

                is commission amount numeric?

                is commission percent numeric?

                is selling fare numeric?

                is published fare numeric?

                is iata no numeric?

                is gb tax numeric?

                is remaining tax numeric?

                is ub tax numeric?
*/
                IF NOT v_void AND NOT v_exchange_found THEN
                    -- now derive commission amt or commission pct in case both
                    -- are not supplied
                    -- convert to numbers so can test contents and work out pct
                    -- if pct is not supplied,
                    -- and work out amt if amt is not supplied
                    IF v_published_fare_amt = 0 THEN
                        -- prevent divide by zero error
                        -- comm amt and pct must be 0
                        v_ins_commission_amt := 0;
                        v_ins_commission_pct := 0;
                    ELSE
                        IF v_commission_amt = 0 AND v_commission_pct != 0 THEN
                            -- calc amt from pct * published fare
                            v_ins_commission_amt := v_published_fare_amt * v_commission_pct / 100;
                        ELSIF v_commission_amt != 0 AND v_commission_pct = 0 THEN
                            -- calc pct from amt / published fare
                            v_ins_commission_pct := v_commission_amt / v_published_fare_amt * 100;
                        END IF;
                    END IF;
                    
                ELSIF v_void THEN
                    v_ins_published_fare_amt := 0;
                    v_ins_selling_fare_amt := 0;
                    v_ins_gb_tax_amt := 0;
                    v_ins_ub_tax_amt := 0;
                    v_ins_remaining_tax_amt := 0;
                    v_ins_other_taxes := '';
                    v_ins_commission_pct := 0;
                    v_ins_commission_amt := 0;
                    
                    v_departure_date := NULL;
                    v_passenger_name := 'VOID';
                    v_season := '';
                    v_branch_code := '';
                    v_ticket_agent := 'ZZ';
                    
                ELSIF v_exchange_found THEN
                    v_ins_commission_pct := 0;
                    v_ins_commission_amt := 0;
                END IF;
                
                IF v_num_pax = 1 THEN
                    v_insertorupdate := 'I';
                ELSE
                    v_insertorupdate := 'U';
                END IF;

                -- usually only have to insert one ticket, BUT sometimes, it may be a conjunctive AIR
                -- this means you have to insert the main ticket with all values obtained so far
                -- but then also insert some dummy tickets with zero amounts but on same pnr
                -- This is done because sometimes there are too many sectors to fit onto one ticket stub,
                -- Conjunctive ticket number are the one followed by - , first 8 digits
				-- will be same as main tkt only last two digits gets changed

                v_conjunction_ticket_ind := 'N';
                v_ins_ticket_no := v_ticket_no; -- This is original ticket for i= 1
                
                IF v_emd_found > 0 AND v_num_emd > 0 THEN
                    v_num_tkts := v_num_emd;
                ELSIF v_exchange_found THEN
                    v_num_tkts := v_num_exchconj_tkts;
                ELSE
                    v_num_tkts := v_num_conj_tkts;
                END IF;
                
                FOR i IN 1..v_num_tkts LOOP
                    -- i =1 does insert for main ticket

                    -- loop through each conjunctive ticket required and insert
                    -- a row for each one
                    -- ensure there is always at one least one ticket inserted
                    -- (the main one)
                    
                    IF i > 1 AND NOT (v_emd_found > 0 AND v_num_emd > 0) THEN
                        -- Processing the conjunctive "dummy" tickets
                        -- so reset amounts to 0 or NULL
                        v_ins_published_fare_amt := 0;
                        v_ins_selling_fare_amt := 0;
                        v_ins_gb_tax_amt := 0;
                        v_ins_ub_tax_amt := 0;
                        v_ins_remaining_tax_amt := 0;
                        v_ins_other_taxes := '';
                        v_ins_commission_pct := 0;
                        v_ins_commission_amt := 0;
                        
                        v_conjunction_ticket_ind := 'Y';

                        -- Get the next conjunction ticket number
						v_ins_ticket_no := v_ins_ticket_no + 1;
                        
                        IF v_exchange_found THEN
							v_ins_exch_ticket_no := v_exch_ticket_no;
                            -- Get the next exchange ticket number if this is an
                            -- exchange conjunction ticket
                            v_exch_ticket_no := v_exch_ticket_no + 1;
                        END IF;
                        
                    END IF;
                    
                    IF (v_emd_found > 0 AND v_num_emd > 0) THEN
						-- Processing EMD tickets populate values from next L_AIR_EMD record
                        SELECT selling_fare_amt, remaining_tax_amt, airline_num, ticket_no, group_code, booking_ref
                        INTO v_ins_selling_fare_amt, v_ins_remaining_tax_amt, v_airline_num, v_ticket_no, v_group, v_booking_ref
                        FROM L_AIR_EMD
                        WHERE record_no = i;
						
						v_ins_ticket_no := v_ticket_no;
						
                        IF v_exchange_found THEN
							v_ins_exch_ticket_no := v_exch_ticket_no;
                            -- Get the next exchange ticket number if this is an
                            -- exchange conjunction ticket
                            v_exch_ticket_no := v_exch_ticket_no + 1;
                        END IF;
                        
                    END IF;
                    
                    IF v_void OR v_departure_date IS NULL THEN
                        v_departure_date := NULL;
                        v_ticket_issue_date := NULL;
                        v_pnr_date := NULL;
                    END IF;
                    
                    IF v_ins_ticket_no = v_ticket_no THEN
                        v_linked_ticket_no := '';
                    ELSE
                        v_linked_ticket_no := v_ticket_no;
                    END IF;
dbms_output.put_line('********************************************************************************');
dbms_output.put_line('p_stella_get_data.insert_ticket() parameters:');
dbms_output.put_line('*');
dbms_output.put_line('v_source_ind             ' || v_source_ind);
dbms_output.put_line('v_pnr_no                 ' || v_pnr_no);
dbms_output.put_line('v_departure_date         ' || v_departure_date);
dbms_output.put_line('v_ins_ticket_no          ' || v_ins_ticket_no);
dbms_output.put_line('v_airline_num            ' || v_airline_num);
dbms_output.put_line('v_branch_code            ' || v_branch_code);
dbms_output.put_line('v_booking_ref            ' || v_booking_ref);
dbms_output.put_line('v_season                 ' || v_season);
dbms_output.put_line('v_e_ticket_ind           ' || v_e_ticket_ind);
dbms_output.put_line('v_ticket_agent           ' || v_ticket_agent);
dbms_output.put_line('v_ins_commission_amt     ' || v_ins_commission_amt);
dbms_output.put_line('v_ins_commission_pct     ' || v_ins_commission_pct);
dbms_output.put_line('v_ins_selling_fare_amt   ' || v_ins_selling_fare_amt);
dbms_output.put_line('v_ins_published_fare_amt ' || v_ins_published_fare_amt);
dbms_output.put_line('v_iata_num               ' || v_iata_num);
dbms_output.put_line('v_entry_user_id          ' || v_entry_user_id);
dbms_output.put_line('v_ticket_issue_date      ' || v_ticket_issue_date);
dbms_output.put_line('v_num_pax                ' || 1);
dbms_output.put_line('v_passenger_name         ' || v_passenger_name);
dbms_output.put_line('v_ins_gb_tax_amt         ' || v_ins_gb_tax_amt);
dbms_output.put_line('v_ins_remaining_tax_amt  ' || v_ins_remaining_tax_amt);
dbms_output.put_line('v_ins_ub_tax_amt         ' || v_ins_ub_tax_amt);
dbms_output.put_line('v_linked_ticket_no       ' || v_linked_ticket_no);
dbms_output.put_line('v_ccy_code               ' || v_ccy_code);
dbms_output.put_line('v_pseudocitycode         ' || v_pseudocitycode);
dbms_output.put_line('v_passenger_type         ' || v_passenger_type);
dbms_output.put_line('v_doc_type_code          ' || v_doc_type_code);
dbms_output.put_line('v_insertorupdate         ' || v_insertorupdate);
dbms_output.put_line('v_exch_ticket_no         ' || v_ins_exch_ticket_no);
dbms_output.put_line('v_pnr_id                 ' || '');
dbms_output.put_line('v_tour_code              ' || v_tour_code);
dbms_output.put_line('v_fare_basis_code        ' || v_fare_basis_code);
dbms_output.put_line('v_conjunction_ticket_ind ' || v_conjunction_ticket_ind);
dbms_output.put_line('v_pnr_date               ' || v_pnr_date);
dbms_output.put_line('v_ins_other_taxes        ' || v_ins_other_taxes);
dbms_output.put_line('v_ticket_type            ' || v_ticket_type);
dbms_output.put_line('v_group                  ' || v_group);
dbms_output.put_line('********************************************************************************');
dbms_output.put_line('*');

                    v_result :=  p_stella_get_data.insert_ticket(
                        v_source_ind,
                        v_pnr_no,
                        v_departure_date,
                        v_ins_ticket_no,
                        v_airline_num,
                        v_branch_code,
                        v_booking_ref,
                        v_season,
                        v_e_ticket_ind,
                        v_ticket_agent,
                        v_ins_commission_amt,
                        v_ins_commission_pct,
                        v_ins_selling_fare_amt,
                        v_ins_published_fare_amt,
                        v_iata_num,
                        v_entry_user_id,
                        v_ticket_issue_date,
                        1,
                        v_passenger_name,
                        v_ins_gb_tax_amt,
                        v_ins_remaining_tax_amt,
                        v_ins_ub_tax_amt,
                        v_linked_ticket_no,
                        v_ccy_code,
                        v_pseudocitycode,
                        v_passenger_type,
                        v_doc_type_code,
                        v_insertorupdate,
                        v_exch_ticket_no,
                        NULL,
                        v_tour_code,
                        v_fare_basis_code,
                        v_conjunction_ticket_ind,
                        v_pnr_date,
                        v_ins_other_taxes,
                        v_ticket_type,
                        v_group
                    );
                    
dbms_output.put_line('Result of insert_ticket() ' || v_result);
                    IF v_result IS NOT NULL THEN
                        IF SUBSTR(v_result, 1, 11) = 'Error, (fk)' THEN
                            core_dataw.sp_errors('AIR_TICKET','AIR_TICKET',SQLCODE,'File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no)
                                || ' - ' || v_result || ', retry next run');
                        ELSIF SUBSTR(v_result, 1, 6) = 'Error ' THEN
                            v_error_message := 'ERROR insert failed, moved to error area';
                            RAISE ticket_error;
                        END IF;
                    ELSE
                        v_num_inserted_rows := v_num_inserted_rows + 1;
                        IF i = 1 THEN
                            -- the original ticket insert, not any conjunctive ones
                            v_num_inserted_tkts := v_num_inserted_tkts + 1;
                        END IF;
                    END IF;

                END LOOP;

            END IF;
            IF SUBSTR(c1_rec.data_text,1,2) = 'I-' THEN
                -- Passenger data
dbms_output.put_line(c1_rec.data_text);
                v_num_pax := v_num_pax + 1;
                v_passenger_no := SUBSTR(c1_rec.data_text, 3, INSTR(c1_rec.data_text, ';', 1, 1)-3);
dbms_output.put_line('Start processing passenger ' || v_passenger_no || ' data');
dbms_output.put_line('v_num_pax ' || v_num_pax);
                IF INSTR(c1_rec.data_text, ';', 1, 2) > 0 THEN
                    v_passenger_name := SUBSTR(c1_rec.data_text, 9, INSTR(c1_rec.data_text, ';', 1, 2) - 9 );
dbms_output.put_line('Passenger name ' || v_passenger_name);
                END IF;

                v_insert_passenger := True;

                IF INSTR(c1_rec.data_text, '(ADT', 1, 1) > 0 THEN
                    v_passenger_type := 'AD';
                ELSIF INSTR(c1_rec.data_text, '(CHD', 1, 1) > 0 THEN
                    v_passenger_type := 'CH';
                ELSIF INSTR(c1_rec.data_text, '(INF', 1, 1) > 0 THEN
                    v_passenger_type := 'IN';
                ELSIF INSTR(c1_rec.data_text, '(SN', 1, 1) > 0 THEN  --senior citizen
                    v_passenger_type := 'SN';
                ELSIF INSTR(c1_rec.data_text, '(CO', 1, 1) > 0 THEN  --companion ticket
                    v_passenger_type := 'CO';
                ELSE
                    v_passenger_type := 'AD';
                END IF;
dbms_output.put_line('Passenger type ' || v_passenger_type);

            END IF;
        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'T-' THEN
            -- Ticket number(s)
dbms_output.put_line(c1_rec.data_text);
            -- Recover all fare values which were set to 0 for conjunction dummies
            v_ins_published_fare_amt := v_published_fare_amt;
            v_ins_selling_fare_amt   := v_selling_fare_amt;
            v_ins_gb_tax_amt         := v_gb_tax_amt;
            v_ins_ub_tax_amt         := v_ub_tax_amt;
            v_ins_remaining_tax_amt  := v_remaining_tax_amt;
            v_ins_other_taxes        := v_other_taxes;
			v_ins_commission_amt     := v_ins_commission_amt;
			v_ins_commission_pct     := v_ins_commission_pct;

            v_num_conj_tkts := 0;

            v_ticket_no := SUBSTR(c1_rec.data_text,8,10);
dbms_output.put_line('Ticket number ' || v_ticket_no);

            v_pos1 := INSTR(c1_rec.data_text, '-', 1, 3);

            v_max_conj_ticket_no := v_ticket_no;
            IF v_pos1 > 0 THEN
                v_last_ticket_no := SUBSTR(c1_rec.data_text, v_pos1 + 1, 2);
                -- Up to 10 conjunction tickets possible
                WHILE SUBSTR(TO_CHAR(v_max_conj_ticket_no), -2, 2) != v_last_ticket_no LOOP
                    v_max_conj_ticket_no := v_max_conj_ticket_no + 1;
                END LOOP;
            END IF;
dbms_output.put_line('Last conjunction ticket number ' || v_max_conj_ticket_no);

            v_num_conj_tkts := v_max_conj_ticket_no - v_ticket_no + 1;
dbms_output.put_line('Number of conjunction tickets ' || v_num_conj_tkts);

            v_ticket_type := SUBSTR(c1_rec.data_text,3,1);
dbms_output.put_line('Ticket type ' || v_ticket_type);

            IF v_ticket_type IN ('E','K','U','P') THEN
                v_e_ticket_ind := 'Y';
            ELSE
                v_e_ticket_ind := 'N';
            END IF;
dbms_output.put_line('eTicket indicator ' || v_e_ticket_ind);

            v_airline_num := SUBSTR(c1_rec.data_text,4,3);
dbms_output.put_line('Airline number ' || v_airline_num);

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'FM' AND NOT SUBSTR(c1_rec.data_text,1,3) = 'FMB' THEN
            -- Get commission Amt / Pct . If letter before ; is A than
            -- amount else pct,FM*C*1;S3-4,6-7;P1-2 OR FM*M*8
dbms_output.put_line(c1_rec.data_text);

            v_pos1 := INSTR(c1_rec.data_text, '*', 1, 2);
            IF v_pos1 <= 0 THEN
                -- Assuming recordText starts with "FM" so not v_pos1 = 0
                v_pos1 := 1;
            END IF;

            v_pos2 := INSTR(c1_rec.data_text, ';', 1, 1);
            IF v_pos2 <= 0 THEN
                v_pos2 := LENGTH(c1_rec.data_text);
            END IF;

            IF SUBSTR(c1_rec.data_text, v_pos2 - 1, 1) = 'P' THEN
                -- Commission percent
                v_commission_pct := TO_NUMBER(SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 -1));
            ELSIF SUBSTR(c1_rec.data_text, v_pos2 - 1, 1) = 'A' THEN
                -- Commission amount
                v_commission_amt := TO_NUMBER(SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 -1));
            ELSE
                v_commission_amt := TO_NUMBER(SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 -1));
            END IF;
dbms_output.put_line('Commission percent ' || v_commission_pct);
dbms_output.put_line('Commission amount '  || v_commission_amt);

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'FT' THEN
            -- Tour code
            -- Tour Code Can start with FTIT (BT / IT) or FTNR (Net remitt),
            -- Tour Code can be very long so make sure you get maximum of 15 chars
            -- look for ;S if found than take up to that or else
            -- look for ;P if found than take up to that or else up to ';' else up to end of line
dbms_output.put_line(c1_rec.data_text);

            v_pos1 := INSTR(c1_rec.data_text, ';S', 1, 1);
            IF v_pos1 <= 0 THEN
                v_pos1 := INSTR(c1_rec.data_text, ';P', 1, 1);
            END IF;
            IF v_pos1 <= 0 THEN
                v_pos1 := INSTR(c1_rec.data_text, ';', 1, 1);
            END IF;
            IF v_pos1 <= 0 THEN
                v_pos1 := LENGTH(c1_rec.data_text);
            END IF;

            v_tour_code := SUBSTR(c1_rec.data_text, 3, v_pos1 - 3);
            IF LENGTH(v_tour_code) > 15 THEN
                v_tour_code := SUBSTR(v_tour_code, 1, 15);
            END IF;
dbms_output.put_line('Tour code '  || v_tour_code);

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'FO' THEN
            -- Original Issue / Exchange data
            -- Example FO1252570513764LON15MAY1891278401
            --         the airline code is 125 (always 3 digits) and the exchange ticket no is 2570513764
dbms_output.put_line(c1_rec.data_text);

            v_exchange_found := True;

            v_fo_record := '';
            v_pos1 := INSTR(c1_rec.data_text, '-', 1, 1);
            IF v_pos1 = 6 THEN
                -- e.g. FO006-2570769969-70LON24MAY18/91212295/006-25707699694E1234*I;S4-8;P3
                v_fo_record := SUBSTR(c1_rec.data_text, 7);
            ELSE
                -- e.g. FO1572570111863LON30APR1891278401
                v_fo_record := SUBSTR(c1_rec.data_text, 6);
            END IF;

            IF LENGTH(v_fo_record) > 10 THEN
--                v_pos2 := REGEXP_INSTR(v_fo_record, '[A-Z]', 1, 1);

                v_num_exchconj_tkts := 0;
                
                v_exch_ticket_no := SUBSTR(v_fo_record,1,10);
dbms_output.put_line('Exchange ticket number ' || v_exch_ticket_no);

                v_pos1 := INSTR(v_fo_record, '-', 1, 1);
                
                v_max_exchconj_ticket_no := v_exch_ticket_no;
                IF v_pos1 > 0 THEN
                    v_last_ticket_no := SUBSTR(v_fo_record, v_pos1 + 1, 2);
                    -- Up to 10 conjunction tickets possible
                    WHILE SUBSTR(TO_CHAR(v_max_exchconj_ticket_no), -2, 2) != v_last_ticket_no LOOP
                        v_max_exchconj_ticket_no := v_max_exchconj_ticket_no + 1;
                    END LOOP;
                END IF;
dbms_output.put_line('Last exchange conjunction ticket number ' || v_max_exchconj_ticket_no);

            v_num_exchconj_tkts := v_max_exchconj_ticket_no - v_exch_ticket_no + 1;
dbms_output.put_line('Number of exchange conjunction tickets ' || v_num_exchconj_tkts);
            END IF;

        ELSIF (SUBSTR(c1_rec.data_text,1,2) = 'FP' AND v_pseudocitycode IN ('38HJ','33CI','33MV','38TW','38TI','38TS','38JT','3100','3101','31FS') AND LENGTH(c1_rec.data_text) > 14)
                OR (SUBSTR(c1_rec.data_text,1,2) = 'RM' AND (SUBSTR(c1_rec.data_text,1,6) = 'RM ##D' OR SUBSTR(c1_rec.data_text,1,5) = 'RM #D'))
                THEN
            -- Form of payment data; supported by customised remarks (RM) data in some cases
            -- FPO record is already read at the beginning to get collection amt
dbms_output.put_line(c1_rec.data_text);

            IF SUBSTR(c1_rec.data_text,1,2) = 'FP' AND INSTR(c1_rec.data_text, 'NONREFAGT', 1, 1) > 0 THEN
                v_pos1 := REGEXP_INSTR(c1_rec.data_text, 'NONREFAGT[A-Z]{1,3}[0-9]{1,10}', 1, 1, 0);
                v_pos2 := REGEXP_INSTR(c1_rec.data_text, 'NONREFAGT[A-Z]{1,3}[0-9]{1,10}', 1, 1, 1);

                IF v_pos2 = 0 THEN
                    v_pos2 := LENGTH(c1_rec.data_text);
                END IF;
                IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
                    v_fprefs := SUBSTR(c1_rec.data_text, v_pos1 + 9, v_pos2 - v_pos1 - 9);
                    v_group := REGEXP_REPLACE(v_fprefs, '[0-9;-]','');
                    v_booking_ref := REGEXP_REPLACE(v_fprefs, '[A-Z;-]','');

                    IF v_emd_found > 0 THEN
                        IF v_bkgref_check IS NULL AND v_booking_ref IS NOT NULL THEN
                            v_bkgref_check := v_booking_ref;
                        ELSE
                            v_bkgref_consistent := (v_bkgref_check != v_booking_ref);
                        END IF;
                    END IF;
                    
                END IF;
            ELSIF SUBSTR(c1_rec.data_text,1,2) = 'FP' AND INSTR(c1_rec.data_text, 'NONREF', 1, 1) > 0 THEN
                v_pos1 := REGEXP_INSTR(c1_rec.data_text, 'NONREF[A-Z]{1,3}[0-9]{1,10}', 1, 1, 0);
                v_pos2 := REGEXP_INSTR(c1_rec.data_text, 'NONREF[A-Z]{1,3}[0-9]{1,10}', 1, 1, 1);
                IF v_pos2 = 0 THEN
                    v_pos2 := LENGTH(c1_rec.data_text);
                END IF;
                IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
                    v_fprefs := SUBSTR(c1_rec.data_text, v_pos1 + 6, v_pos2 - v_pos1 - 6);
                    v_group := REGEXP_REPLACE(v_fprefs, '[0-9;-]','');
                    v_booking_ref := REGEXP_REPLACE(v_fprefs, '[A-Z;-]','');

                    IF v_emd_found > 0 THEN
                        IF v_bkgref_check IS NULL AND v_booking_ref IS NOT NULL THEN
                            v_bkgref_check := v_booking_ref;
                        ELSE
                            v_bkgref_consistent := (v_bkgref_check != v_booking_ref);
                        END IF;
                    END IF;
                    
                END IF;
            ELSIF SUBSTR(c1_rec.data_text,1,6) = 'RM ##D' OR SUBSTR(c1_rec.data_text,1,5) = 'RM #D' THEN
                v_inside_rm := True;
                v_pos1 := REGEXP_INSTR(c1_rec.data_text, '#D[A-Z]{1,3}[0-9]{1,10}', 1, 1, 0);
                v_pos2 := REGEXP_INSTR(c1_rec.data_text, '#D[A-Z]{1,3}[0-9]{1,10}', 1, 1, 1);
                IF v_pos2 = 0 THEN
                    v_pos2 := LENGTH(c1_rec.data_text);
                END IF;
                IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
                    v_rmrefs := SUBSTR(c1_rec.data_text, v_pos1 + 2, v_pos2 - v_pos1 - 2);
                    IF v_group IS NULL THEN
                        v_group := REGEXP_REPLACE(v_rmrefs, '[0-9;-]','');
                    END IF;
                    IF v_booking_ref = '1' THEN
                        v_booking_ref := REGEXP_REPLACE(v_rmrefs, '[A-Z;-]','');
                    END IF;

                    IF v_emd_found > 0 THEN
                        IF v_bkgref_check IS NULL AND v_booking_ref IS NOT NULL THEN
                            v_bkgref_check := v_booking_ref;
                        ELSE
                            v_bkgref_consistent := (v_bkgref_check != v_booking_ref);
                        END IF;
                    END IF;
                    
                END IF;
            ELSIF v_inside_rm AND v_group IS NULL THEN
dbms_output.put_line('File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no) || ' cannot read Travelink booking ref in either FP line or RM line');
                RAISE no_booking_ref;
            END IF;
dbms_output.put_line('Group ' || v_group);
dbms_output.put_line('Booking ref ' || v_booking_ref);

        ELSIF SUBSTR(c1_rec.data_text,1,4) = 'TMCD' THEN
            -- Ticket number(s) associated with electronic miscellaneous document
            -- TMCD160-1855273067;;D1
dbms_output.put_line(c1_rec.data_text);
			v_pos1 := REGEXP_INSTR(c1_rec.data_text, '-[0-9]{1,10}', 1, 1, 0);
			v_pos2 := REGEXP_INSTR(c1_rec.data_text, '-[0-9]{1,10}', 1, 1, 1);
            IF v_pos2 = 0 THEN
                v_pos2 := LENGTH(c1_rec.data_text);
            END IF;

            IF v_pos1 > 5 AND v_pos2 > 7 AND v_pos1 < v_pos2 THEN
                e_airline_num := SUBSTR(c1_rec.data_text, 5, v_pos1 - 5);
dbms_output.put_line('EMD airline number ' || e_airline_num);

                e_ticket_no := SUBSTR(c1_rec.data_text, v_pos1 + 1, v_pos2 - v_pos1 - 1);
dbms_output.put_line('EMD ticket number ' || e_ticket_no);

                v_pos1 := INSTR(c1_rec.data_text, ';', 1, 2);
                e_doc_id := SUBSTR(c1_rec.data_text, v_pos1 + 1);
dbms_output.put_line('EMD doc id ' || e_doc_id);

                BEGIN
                    UPDATE L_AIR_EMD
                        SET
                            AIRLINE_NUM = e_airline_num,
                            TICKET_NO = e_ticket_no
                    WHERE DOC_ID = e_doc_id;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_error_message := 'ERROR updating EMD airline and ticket data in table L_AIR_EMD';
                        RAISE emd_error;
                END;
                COMMIT;
                
            END IF;

        ELSIF SUBSTR(c1_rec.data_text,1,2) = 'MF' OR SUBSTR(c1_rec.data_text,1,12) = 'MFPNONREFAGT' THEN
            -- Form of payment data associated with electronic miscellaneous document
            -- MFPNONREFAGTZ1211039;;;D1
dbms_output.put_line(c1_rec.data_text);
            v_pos1 := REGEXP_INSTR(c1_rec.data_text, 'NONREFAGT[A-Z]{1,3}[0-9]{1,10}', 1, 1, 0);
			v_pos2 := REGEXP_INSTR(c1_rec.data_text, 'NONREFAGT[A-Z]{1,3}[0-9]{1,10}', 1, 1, 1);
			
            IF v_pos1 > 0 AND v_pos2 > 0 AND v_pos1 < v_pos2 THEN
				v_temp_string := SUBSTR(c1_rec.data_text, v_pos1 + 9, v_pos2 - v_pos1 - 9);
                e_group := REGEXP_REPLACE(v_temp_string, '[0-9;-]','');
dbms_output.put_line('EMD group ' || e_group);
                e_booking_ref := REGEXP_REPLACE(v_temp_string, '[A-Z;-]','');
dbms_output.put_line('EMD booking ref ' || e_booking_ref);
            
                v_pos1 := INSTR(c1_rec.data_text, ';', 1, 3);
                e_doc_id := SUBSTR(c1_rec.data_text, v_pos1 + 1);
dbms_output.put_line('EMD doc id ' || e_doc_id);
                
                IF v_bkgref_check IS NULL AND v_booking_ref IS NOT NULL THEN
                    v_bkgref_check := v_booking_ref;
                ELSE
                    v_bkgref_consistent := (v_bkgref_check != v_booking_ref);
                END IF;

                BEGIN
                    UPDATE L_AIR_EMD
                        SET
                            GROUP_CODE = e_group,
                            BOOKING_REF = e_booking_ref
                    WHERE DOC_ID = e_doc_id;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_error_message := 'ERROR updating EMD group and booking ref data in table L_AIR_EMD';
                        RAISE emd_error;
                END;
                COMMIT;
                
            END IF;
                
        END IF;

    END LOOP; 
    
    IF (v_emd_found > 0 AND v_num_emd > 0) THEN
        IF v_num_inserted_rows != v_num_emd THEN
            v_error_message := 'ERROR num EMD records (' || v_num_emd || ') <> num inserts (' || v_num_inserted_rows || ')';
            RAISE emd_error;
        END IF;
        IF NOT v_bkgref_consistent THEN
            v_error_message := 'ERROR the booking references in this file are inconsistent)';
            RAISE emd_error;
        END IF;
    END IF;
   
    IF v_num_inserted_tkts != v_num_pax THEN
        v_error_message := 'ERROR num pax (' || v_num_pax || ') <> num inserts (' || v_num_inserted_tkts || ')';
        RAISE consistency_error;
    END IF;
    
    COMMIT;

dbms_output.put_line('End Of File');

--
EXCEPTION
  --
  -- A serious error has occurred
  --
    WHEN already_loaded THEN   
        core_dataw.sp_errors('AIR_TICKET','AIR_TICKET',SQLCODE,'File ' || fileToProcess || ' already loaded - ' || SQLERRM);
    WHEN no_booking_ref THEN
        core_dataw.sp_errors('AIR_TICKET','AIR_TICKET',SQLCODE,'File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no) || ' cannot read Travelink booking ref in either FP line or RM line - ' || SQLERRM);
    WHEN ticket_error THEN
        core_dataw.sp_errors('AIR_TICKET','AIR_TICKET',SQLCODE,'File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no) || ' ' || v_error_message || ' - ' || SQLERRM);
    WHEN consistency_error THEN
        core_dataw.sp_errors('AIR_TICKET','AIR_TICKET',SQLCODE,'File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no) || ' ' || v_error_message || ' - ' || SQLERRM);
    WHEN emd_error THEN
        core_dataw.sp_errors('AIR_TICKET','EMD',SQLCODE,'File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no) || ' ' || v_error_message || ' - ' || SQLERRM);
    WHEN OTHERS THEN
        core_dataw.sp_errors('AIR_TICKET','AIR_TICKET',SQLCODE,'File ' || fileToProcess || ' PNR ' || TO_CHAR(v_pnr_no) || ' Tkt ' || TO_CHAR(v_ticket_no) || ' - ' || SQLERRM);
  --    
END sp_l_air_tickets;
/

