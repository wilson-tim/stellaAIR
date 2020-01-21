--------------------------------------------------------
--  DDL for Package Body P_STELLA_GET_DATA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "STELLA"."P_STELLA_GET_DATA" IS

--========================================================================================================================
-- Package P_STELLA_GET_DATA
--===========================================================
-- Change History
--===========================================================
-- Date         Ver Who         Comment
-- 03-Jan-03    1.0 LAshton     Original Version
-- 24-Aug-18	1.1 TWilson     FUNCTION insert_ticket()
--                              v_group now CHAR and not CHAR(1)
-- 09-Nov-18	1.2 TWilson     v_group now VARCHAR2(3) not CHAR
-- 06-Feb-19    1.3 TWilson     FUNCTION insert_ticket()
--                              season validation now >20
-- 
-- SET DEFINE OFF;
-- @C:\Workspace\UK\Database\dwl\stella\packages\P_STELLA_GET_DATA_1.1.pkg;
--
--===========================================================

  -- Author  : leigh ashton
  -- Created : 03/01/03
  -- Purpose : Defines the procedures and functions for the Stella system
  --         : data extracts. These stored procedures / functions are used by java programs to
  --         : read data from Oracle

  -- Define the application key, used to retrieve properties from the registry
  -- and as the top level key for logging.
  --v_app_key CHAR(8) := 'STELLADT';

  ----------------------------------------------------------------
  ----------------------------------------------------------------

  FUNCTION get_first_departure_date(p_pnr_id IN NUMBER) RETURN DATE IS

    v_result DATE;
  BEGIN

    SELECT MIN(departure_date)
      INTO v_result
      FROM ticket t
     WHERE t.pnr_id = p_pnr_id;

    RETURN v_result;

  EXCEPTION
    WHEN no_data_found THEN
      RETURN NULL;

  END get_first_departure_date;

  ----------------------------------------------------------------
  ----------------------------------------------------------------

  FUNCTION convert_season_type(p_in_season_type CHAR,
                               p_in_season_year CHAR,
                               p_in_branch_code CHAR) RETURN VARCHAR2 IS

    v_return CHAR(1);

  BEGIN
    IF p_in_season_type NOT IN ('S', 'W') THEN
      RETURN '?';
    END IF;

    IF p_in_branch_code IS NULL THEN
      -- e.g. in case of void ticket
      RETURN p_in_season_type;
    END IF;

    SELECT decode(p_in_season_type, 'S', c.summer_code, c.winter_code)
      INTO v_return
      FROM company c, branch b
     WHERE c.company_code = b.company_code
       AND b.season_type = p_in_season_type
       AND b.season_year = p_in_season_year
       AND b.branch_code = p_in_branch_code;

    RETURN v_return;
  EXCEPTION
    WHEN no_data_found THEN
      RETURN '?';

  END convert_season_type;

  --==============================================================
  --==============================================================
  FUNCTION get_airline_user_allocations(p_user_name   IN CHAR,
                                        p_airline_num IN NUMBER)
    RETURN p_stella_get_data.return_refcursor IS

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT aua.user_name,
             aua.airline_num,
             aua.amended_user_id,
             aua.amended_date,
             a.airline_name
        FROM airline_user_allocation aua, airline a
      /* optional parameters for where clause so the same piece of sql can be used
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                to return data from different front-end situations */
       WHERE (nvl(p_airline_num, aua.airline_num) = aua.airline_num)
         AND (nvl(p_user_name, aua.user_name) = aua.user_name)
         AND a.airline_num = aua.airline_num;

    RETURN(v_result);

  END get_airline_user_allocations;

  --==============================================================
  --==============================================================

  FUNCTION get_user_reasons(p_user_name  IN CHAR,
                            p_user_roles IN VARCHAR2)
    RETURN p_stella_get_data.return_refcursor IS

    /* return reason codes and roles they are assigned to this user
    used in exception screen
    return a row for each role that can use that reason code
    so exceptions screen can filter on user-type */

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

    -- removed reference to , jutil.security_user_role
      SELECT DISTINCT r.reason_code,
                      r.application_key,
                      r.description description,
                      r.prompt_refund_letter_ind
        FROM reason r, reason_role rr
       WHERE r.reason_code = rr.reason_code
         AND rr.user_role IN
             (SELECT t.list_element
                FROM TABLE(p_common.get_separated_list(p_user_roles,
                                                       ',',
                                                       'OFF')) t)
         AND rr.use_enabled_ind = 'Y'
         AND r.application_key IN ('STLDWHSR', 'STLBSPR ');


    /*      SELECT DISTINCT r.reason_code,
                   r.application_key,
                   r.description description,
                   r.prompt_refund_letter_ind
     FROM reason r, reason_role rr, jutil.security_user_role srole
    WHERE r.reason_code = rr.reason_code
      AND rr.user_role = srole.user_role
      AND srole.user_name = p_user_name
      AND rr.use_enabled_ind = 'Y'
      AND r.application_key IN ('STLDWHSR', 'STLBSPR ');*/

    RETURN(v_result);

  END get_user_reasons;
  --==============================================================

  FUNCTION get_all_reasons RETURN p_stella_get_data.return_refcursor IS

    /* return full list of reason codes and descriptions
      used in exception screen
    */

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT r.reason_code,
             r.application_key,
             r.description,
             r.system_generated_ind,
             r.application_key,
             a.description application_desc,
             r.prompt_refund_letter_ind

        FROM reason r, application a
       WHERE a.application_key = r.application_key
       ORDER BY r.reason_code;

    RETURN(v_result);

  END get_all_reasons;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  FUNCTION get_user_exceptions(p_user_name             IN CHAR,
                               p_show_bsp_exceptions   IN CHAR,
                               p_show_dwhse_exceptions IN CHAR,
                               p_show_itour_exceptions IN CHAR,
                               p_show_tlink_exceptions IN CHAR,
                               p_specialist_branch     IN VARCHAR2,
                               p_user_roles            IN VARCHAR2)
    RETURN p_stella_get_data.return_refcursor IS

    /* return full list of all current exceptions for a particular user's associated airlines
      and where that user is allowed to see that exception
      used in exception screen

      V1.08 Changed to add Itout Reconciliation exceptions
    */

    v_result p_stella_get_data.return_refcursor;

  BEGIN
    OPEN v_result FOR
    --  first get data warehouse/Gemini  exceptions
      SELECT DISTINCT 'DWH' reconcile_type,
                      aua.airline_num,
                      pnr.pnr_no,
                      pnr.crs_code,
                      pnr.pnr_id,
                      pnr.booking_reference_no,
                      pnr.season_type || substr(pnr.season_year, 3, 2) season,
                      pnr.branch_code,
                      pnr.booking_reason_code,
                      p_stella_get_data.get_first_departure_date(pnr.pnr_id) first_departure_date,
                      r.reason_type_code discrepancy_type,
                      pnr.last_reconcile_attempt_date,
                      prh.data_warehouse_seat_cost,
                      prh.data_warehouse_tax_cost,
                      (prh.data_warehouse_seat_cost +
                      prh.data_warehouse_tax_cost) data_warehouse_total_cost,
                      prh.stella_seat_cost,
                      prh.stella_tax_cost,
                      (prh.stella_seat_cost + prh.stella_tax_cost) stella_total_cost,
                      prh.unmatched_amt
        FROM --jutil.v_security_user u,
             airline_user_allocation aua,
             pnr,
             ticket t,
             reason r,
             pnr_reconciliation_history prh
       WHERE --u.user_name = aua.user_name         AND
       aua.user_name = p_user_name
       AND t.airline_num = aua.airline_num
       AND t.pnr_id = pnr.pnr_id
       AND pnr.booking_reconciled_ind <> 'Y'
       AND r.business_rule_code <> 'MTCH' --exclude those that have been set as "matched"/reconciled
       AND pnr.booking_reason_code IS NOT NULL
       AND pnr.booking_reason_code IN
       (SELECT DISTINCT reason_code
          FROM reason_role rr
         WHERE p_common.value_exists_inlist(rr.user_role, p_user_roles, ',') = 'Y'
           AND rr.view_enabled_ind = 'Y')
       AND r.reason_code = pnr.booking_reason_code
       AND prh.pnr_id = pnr.pnr_id
      -- show latest reconciliation results
       AND prh.process_date =
       (SELECT MAX(process_date)
          FROM pnr_reconciliation_history prh2
         WHERE prh2.pnr_id = prh.pnr_id)
       AND p_show_dwhse_exceptions = 'Y'
       AND prh.process_code IN ('STLDWHSR', 'STELEXCP')
       AND t.doc_type_code <> 'AMA' -- added to filter Itour exceptions
      UNION
      -- Itour /Amadeus exceptions
      SELECT DISTINCT 'ITR' reconcile_type,
                      aua.airline_num,
                      pnr.pnr_no,
                      pnr.crs_code,
                      pnr.pnr_id,
                      pnr.booking_reference_no,
                      pnr.season_type || substr(pnr.season_year, 3, 2) season,
                      pnr.branch_code,
                      pnr.booking_reason_code,
                      p_stella_get_data.get_first_departure_date(pnr.pnr_id) first_departure_date,
                      r.reason_type_code discrepancy_type,
                      pnr.last_reconcile_attempt_date,
                      prh.data_warehouse_seat_cost,
                      prh.data_warehouse_tax_cost,
                      (prh.data_warehouse_seat_cost +
                      prh.data_warehouse_tax_cost) data_warehouse_total_cost,
                      prh.stella_seat_cost,
                      prh.stella_tax_cost,
                      (prh.stella_seat_cost + prh.stella_tax_cost) stella_total_cost,
                      prh.unmatched_amt
        FROM --jutil.v_security_user u,
             airline_user_allocation aua,
             pnr,
             ticket t,
             reason r,
             pnr_reconciliation_history prh
       WHERE --u.user_name = aua.user_name         AND
       aua.user_name = p_user_name
       AND t.airline_num = aua.airline_num
       AND t.pnr_id = pnr.pnr_id
       AND pnr.booking_reconciled_ind <> 'Y'
       AND r.business_rule_code <> 'MTCH' --exclude those that have been set as "matched"/reconciled
       AND pnr.booking_reason_code IS NOT NULL
       AND pnr.booking_reason_code IN
       (SELECT DISTINCT reason_code
          FROM reason_role rr
         WHERE p_common.value_exists_inlist(rr.user_role, p_user_roles, ',') = 'Y'
           AND rr.view_enabled_ind = 'Y')
       AND r.reason_code = pnr.booking_reason_code
       AND prh.pnr_id = pnr.pnr_id
      -- show latest reconciliation results
       AND prh.process_date =
       (SELECT MAX(process_date)
          FROM pnr_reconciliation_history prh2
         WHERE prh2.pnr_id = prh.pnr_id)
       AND p_show_itour_exceptions = 'Y'
       AND prh.process_code IN ('STLITRR', 'STELEXCP')
       AND t.doc_type_code = 'AMA'
      -- Travelink Exceptions
      UNION
      SELECT DISTINCT 'TRL' reconcile_type,
                      aua.airline_num,
                      pnr.pnr_no,
                      pnr.crs_code,
                      pnr.pnr_id,
                      pnr.booking_reference_no,
                      pnr.season_type || substr(pnr.season_year, 3, 2) season,
                      pnr.branch_code,
                      pnr.booking_reason_code,
                      p_stella_get_data.get_first_departure_date(pnr.pnr_id) first_departure_date,
                      r.reason_type_code discrepancy_type,
                      pnr.last_reconcile_attempt_date,
                      prh.data_warehouse_seat_cost,
                      prh.data_warehouse_tax_cost,
                      (prh.data_warehouse_seat_cost +
                      prh.data_warehouse_tax_cost) data_warehouse_total_cost,
                      prh.stella_seat_cost,
                      prh.stella_tax_cost,
                      (prh.stella_seat_cost + prh.stella_tax_cost) stella_total_cost,
                      prh.unmatched_amt
        FROM --jutil.v_security_user u,
             airline_user_allocation aua,
             pnr,
             ticket t,
             reason r,
             pnr_reconciliation_history prh,
             branch b
       WHERE --u.user_name = aua.user_name         AND
       aua.user_name = p_user_name
       AND t.airline_num = aua.airline_num
       AND t.pnr_id = pnr.pnr_id
       AND pnr.booking_reconciled_ind <> 'Y'
       AND r.business_rule_code <> 'MTCH' --exclude those that have been set as "matched"/reconciled
       AND pnr.booking_reason_code IS NOT NULL
       AND pnr.booking_reason_code IN
       (SELECT DISTINCT reason_code
          FROM reason_role rr
         WHERE p_common.value_exists_inlist(rr.user_role, p_user_roles, ',') = 'Y'
           AND rr.view_enabled_ind = 'Y')
       AND r.reason_code = pnr.booking_reason_code
       AND prh.pnr_id = pnr.pnr_id
      -- show latest reconciliation results
       AND prh.process_date =
       (SELECT MAX(process_date)
          FROM pnr_reconciliation_history prh2
         WHERE prh2.pnr_id = prh.pnr_id)
       AND p_show_tlink_exceptions = 'Y'
       AND prh.process_code IN ('STLTLR', 'STELEXCP')
       AND t.doc_type_code = 'AMA'
       AND pnr.branch_code = b.branch_code
       AND b.branch_code = p_specialist_branch
      UNION
      -- bsp reconciliation exceptions
      -- BSP for FCH and anything else  ,   */
      SELECT 'BSP' || decode(nvl(b.refund_document_no, 0), 0, 'T', 'R') reconcile_type, -- gets set to BSPT or BSPR
             b.airline_num,
             to_char(nvl(b.ticket_no, b.refund_document_no)),
             substr(b.bsp_crs_code, 1, 1) || '/' ||
             substr(b.transaction_code, 1, 2),
             b.bsp_trans_id, -- key used to launch screen to view ticket or refund
             p.booking_reference_no, -- bkg ref
             p.season_type || substr(p.season_year, 3, 2) season,
             p.branch_code, -- branch code from pnr?
             b.reason_code,
             t.departure_date, --departure date
             r.reason_type_code discrepancy_type,
             b.last_reconciled_date,
             b.commissionable_amt - b.supp_commission_amt -
             b.commission_amt bsp_seat_cost,
             b.tax_amt,
             (b.commissionable_amt - b.supp_commission_amt -
             b.commission_amt + b.tax_amt) data_warehouse_total_cost,
             b.stella_seat_amt, -- stella seat cost
             b.stella_tax_amt, -- stella tax cost
             (b.stella_seat_amt + b.stella_tax_amt) stella_total_cost,
             (b.commissionable_amt - b.supp_commission_amt -
             b.commission_amt + b.tax_amt) -
             (b.stella_seat_amt + b.stella_tax_amt) unmatched_amt -- unmatched amt

        FROM --jutil.v_security_user   u,
             airline_user_allocation aua,
             bsp_transaction         b,
             reason                  r,
             pnr                     p,
             ticket                  t,
             branch                  b

       WHERE --u.user_name = aua.user_name          AND
       aua.user_name = p_user_name
       AND b.airline_num = aua.airline_num
       AND b.reconciled_ind <> 'Y'
       AND r.reason_code = b.reason_code
       AND r.business_rule_code <> 'MTCH' -- exclude those that have been set as "matched"/reconciled
       AND b.last_reconciled_date IS NOT NULL -- exclude those that haven't been through auto reconciliation process yet
       AND b.reason_code IN
       (SELECT DISTINCT reason_code
          FROM reason_role rr
         WHERE p_common.value_exists_inlist(rr.user_role, p_user_roles, ',') = 'Y'
           AND rr.view_enabled_ind = 'Y')
       AND p.pnr_id(+) = t.pnr_id
       AND t.ticket_no(+) = b.ticket_no
       AND t.doc_type_code = 'AMA'
       AND p_show_bsp_exceptions = 'Y'
       AND p.branch_code = b.branch_code
       AND b.branch_code <> 'AAIR'
       AND b.branch_code <> p_specialist_branch

      UNION
      -- BSP for amadeus /Itour
      SELECT 'BSP' || decode(nvl(b.refund_document_no, 0), 0, 'T', 'R') reconcile_type, -- gets set to BSPT or BSPR,
             b.airline_num,
             to_char(nvl(b.ticket_no, b.refund_document_no)),
             substr(b.bsp_crs_code, 1, 1) || '/' ||
             substr(b.transaction_code, 1, 2),
             b.bsp_trans_id, -- key used to launch screen to view ticket or refund
             p.booking_reference_no, -- bkg ref
             p.season_type || substr(p.season_year, 3, 2) season,
             p.branch_code, -- branch code from pnr?
             b.reason_code,
             t.departure_date, --departure date
             r.reason_type_code discrepancy_type,
             b.last_reconciled_date,
             b.commissionable_amt - b.supp_commission_amt -
             b.commission_amt bsp_seat_cost,
             b.tax_amt,
             (b.commissionable_amt - b.supp_commission_amt -
             b.commission_amt + b.tax_amt) data_warehouse_total_cost,
             b.stella_seat_amt, -- stella seat cost
             b.stella_tax_amt, -- stella tax cost
             (b.stella_seat_amt + b.stella_tax_amt) stella_total_cost,
             (b.commissionable_amt - b.supp_commission_amt -
             b.commission_amt + b.tax_amt) -
             (b.stella_seat_amt + b.stella_tax_amt) unmatched_amt -- unmatched amt

        FROM --jutil.v_security_user   u,
             airline_user_allocation aua,
             bsp_transaction         b,
             reason                  r,
             pnr                     p,
             ticket                  t,
             branch                  b

       WHERE --u.user_name = aua.user_name          AND
       aua.user_name = p_user_name
       AND b.airline_num = aua.airline_num
       AND b.reconciled_ind <> 'Y'
       AND r.reason_code = b.reason_code
       AND r.business_rule_code <> 'MTCH' -- exclude those that have been set as "matched"/reconciled
       AND b.last_reconciled_date IS NOT NULL -- exclude those that haven't been through auto reconciliation process yet
       AND b.reason_code IN
       (SELECT DISTINCT reason_code
          FROM reason_role rr
         WHERE p_common.value_exists_inlist(rr.user_role, p_user_roles, ',') = 'Y'
           AND rr.view_enabled_ind = 'Y')
       AND p.pnr_id(+) = t.pnr_id
       AND t.ticket_no(+) = b.ticket_no
       AND t.doc_type_code = 'AMA'
       AND p_show_bsp_exceptions = 'Y'
       AND p.branch_code = b.branch_code
       AND b.branch_code = p_specialist_branch -- AAIR , it was AAIR ,before Jan release

      ;

    /*
          UNION
          -- BSP for specialist , H&J,Soverign , Meon , Citalia
          SELECT 'BSP' || decode(nvl(b.refund_document_no, 0), 0, 'T', 'R') reconcile_type, -- gets set to BSPT or BSPR
                 b.airline_num,
                 to_char(nvl(b.ticket_no, b.refund_document_no)),
                 substr(b.bsp_crs_code, 1, 1) || '/' ||
                 substr(b.transaction_code, 1, 2),
                 b.bsp_trans_id, -- key used to launch screen to view ticket or refund
                 p.booking_reference_no, -- bkg ref
                 p.season_type || substr(p.season_year, 3, 2) season,
                 p.branch_code, -- branch code from pnr?
                 b.reason_code,
                 t.departure_date, --departure date
                 r.reason_type_code discrepancy_type,
                 b.last_reconciled_date,
                 b.commissionable_amt - b.supp_commission_amt -
                 b.commission_amt bsp_seat_cost,
                 b.tax_amt,
                 (b.commissionable_amt - b.supp_commission_amt -
                 b.commission_amt + b.tax_amt) data_warehouse_total_cost,
                 b.stella_seat_amt, -- stella seat cost
                 b.stella_tax_amt, -- stella tax cost
                 (b.stella_seat_amt + b.stella_tax_amt) stella_total_cost,
                 (b.commissionable_amt - b.supp_commission_amt -
                 b.commission_amt + b.tax_amt) -
                 (b.stella_seat_amt + b.stella_tax_amt) unmatched_amt -- unmatched amt

            FROM --jutil.v_security_user   u,
                 airline_user_allocation aua,
                 bsp_transaction         b,
                 reason                  r,
                 pnr                     p,
                 ticket                  t,
                 branch                  b

           WHERE --u.user_name = aua.user_name          AND
           aua.user_name = p_user_name
           AND b.airline_num = aua.airline_num
           AND b.reconciled_ind <> 'Y'
           AND r.reason_code = b.reason_code
           AND r.business_rule_code <> 'MTCH' -- exclude those that have been set as "matched"/reconciled
           AND b.last_reconciled_date IS NOT NULL -- exclude those that haven't been through auto reconciliation process yet
           AND b.reason_code IN
           (SELECT DISTINCT reason_code
              FROM reason_role rr
             WHERE p_common.value_exists_inlist(rr.user_role, p_user_roles, ',') = 'Y'
               AND rr.view_enabled_ind = 'Y')
           AND p.pnr_id(+) = t.pnr_id
           AND t.ticket_no(+) = b.ticket_no
           AND t.doc_type_code = 'AMA'
           AND p_show_bsp_exceptions = 'Y'
           AND p.branch_code = b.branch_code
           AND b.branch_code = p_specialist_branch
           ORDER BY 12;

    */

    RETURN(v_result);

  END get_user_exceptions;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  FUNCTION get_reconciliation_history(p_pnr_id IN NUMBER)
    RETURN p_stella_get_data.return_refcursor IS

    /* return full list of the reconciliation history for a particular pnr
    */

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT prh.process_code,
             prh.process_date,
             prh.reason_code,
             r.short_desc reason_short_desc,
             prh.pnr_id,
             prh.amended_user_id,
             prh.data_warehouse_seat_cost,
             prh.data_warehouse_tax_cost,
             (prh.data_warehouse_seat_cost + prh.data_warehouse_tax_cost) data_warehouse_total_cost,
             prh.stella_seat_cost,
             prh.stella_tax_cost,
             (prh.stella_seat_cost + prh.stella_tax_cost) stella_total_cost,
             prh.unmatched_amt
        FROM pnr_reconciliation_history prh, reason r
       WHERE prh.pnr_id = p_pnr_id
         AND r.reason_code = prh.reason_code(+)
       ORDER BY prh.process_date ASC;

    RETURN(v_result);

  END get_reconciliation_history;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  FUNCTION get_refund_tickets(p_document_no NUMBER)
    RETURN p_stella_get_data.return_refcursor IS

    /* return refund batch and child tickets
    used in refund screen*/

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT 'REF' rectype,
             refb.refund_document_no,
             refb.doc_type_code,
             refb.issue_date issue_date,
             refb.dispute_adm_ind,
             refb.dispute_date,
             refb.entry_date,
             refb.entry_user_id,
             refb.amended_date,
             refb.amended_user_id,
             refb.pseudo_city_code,
             refb.source_ind,
             doc.description,
             reft.ticket_no,
             reft.airline_num,
             reft.seat_amt,
             reft.tax_amt,
             reft.fare_used_amt,
             reft.airline_penalty_amt,
             reft.ccy_code,
             reft.commission_amt,
             reft.commission_pct,
             reft.tax_used_amt,
             a.airline_name,
             refb.iata_no,
             p_stella_reconciliation.calc_stella_refund_doc_total(refb.refund_document_no,
                                                                  reft.ticket_no) total_doc_cost,
             reft.amended_user_id refund_tkt_amended_user,
             reft.amended_date refund_tkt_amended_date,
             reft.entry_user_id refund_tkt_entry_user,
             reft.entry_date refund_tkt_entry_date,
             refb.entry_user_id refund_batch_entry_user,
             refb.entry_date refund_batch_entry_date,
             refb.amended_user_id refund_batch_amended_user,
             refb.amended_date refund_batch_amended_date,
             refb.refund_reason_code,
             nvl(refb.reason_free_text, rr.description) reason_free_text

        FROM doc_type      doc,
             refund_batch  refb,
             refund_ticket reft,
             airline       a,
             refund_reason rr

       WHERE doc.doc_type_code = refb.doc_type_code
         AND refb.refund_document_no = p_document_no
         AND reft.refund_document_no = refb.refund_document_no
         AND a.airline_num = reft.airline_num
         AND rr.refund_reason_code(+) = refb.refund_reason_code;

    RETURN(v_result);
  END; -- end function

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_single_ticket_details(p_in_ticket_no NUMBER)
    RETURN p_stella_get_data.return_refcursor IS

    /* return tickets info
    used in refund entry screen and to retrieve info to edit ticket in ticket screen

    V1.08 JR added pnr_creation_date in result set
    */

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT 'TKT' rectype,
             p.pnr_no,
             t.ticket_no,
             t.airline_num,
             t.iata_no,
             t.published_fare_amt,
             t.selling_fare_amt,
             t.commission_pct,
             t.commission_amt,
             ta.ticketing_agent_initials,
             t.e_ticket_ind,
             to_char(t.ticket_issue_date, 'dd/mm/yyyy') ticket_issue_date,
             t.passenger_name,
             t.num_pax,
             t.gb_tax_amt,
             t.ub_tax_amt,
             t.remaining_tax_amt,
             t.passenger_type,
             p.booking_reference_no,
             p.branch_code,
             p.season_type || substr(p.season_year, 3, 2) season,
             to_char(t.departure_date, 'dd/mm/yyyy') departure_date,
             t.doc_type_code,
             doc.description,
             airline.airline_name,
             /* next call returns the season_type that users want to see e.g. C for summer HandJ */
             p_stella_get_data.convert_season_type(p.season_type,
                                                   p.season_year,
                                                   p.branch_code) ||
             substr(p.season_year, 3, 2) derived_season,
             t.source_ind,
             t.exchange_ticket_no,
             p.pnr_id,
             p_stella_reconciliation.calc_stella_ticket_cost(t.ticket_no) total_doc_cost,
             nvl(airline.sector_payment_ind, 'N') sector_payment_ind,
             t.tour_code,
             t.fare_basis_code,
             t.conjunction_ticket_ind,
             t.linked_ticket_no,
             t.amended_date,
             t.amended_user_id,
             t.entry_user_id,
             t.entry_date,
             to_char(p.pnr_creation_date, 'dd/mm/yyyy') pnr_creation_date -- new in V1.08
        FROM ticket t, pnr p, airline, ticketing_agent ta, doc_type doc
       WHERE t.pnr_id = p.pnr_id
         AND ta.ticketing_agent_initials = t.ticketing_agent_initials
         AND airline.airline_num = t.airline_num
         AND t.doc_type_code = doc.doc_type_code
         AND t.ticket_no = p_in_ticket_no;

    RETURN(v_result);
  END; -- end function

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_all_airlines RETURN p_stella_get_data.return_refcursor IS

    /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values */

    v_result p_stella_get_data.return_refcursor;

  BEGIN
    OPEN v_result FOR
      SELECT airline_num,
             airline_name,
             nvl(sector_payment_ind, 'N') sector_payment_ind
        FROM airline
       ORDER BY airline_name;

    RETURN(v_result);
  END; -- end function get_all_airlines

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_all_branches RETURN p_stella_get_data.return_refcursor IS

    /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values*/

    v_result p_stella_get_data.return_refcursor;

  BEGIN
    OPEN v_result FOR
      SELECT b.branch_code, b.description FROM branch b;

    RETURN(v_result);
  END; -- end function get_all_branches

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  FUNCTION get_all_iata_details RETURN p_stella_get_data.return_refcursor IS

    /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values*/

    v_result p_stella_get_data.return_refcursor;

  BEGIN
    OPEN v_result FOR
      SELECT b.iata_no, b.description FROM iata_details b;

    RETURN(v_result);
  END; -- end function

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_all_doc_types(p_doc_category IN CHAR)
    RETURN p_stella_get_data.return_refcursor IS

    /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values*/

    v_result p_stella_get_data.return_refcursor;

  BEGIN
    -- return all rows if passed param is null
    OPEN v_result FOR
      SELECT d.doc_type_code, d.description, d.doc_category_code
        FROM doc_type d
       WHERE d.doc_category_code = p_doc_category
          OR p_doc_category IS NULL
       ORDER BY d.description;

    RETURN(v_result);
  END; -- end function get_all_doc_types

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  -- function get_airline looks up a single airline using airline_Num and returns the airline name or -1 if none found

  FUNCTION get_airline(p_airline_num NUMBER)
    RETURN p_stella_get_data.return_refcursor

   IS

    /* RETURN ONE row from the table */

    --v_airline_name airline.airline_name%TYPE := '-1';
    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR
      SELECT * FROM airline WHERE airline_num = p_airline_num;

    RETURN(v_result);
  END; -- end function get_airline

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  -- stored proc to insert a ticket and pnr - used from either GUI or from batch MIR load

  FUNCTION insert_ticket(p_source_ind               CHAR,
                         p_pnr_no                   CHAR,
                         p_departure_date           DATE,
                         p_ticket_no                CHAR,
                         p_airline_num              CHAR,
                         p_branch_code              CHAR,
                         p_booking_reference_no     CHAR,
                         p_season                   CHAR,
                         p_e_ticket_ind             CHAR,
                         p_ticketing_agent_initials CHAR,
                         p_commission_amt           CHAR,
                         p_commission_pct           CHAR,
                         p_selling_fare_amt         CHAR,
                         p_published_fare_amt       CHAR,
                         p_iata_no                  CHAR,
                         p_entry_user_id            CHAR,
                         p_ticket_issue_date        DATE,
                         p_num_pax                  CHAR,
                         p_passenger_name           CHAR,
                         p_gb_tax_amt               CHAR,
                         p_remaining_tax_amt        CHAR,
                         p_ub_tax_amt               CHAR,
                         p_linked_ticket_no         CHAR,
                         p_ccy_code                 CHAR,
                         p_pseudo_city_code         CHAR,
                         p_passenger_type           CHAR,
                         p_doc_type_code            CHAR,
                         p_update_or_insert         CHAR,
                         p_exchange_ticket_no       CHAR,
                         p_pnr_id_1                 NUMBER, -- passed only if an update is to be issued
                         p_tour_code                CHAR,
                         p_fare_basis_code          CHAR,
                         p_conjunction_ticket_ind   CHAR,
                         p_pnr_creation_date        DATE DEFAULT NULL,
                         p_othertaxes               CHAR DEFAULT NULL, -- added for Air Load , and  passed only for Air load
                         p_ticket_type              CHAR DEFAULT NULL, -- added for Air Load , will not be passed for Mir load
                         p_group                    CHAR DEFAULT NULL)
    RETURN CHAR IS

    v_proc_name CHAR(8) := 'STLTKTIN';

    v_error_statement VARCHAR2(250) := '';
    ex_data_invalid EXCEPTION;
    v_season_type season.season_type%TYPE;
    v_season_year season.season_year%TYPE;
    v_season      VARCHAR2(1000);
    v_error_tab   p_error.g_param_t; -- used in exception handling

    v_crs_code crs.crs_code%TYPE;

    v_seq    NUMBER;
    v_pnr_id pnr.pnr_id%TYPE;
    p_pnr_id pnr.pnr_id%TYPE;

    v_voided_air  CHAR(1) := 'N'; -- used for season to stay for voided air
    v_branch_code branch.branch_code%TYPE := NULL;
    --   v_tlink_booking_reference_no VARCHAR2(50);
    v_booking_reference_no pnr.booking_reference_no%TYPE;
    v_group                VARCHAR2(3);  -- Group may 1, 2 or 3 characters
    v_parameters           VARCHAR2(2000);
    v_count                NUMBER(5) := 0;
    v_reccount             NUMBER(5) := 0;
    v_spec_pseudo_list     VARCHAR2(200);

  BEGIN

    p_pnr_id    := p_pnr_id_1;
    g_statement := 10;
--  dbms_output.put_line('Src:'||p_source_ind||' P:'||v_proc_name||' strt:'||to_char(sysdate,'dd-mon-yy hh24:mi:ss'));
    p_common.debug_message('Src:' || p_source_ind || ' Proc:' || v_proc_name ||
                           ' started:' ||
                           to_char(SYSDATE, 'dd-mon-yy hh24:mi:ss'));
    -- display all values
    p_common.debug_message('pnr:' || p_pnr_no || ' dept:' ||
                           p_departure_date || ' tkt:' || p_ticket_no ||
                           ' airl:' || p_airline_num || ' iata:' ||
                           p_iata_no);


    -- check if ticke/pnr already exists for void tickets
    SELECT COUNT(*)
      INTO v_count
      FROM pnr p, ticket t
     WHERE p.pnr_id = t.pnr_id
       AND t.ticket_no = p_ticket_no;


    -- validate passed values

    IF p_update_or_insert NOT IN ('I', 'U') OR p_update_or_insert IS NULL THEN
      v_error_statement := 'Error, update/insert type is invalid';
      RAISE ex_data_invalid;
    END IF;

    IF p_source_ind = 'OL' AND p_update_or_insert = 'U' AND
       p_pnr_id IS NULL THEN
      v_error_statement := 'Error, SYSTEM ERROR trying to update but pnr id is null';
      RAISE ex_data_invalid;
    END IF;

    IF p_update_or_insert = 'I' AND p_pnr_id IS NOT NULL THEN
      RAISE ex_data_invalid;
    END IF;

    IF p_source_ind IS NULL THEN
      v_error_statement := 'Error, source is blank';
      RAISE ex_data_invalid;
    END IF;
--  dbms_output.put_line('pnr no is ' || p_pnr_no );
--  dbms_output.put_line('pnr no len is ' || to_char(length(p_source_ind)));

    IF p_pnr_no IS NULL OR length(p_pnr_no) <> 6 THEN
      v_error_statement := 'Error, PNR is blank or invalid';
      RAISE ex_data_invalid;
    END IF;

    IF p_iata_no IS NULL THEN
      v_error_statement := 'Error, iata num is blank';
      RAISE ex_data_invalid;
    END IF;

    -- Added by JR, new columsn V1.8
    -- check if ticket is not void than pnr creation date can not be null
    IF (p_passenger_name <> 'VOID') AND (p_pnr_creation_date IS NULL) THEN
      v_error_statement := 'Error, pnr creation date is blank';
      RAISE ex_data_invalid;
    END IF;


    -- if pass name VOID, then this is a void ticket
    IF p_passenger_name <> 'VOID' THEN
      IF p_departure_date IS NULL THEN
        v_error_statement := 'Error, departure date is blank';
        RAISE ex_data_invalid;
      END IF;
      IF p_branch_code IS NULL THEN
        v_error_statement := 'Error, branch code is blank';
        RAISE ex_data_invalid;
      END IF;
      IF p_passenger_type IS NULL THEN
        v_error_statement := 'Error, pax type blank';
        RAISE ex_data_invalid;
      END IF;
    END IF;

    -- if season is null then it is calculated later
    IF p_season IS NOT NULL THEN
      IF substr(p_season, 1, 1) NOT IN ('S', 'W', 'Y') OR -- Y is asdded for air load
         NOT app_util.is_number(substr(p_season, 2, 2)) THEN
        v_error_statement := 'Error, season is blank or invalid';
        RAISE ex_data_invalid;
      END IF;

      IF substr(p_season, 2, 2) > 21 THEN
        v_error_statement := 'Error, season is invalid (>21)';
        RAISE ex_data_invalid;
      END IF;
    END IF;
    
    IF NOT app_util.is_number(p_booking_reference_no) THEN
      v_error_statement := 'Error, booking ref is not numeric';
      RAISE ex_data_invalid;
    END IF;
    
    IF p_airline_num IS NULL THEN
      v_error_statement := 'Error, airline is blank';
      RAISE ex_data_invalid;
    END IF;

    IF NOT app_util.is_number(p_airline_num) THEN
      v_error_statement := 'Error, airline num is not numeric';
      RAISE ex_data_invalid;
    END IF;

    -- Create default airline record if airline number is not found
    SELECT COUNT(*)
    INTO v_reccount
    FROM AIRLINE
    WHERE AIRLINE_NUM = p_airline_num;
    IF v_reccount = 0 THEN
      BEGIN
        INSERT INTO AIRLINE (AIRLINE_NUM, AIRLINE_CODE, AIRLINE_NAME)
        VALUES (p_airline_num, 'ZZ', 'AIRLINE ' || TO_CHAR(p_airline_num));
        dbms_output.put_line('INFO Airline ' || TO_CHAR(p_airline_num) || ' created, please check');
      END;
    END IF;

    IF p_ticket_no IS NULL THEN
      v_error_statement := 'Error, ticket no is blank';
      RAISE ex_data_invalid;
    END IF;

    IF NOT app_util.is_number(p_ticket_no) THEN
      v_error_statement := 'Error, ticket no. is not numeric';
      RAISE ex_data_invalid;
    END IF;
    g_statement := 15;

    IF p_e_ticket_ind IS NULL OR p_e_ticket_ind NOT IN ('Y', 'N') THEN
      v_error_statement := 'Error, eticket ind invalid';
      RAISE ex_data_invalid;
    END IF;

    -- Added by JR as part of Airload, V1.07
    -- Here A for  ATB , O for OPATB , T for TAT or (O) OPTAT ,
    -- E for Electronic ticket (ATB/OPATB)
    -- if condition is changed so won't fire for Mir Load
    IF (p_ticket_type IS NOT NULL) AND
       (p_ticket_type NOT IN ('A', 'T', 'E', 'O', 'K', 'U', 'P')) THEN
      v_error_statement := 'Error, ticket type invalid';
      RAISE ex_data_invalid;
    END IF;


    IF p_commission_pct IS NULL OR NOT app_util.is_number(p_commission_pct) OR
       p_commission_amt IS NULL OR NOT app_util.is_number(p_commission_amt) THEN
      v_error_statement := 'Error, comm pct/amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_selling_fare_amt IS NULL OR
       NOT app_util.is_number(p_selling_fare_amt) THEN
      v_error_statement := 'Error, selling amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;

    IF p_gb_tax_amt IS NULL OR NOT app_util.is_number(p_gb_tax_amt) THEN
      v_error_statement := 'Error, gb tax amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;

    IF p_published_fare_amt IS NULL OR
       NOT app_util.is_number(p_published_fare_amt) THEN
      v_error_statement := 'Error, published fare amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;

    IF p_remaining_tax_amt IS NULL OR
       NOT app_util.is_number(p_remaining_tax_amt) THEN
      v_error_statement := 'Error, remaining_tax amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;

    IF p_ub_tax_amt IS NULL OR NOT app_util.is_number(p_ub_tax_amt) THEN
      v_error_statement := 'Error, ub_tax amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    g_statement := 18;
    IF p_iata_no IS NOT NULL AND NOT app_util.is_number(p_iata_no) THEN
      v_error_statement := 'Error, IATA no. not numeric (' || p_iata_no || ')';
      RAISE ex_data_invalid;
    END IF;

    IF p_entry_user_id IS NULL THEN
      v_error_statement := 'Error, entry user blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_num_pax IS NOT NULL THEN
      IF NOT app_util.is_number(p_num_pax) THEN
        v_error_statement := 'Error, number of pax not numeric';
        RAISE ex_data_invalid;
      END IF;
    END IF;

    IF p_passenger_name IS NULL THEN
      v_error_statement := 'Error, pax name blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_conjunction_ticket_ind NOT IN ('Y', 'N') THEN
      v_error_statement := 'Error, conjunction ind blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_linked_ticket_no IS NOT NULL THEN
      IF NOT app_util.is_number(p_linked_ticket_no) THEN
        v_error_statement := 'Error, linked tkt no not numeric';
        RAISE ex_data_invalid;
      END IF;
    END IF;

    IF p_exchange_ticket_no IS NOT NULL THEN
      IF NOT app_util.is_number(p_exchange_ticket_no) THEN
        v_error_statement := 'Error, exchange tkt no not numeric';
        RAISE ex_data_invalid;
      END IF;
    END IF;

    IF p_doc_type_code IS NULL THEN
      v_error_statement := 'Error, document type is blank';
      RAISE ex_data_invalid;
    END IF;

    -- ticketing agent initials field can be used to store either
    -- ticketing agent initials OR ticketing agent number
    -- if all three digits are numeric then is a number, so store in number column of ticket table

    IF p_ticketing_agent_initials IS NULL THEN
      v_error_statement := 'Error, tkt agent blank';
      RAISE ex_data_invalid;
    END IF;

    g_statement := 22;
    p_common.debug_message(g_statement);
    -- now check length of passed columns

    IF length(p_ticket_no) > 12 THEN
      v_error_statement := 'Error, ticket num too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_airline_num) > 3 THEN
      v_error_statement := 'Error, airline num too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_iata_no) > 10 THEN
      v_error_statement := 'Error, iata num too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_published_fare_amt) > 18 THEN
      v_error_statement := 'Error, pub. fare amt too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_selling_fare_amt) > 18 THEN
      v_error_statement := 'Error, selling fare too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(round(p_commission_pct, 2)) > 6 THEN
      v_error_statement := 'Error, comm. pct too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_commission_amt) > 18 THEN
      v_error_statement := 'Error, comm. amt too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_e_ticket_ind) > 1 THEN
      v_error_statement := 'Error, e ticket ind. too long';
      RAISE ex_data_invalid;
    END IF;

    -- Added by JR as part of Airload, V1.07
    IF length(p_ticket_type) > 1 THEN
      v_error_statement := 'Error, ticket type too long';
      RAISE ex_data_invalid;
    END IF;

    IF length(p_source_ind) > 2 THEN
      v_error_statement := 'Error, source ind. too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_entry_user_id) > 30 THEN
      v_error_statement := 'Error, entry user too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_passenger_name) > 50 THEN
      v_error_statement := 'Error, pass. name too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_num_pax) > 4 THEN
      v_error_statement := 'Error, num pax too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_gb_tax_amt) > 18 THEN
      v_error_statement := 'Error, gb tax too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_ub_tax_amt) > 18 THEN
      v_error_statement := 'Error, ub tax too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_remaining_tax_amt) > 18 THEN
      v_error_statement := 'Error, rem. tax too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_linked_ticket_no) > 12 THEN
      v_error_statement := 'Error, linked ticket no. too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_ccy_code) > 3 THEN
      v_error_statement := 'Error, currency too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_pseudo_city_code) > 4 THEN
      v_error_statement := 'Error, pseudo city code too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_passenger_type) > 2 THEN
      v_error_statement := 'Error, pax. type too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_doc_type_code) > 3 THEN
      v_error_statement := 'Error, doc. type too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_pnr_id) > 10 THEN
      v_error_statement := 'Error, pnr id too long';
      RAISE ex_data_invalid;
    END IF;
    IF length(p_tour_code) > 15 THEN
      v_error_statement := 'Error, tour code too long';
      RAISE ex_data_invalid;
    END IF;

    IF p_season IS NOT NULL THEN

      v_season_type := substr(p_season, 1, 1);
      v_season_year := '20' || substr(p_season, 2, 2);
    ELSE
      -- calculate the season based on the departure date
      -- 5 is passed to return in format S2003 instead of S03
      g_statement := 21;

      IF p_departure_date IS NOT NULL THEN
        g_statement := 22;
        -- departure date can be null for a void ticket
        -- season type should be Y for Airs
        IF (p_doc_type_code = 'AMA' OR p_source_ind = 'AL') THEN
--          v_season := app_util.calc_season(SYSDATE, 5, 'Y');
          v_season := 'Y' || TO_CHAR(TO_NUMBER(TO_CHAR(SYSDATE,'YYYY')));
        ELSE
          --  MIR Load
--          v_season := app_util.calc_season(p_departure_date, 5);
          v_season := CASE WHEN TO_NUMBER(TO_CHAR(p_departure_date,'MM')) < 4
                                        THEN 'W' || TO_CHAR(TO_NUMBER(TO_CHAR(p_departure_date,'YYYY')) - 1) -- winter of previous year
                                    WHEN TO_NUMBER(TO_CHAR(p_departure_date,'MM')) >= 4 
                                        AND TO_NUMBER(TO_CHAR(p_departure_date,'MM')) <= 9
                                        THEN 'S' || TO_CHAR(p_departure_date,'YYYY') -- summer of current year
                                    WHEN TO_NUMBER(TO_CHAR(p_departure_date,'MM')) > 9
                                        THEN 'W' || TO_CHAR(p_departure_date,'YYYY') -- winter of current year
                                    END;
        END IF;
      ELSE
        -- don't change season in pnr table if tickets are voided from air
--        dbms_output.put_line('p_source_ind: ' || p_source_ind || '/' || p_passenger_name || '.');
        IF ((p_source_ind = 'AL' AND p_passenger_name = 'VOID') AND
           p_source_ind <> 'OL') THEN
          v_voided_air := 'Y';
          g_statement  := 23;
          v_parameters := 'tkt:' || to_char(p_ticket_no);
          -- get original season for existing tickets which are coming as void

          IF v_count > 0 THEN

            SELECT (p.season_type || p.season_year)
              INTO v_season
              FROM pnr p, ticket t
             WHERE p.pnr_id = t.pnr_id
               AND t.ticket_no = p_ticket_no;

          ELSE
--            v_season := app_util.calc_season(SYSDATE, 5);
            v_season := 'Y' || TO_CHAR(TO_NUMBER(TO_CHAR(SYSDATE,'YYYY')));

          END IF;

        ELSE
          -- it should never go into this else
          -- so assign the season as the current one
--          v_season := app_util.calc_season(SYSDATE, 5);
          v_season := 'Y' || TO_CHAR(TO_NUMBER(TO_CHAR(SYSDATE,'YYYY')));
        END IF;
      END IF;
      v_season_type := substr(v_season, 1, 1);
      v_season_year := substr(v_season, 2);

    END IF;

--  dbms_output.put_line('seas:'||v_season);

    IF p_doc_type_code = 'GAL' OR p_source_ind = 'ML' THEN
      v_crs_code := 'GAL';
    ELSIF p_doc_type_code = 'AMA' OR p_source_ind = 'AL' THEN
      v_crs_code := 'AMA';
    ELSIF p_doc_type_code = 'WST' THEN
      v_crs_code := 'WOR';
    ELSE
      v_crs_code := '???';
    END IF;

    -- Added by JR as part of Airload, V1.08
    IF length(p_othertaxes) > 2000 THEN
      v_error_statement := 'Error, Other Taxes too long';
      RAISE ex_data_invalid;
    END IF;

    -- first process parent PNR row

    g_statement := 29;
    v_parameters := 'p_pnr_id:' || p_pnr_id || ' p_passenger_name:' || p_passenger_name || ' p_source_ind:' || p_source_ind || ' tkt:' || p_ticket_no || ' p_group:' || p_group || ' p_booking_reference_no:' || p_booking_reference_no;

    /* this will not be called from front end
    this if will only fire when airload - void tickets are loaded */
    /* added  check for Online (OL) source */
    IF (p_pnr_id IS NULL AND p_passenger_name = 'VOID' AND
       p_source_ind <> 'OL') THEN
      /* Fix for voided air ,
      bug : void air fails saying duplicate on index
      Fix applied : Always update when there is a void airs came through
       */
      --  BEGIN
      -- find out pnr id
      g_statement := 30;

      v_parameters := 'tkt:' || to_char(p_ticket_no);
      -- get pnr_id from the ticket number
      IF v_count > 0 THEN
        SELECT pnr_id
          INTO v_pnr_id
          FROM ticket
         WHERE ticket_no = p_ticket_no;
        p_pnr_id := v_pnr_id;
      END IF;

    END IF;

    v_group                := upper(p_group);
    v_booking_reference_no := p_booking_reference_no; -- keep it same as received fromjava code

    -- Done in Java program
    -- for  specialist  airs only ,
    --- populate bookign reference from travellink if not came through air files

    -- V1.11  added to get specialist branch code ,  needs to add for Citalia , Meon and Soverign
    -- IF (p_pseudo_city_code = '38HJ') THEN
    v_spec_pseudo_list := app_util.get_registry_property('STELAIRL',
                                                        'LIVE',
                                                        'ALL',
                                                        'SpecialistPseudoList');

--    dbms_output.put_line('p_pseudo_city_code: ' || p_pseudo_city_code);
--    dbms_output.put_line('v_spec_pseudo_list: ' || v_spec_pseudo_list);


    IF (p_common.value_exists_inlist(p_pseudo_city_code,
                                     v_spec_pseudo_list,
                                     ',') = 'Y') THEN

      -- Lots of air coming as no info on FP fo rTlink booking ref , which means no group info for Specialist
      -- so there is extra check
      IF v_group IS NULL THEN
        v_error_statement := 'Error, Group Code is Blank in Air File';
        RAISE ex_data_invalid;
      END IF;

      p_common.debug_message(g_statement);
      g_statement  := 31;
      v_parameters := 'Specialist grp:' || v_group || ' season:' ||
                      v_season_type || v_season_year;
      p_common.debug_message(v_parameters);

--      dbms_output.put_line('param: ' || v_parameters);

      v_branch_code := get_specialist_branchcode(v_group,
                                                 v_season_year,
                                                 v_season_type);

--      dbms_output.put_line('branch is: ' || v_branch_code);
    END IF;

    -- mirload and airload (not void) will do insert here
    IF (p_pnr_id IS NULL) THEN
      --AND  p_passenger_name <> 'VOID')  THEN
      BEGIN

        -- block to do insert of pnr

        SELECT pnr_sequence.NEXTVAL INTO v_seq FROM dual;

        g_statement := 32;
        p_common.debug_message(g_statement);
--        dbms_output.put_line(g_statement);

--   dbms_output.put_line('pnr_id - pnr_no - crs-code - pnr creation_date'
--   ||v_seq||'-'|| p_pnr_no ||'-'|| v_crs_code||'-'||p_pnr_creation_date);
        INSERT INTO pnr
          (pnr_id,
           pnr_no,
           crs_code,
           booking_reference_no,
           branch_code,
           season_type,
           season_year,
           entry_date,
           entry_user_id,
           booking_reconciled_ind,
           pnr_creation_date

           )
        VALUES
          (v_seq,
           p_pnr_no,
           v_crs_code,
           v_booking_reference_no,
           decode(v_branch_code,
                  NULL,
                  p_branch_code, -- AAIR/ MIR branch code
                  v_branch_code),
           v_season_type,
           v_season_year,
           SYSDATE,
           p_entry_user_id,
           'N',
           p_pnr_creation_date);

        g_statement := 33;
        p_common.debug_message(g_statement);
--          dbms_output.put_line(g_statement);

      EXCEPTION
        WHEN dup_val_on_index THEN
          -- dup val on alternate unique index
          -- i.e. we are entering a ticket on a pnr which already exists, so find
          -- its primary key
          -- perhaps the PNR_SEQUENCE value is incorrect and needs to be reset to MAX(PNR.PNR_ID)+1?
          g_statement       := 35;
          v_error_statement := 'Existing PNR';
          p_common.debug_message(g_statement);
--            dbms_output.put_line(g_statement);
--            dbms_output.put_line('p_pseudo_city_code: ' || p_pseudo_city_code);
          -- for voided air pnr creation date is coming as blank , no D- record
          SELECT pnr_id
            INTO v_seq
            FROM pnr p
           WHERE p.pnr_no = p_pnr_no
             AND
                -- p.season_year = v_season_year AND
                -- p.pnr_creation_date = p_pnr_creation_date
                 p.pnr_creation_date =
                 decode(v_voided_air,
                        'Y',
                        p.pnr_creation_date,
                        p_pnr_creation_date)
             AND p.crs_code = v_crs_code;

         g_statement       := 36;
                 p_common.debug_message(g_statement);
--            dbms_output.put_line(g_statement);
      END;

    ELSE
      -- update the pnr
      g_statement := 39;
      p_common.debug_message(g_statement);
--    dbms_output.put_line(g_statement);

      UPDATE pnr
         SET pnr_no                 = p_pnr_no,
             booking_reference_no   = p_booking_reference_no,
             season_type            = decode(v_voided_air,
                                             'Y',
                                             season_type,
                                             v_season_type), -- don't change season for voided air
             season_year            = decode(v_voided_air,
                                             'Y',
                                             season_year,
                                             v_season_year),
             branch_code            = decode(v_branch_code,
                                             NULL,
                                             decode(p_branch_code,
                                                    NULL,
                                                    branch_code,
                                                    p_branch_code), -- AAIR/ MIR branch code, leave as it is if coming as blank
                                             v_branch_code), -- HAJS branch code
             booking_reconciled_ind = 'N',
             booking_reason_code    = NULL,
             amended_user_id        = p_entry_user_id,
             amended_date           = SYSDATE,
             pnr_creation_date      = nvl(p_pnr_creation_date,
                                          pnr_creation_date) -- don't set pnr_creation date as null for void tickets
       WHERE pnr_id = nvl(p_pnr_id, v_pnr_id); -- added v_pnr_id for voided air

      g_statement := 41;
      p_common.debug_message(g_statement);
--      dbms_output.put_line(g_statement);

      IF SQL%ROWCOUNT <> 1 THEN
        -- should never happen
        v_error_statement := 'Error, cannot find pnr to update (' ||
                             p_pnr_no || '). Inform support (pnrid:' ||
                             p_pnr_id || ')';
        RAISE ex_data_invalid;
      END IF;

    END IF; -- if pnr_id is null

    BEGIN
      g_statement := 50;

      p_common.debug_message(g_statement);
--      dbms_output.put_line(g_statement);


      INSERT INTO ticket
        (e_ticket_ind,
         commission_amt,
         commission_pct,
         selling_fare_amt,
         published_fare_amt,
         iata_no,
         ticket_no,
         airline_num,
         source_ind,
         entry_date,
         entry_user_id,
         ticket_issue_date,
         num_pax,
         passenger_name,
         gb_tax_amt,
         remaining_tax_amt,
         ub_tax_amt,
         amended_user_id,
         amended_date,
         exception_code,
         linked_ticket_no,
         ccy_code,
         pseudo_city_code,
         passenger_type,
         ticketing_agent_initials,
         doc_type_code,
         exchange_ticket_no,
         departure_date,
         pnr_id,
         tour_code,
         fare_basis_code,
         conjunction_ticket_ind,
         bsp_trans_id,
         remaining_taxes,
         ticket_type)
      VALUES
        (p_e_ticket_ind,
         round(p_commission_amt, 2),
         round(p_commission_pct, 2),
         p_selling_fare_amt,
         p_published_fare_amt,
         p_iata_no,
         p_ticket_no,
         p_airline_num,
         p_source_ind,
         SYSDATE, -- entry date
         p_entry_user_id,
         p_ticket_issue_date,
         p_num_pax,
         p_passenger_name,
         p_gb_tax_amt,
         p_remaining_tax_amt,
         p_ub_tax_amt,
         NULL, -- p_amended_user_id,
         NULL, -- p_amended_date,
         NULL, -- p_exception_code,
         p_linked_ticket_no,
         nvl(p_ccy_code, 'GBP'),
         p_pseudo_city_code,
         p_passenger_type,
         --         nvl(p_ticketing_agent_initials,ticketing_agent_initials,p_ticketing_agent_initials),
         p_ticketing_agent_initials,
         p_doc_type_code,
         p_exchange_ticket_no,
         p_departure_date,
         nvl(p_pnr_id, nvl(v_seq, v_pnr_id)), --  for voided air , v_seq will be null
         p_tour_code,
         p_fare_basis_code,
         p_conjunction_ticket_ind, -- should be Y if not the main ticket, but a "child" of the main ticket i.e. created because it is a conjunction tkt
         NULL, --means has not been reconciled to BSP
         p_othertaxes,
         p_ticket_type);

      g_statement := 65;
        p_common.debug_message(g_statement);
--              dbms_output.put_line(g_statement);
    EXCEPTION
      WHEN dup_val_on_index THEN

        -- update ticket values instead

        g_statement := 95;
        p_common.debug_message(g_statement);
--             dbms_output.put_line(g_statement);

        UPDATE ticket
           SET airline_num              = p_airline_num,
               iata_no                  = p_iata_no,
               published_fare_amt       = p_published_fare_amt,
               selling_fare_amt         = p_selling_fare_amt,
               commission_pct           = round(p_commission_pct, 2),
               commission_amt           = round(p_commission_amt, 2),
               e_ticket_ind             = p_e_ticket_ind,
               source_ind               = p_source_ind,
               departure_date           = p_departure_date,
               ticket_issue_date        = decode(v_voided_air,
                                                 'Y',
                                                 nvl(p_ticket_issue_date,
                                                     ticket_issue_date),
                                                 p_ticket_issue_date),
               passenger_name           = p_passenger_name,
               num_pax                  = p_num_pax,
               gb_tax_amt               = p_gb_tax_amt,
               ub_tax_amt               = p_ub_tax_amt,
               remaining_tax_amt        = p_remaining_tax_amt,
               amended_date             = SYSDATE,
               amended_user_id          = p_entry_user_id,
               exception_code           = NULL,
               linked_ticket_no         = p_linked_ticket_no,
               ccy_code                 = nvl(p_ccy_code, 'GBP'),
               pseudo_city_code         = p_pseudo_city_code,
               passenger_type           = p_passenger_type,
               ticketing_agent_initials = decode(v_voided_air,
                                                 'Y',
                                                 decode(p_ticketing_agent_initials,
                                                        'ZZ',
                                                        ticketing_agent_initials,
                                                        p_ticketing_agent_initials),
                                                 p_ticketing_agent_initials),
               doc_type_code            = p_doc_type_code,
               exchange_ticket_no       = p_exchange_ticket_no,
               tour_code                = decode(v_voided_air,
                                                 'Y',
                                                 nvl(p_tour_code, tour_code),
                                                 p_tour_code),
               fare_basis_code          = decode(v_voided_air,
                                                 'Y',
                                                 nvl(p_fare_basis_code,
                                                     fare_basis_code),
                                                 p_fare_basis_code),
               conjunction_ticket_ind   = p_conjunction_ticket_ind,
               remaining_taxes          = p_othertaxes,
               ticket_type              = p_ticket_type
         WHERE ticket.ticket_no = p_ticket_no;

        g_statement := 100;
        p_common.debug_message(g_statement);
--             dbms_output.put_line(g_statement);

        IF SQL%ROWCOUNT = 0 THEN
          -- should never happen, must have keys defined wrongly
          v_error_statement := 'Error, cannot find ticket to update (' ||
                               p_ticket_no || '). Inform support';

          RAISE ex_data_invalid;
        END IF;

    END;

    g_statement := 999;

    p_common.debug_message('Src:' || p_source_ind || 'Proc:' || v_proc_name || ' ended:' ||
                           to_char(SYSDATE, 'dd-mon-yyyy hh24:mi:ss'));

    COMMIT;
    RETURN NULL;

  EXCEPTION
    WHEN ex_data_invalid THEN
      ROLLBACK;
      RETURN v_error_statement;

    WHEN OTHERS THEN

      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      p_common.debug_message('Excp others, state:' || g_statement ||
                             ' param:' || v_parameters);
      dbms_output.put_line('Excp others, state:' || g_statement ||
                             ' param:' || v_parameters);

      v_error_statement := 'Error, state:' || to_number(g_statement) ||
                           ' Param:' || v_parameters;

      p_common.debug_message(v_error_statement || ' ' || g_sqlcode);
      dbms_output.put_line(v_error_statement || ' ' || g_sqlcode);
      /*IF g_sqlcode = -2291 THEN
         -- foreign key raised, give decent error message to users
         v_error_statement := '(fk) '||f_format_fk_statement(g_sqlerrm);
      ELSE
         -- other error, just send back sqlerrm
         v_error_statement := v_error_statement||' '||g_sqlerrm;
      END IF;
      */
      IF g_sqlcode = -2291 THEN
        -- foreign key raised, give decent error message to users
        -- buld up table of values to be passed to users via user-friendly error message
        p_error.add_error_parameter(v_error_tab,
                                    'val_airline',
                                    p_airline_num);
        p_error.add_error_parameter(v_error_tab, 'val_src', p_source_ind);
        p_error.add_error_parameter(v_error_tab, 'val_iata', p_iata_no);
        p_error.add_error_parameter(v_error_tab,
                                    'val_branch',
                                    p_branch_code);
        p_error.add_error_parameter(v_error_tab,
                                    'val_season',
                                    v_season_type || v_season_year);
        p_error.add_error_parameter(v_error_tab,
                                    'val_agent',
                                    p_ticketing_agent_initials);
        p_error.add_error_parameter(v_error_tab,
                                    'val_doctype',
                                    p_doc_type_code);
        p_error.add_error_parameter(v_error_tab, 'val_pnrid', p_pnr_id);
        -- get user-friendly error message
        v_error_statement := '(fk) ' ||
                             p_error.get_constraint_error(g_sqlerrm,
                                                          v_error_tab);
      ELSE
        -- other error, just send back sqlerrm
        v_error_statement := v_error_statement || ' ' || g_sqlerrm || ' ' ||
                             v_parameters;

      END IF;

      p_common.debug_message(v_error_statement);
      RETURN 'Error, ' || v_error_statement;

  END insert_ticket;

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION insert_refund(p_refund_document_no        CHAR,
                         p_doc_type_code             CHAR,
                         p_issue_date                DATE,
                         p_dispute_adm_ind           CHAR,
                         p_dispute_date              DATE,
                         p_entry_user_id             CHAR,
                         p_pseudo_city_code          CHAR,
                         p_ccy_code                  CHAR,
                         p_source_ind                CHAR,
                         p_ticket_no                 CHAR,
                         p_airline_num               CHAR,
                         p_seat_amt                  CHAR,
                         p_tax_amt                   CHAR,
                         p_fare_used_amt             CHAR,
                         p_airline_penalty_amt       CHAR,
                         p_tax_adj_amt               CHAR,
                         p_update_delete_insert_flag CHAR,
                         p_commission_amt            CHAR,
                         p_commission_pct            CHAR,
                         p_iata_no                   CHAR,
                         p_refund_reason_code        CHAR,
                         p_refund_free_text          CHAR

                         ) RETURN CHAR IS

    v_proc_name CHAR(8) := 'STINSREF';

    v_error_statement VARCHAR2(150) := '';
    ex_data_invalid EXCEPTION;
    v_error_tab             p_error.g_param_t; -- used in exception handling
    v_free_text_allowed_ind refund_reason.allow_additional_free_text_ind%TYPE;

  BEGIN
    g_statement := 10;
    dbms_output.put_line('Src:' || p_source_ind || ' Proc:' || v_proc_name ||
                         ' started:' ||
                         to_char(SYSDATE, 'dd-mon-yyyy hh24:mi:ss'));
    -- display all values
    dbms_output.put_line('ref:' || p_refund_document_no || ' tkt:' ||
                         p_ticket_no || ' airl:' || p_airline_num);

    -- validate passed values

    IF p_update_delete_insert_flag NOT IN ('I', 'U', 'D') OR
       p_update_delete_insert_flag IS NULL THEN
      v_error_statement := 'Error, update/insert type is invalid or null';
      RAISE ex_data_invalid;
    END IF;

    IF p_source_ind IS NULL THEN
      v_error_statement := 'Error, source is blank';
      RAISE ex_data_invalid;
    END IF;
    IF p_refund_document_no IS NULL THEN
      v_error_statement := 'Error, doc no. is blank';
      RAISE ex_data_invalid;
    END IF;

    IF NOT app_util.is_number(p_refund_document_no) THEN
      v_error_statement := 'Error, refund doc no.' || p_refund_document_no ||
                           ' is not numeric';
      RAISE ex_data_invalid;
    END IF;

    IF p_ticket_no IS NULL THEN
      v_error_statement := 'Error, ticket no is blank';
      RAISE ex_data_invalid;
    END IF;

    -- reason code is mandatory for ADM docs
    IF p_doc_type_code = 'ADM' THEN
      IF p_refund_reason_code IS NULL THEN
        v_error_statement := 'Error, reason cannot be blank for ADM';
        RAISE ex_data_invalid;
      END IF;

      BEGIN
        SELECT allow_additional_free_text_ind
          INTO v_free_text_allowed_ind
          FROM refund_reason t
         WHERE refund_reason_code = p_refund_reason_code;
      EXCEPTION
        WHEN no_data_found THEN
          v_error_statement := 'Error, reason code(' ||
                               p_refund_reason_code || ') IS NOT valid';
          RAISE ex_data_invalid;
      END;

      IF v_free_text_allowed_ind = 'N' AND p_refund_free_text IS NOT NULL THEN
        v_error_statement := ' error, free text NOT allowed FOR that reason code(' ||
                             p_refund_reason_code || ')';
        RAISE ex_data_invalid;
      END IF;

    END IF; -- if adm

    IF p_update_delete_insert_flag = 'D' THEN
      DELETE FROM refund_ticket rt
       WHERE rt.refund_document_no = p_refund_document_no
         AND rt.ticket_no = p_ticket_no;

      IF SQL%ROWCOUNT = 0 THEN
        ROLLBACK;
        RETURN 'Error, cannot find ticket to delete (' || p_refund_document_no || '/' || p_ticket_no;
      END IF;

      RETURN NULL;
    END IF;

    IF p_doc_type_code IS NULL THEN
      v_error_statement := 'Error, document type is blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_issue_date IS NULL THEN
      v_error_statement := 'Error, issue date is blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_dispute_adm_ind IS NULL THEN
      v_error_statement := 'Error, dispute adm ind. is blank';
      RAISE ex_data_invalid;
    END IF;
    IF p_dispute_adm_ind NOT IN ('Y', 'N') THEN
      v_error_statement := 'Error, dispute adm ind. must be Y or N';
      RAISE ex_data_invalid;
    END IF;

    IF p_dispute_adm_ind = 'Y' AND p_dispute_date IS NULL THEN
      v_error_statement := 'Error, dispute date is blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_dispute_adm_ind = 'N' AND p_dispute_date IS NOT NULL THEN
      v_error_statement := 'Error, dispute date is supplied, but not a dispute ADM';
      RAISE ex_data_invalid;
    END IF;

    IF p_iata_no IS NULL THEN
      v_error_statement := 'Error, iata number is blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_entry_user_id IS NULL THEN
      v_error_statement := 'Error, entry user blank';
      RAISE ex_data_invalid;
    END IF;

    IF p_airline_num IS NULL THEN
      v_error_statement := 'Error, airline is blank';
      RAISE ex_data_invalid;
    END IF;

    IF NOT app_util.is_number(p_airline_num) THEN
      v_error_statement := 'Error, airline num is not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_ticket_no IS NULL THEN
      v_error_statement := 'Error, ticket no is blank';
      RAISE ex_data_invalid;
    END IF;

    IF NOT app_util.is_number(p_ticket_no) THEN
      v_error_statement := 'Error, ticket no. is not numeric';
      RAISE ex_data_invalid;
    END IF;
    g_statement := 15;

    IF p_seat_amt IS NULL OR NOT app_util.is_number(p_seat_amt) THEN
      v_error_statement := 'Error, seat amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_tax_amt IS NULL OR NOT app_util.is_number(p_tax_amt) THEN
      v_error_statement := 'Error, tax amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_fare_used_amt IS NULL OR NOT app_util.is_number(p_fare_used_amt) THEN
      v_error_statement := 'Error, fare used amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_airline_penalty_amt IS NULL OR
       NOT app_util.is_number(p_airline_penalty_amt) THEN
      v_error_statement := 'Error, airline penalty amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_tax_adj_amt IS NULL OR NOT app_util.is_number(p_tax_adj_amt) THEN
      v_error_statement := 'Error, tax adj. amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_commission_amt IS NULL OR NOT app_util.is_number(p_commission_amt) THEN
      v_error_statement := 'Error, commission amt null/not numeric';
      RAISE ex_data_invalid;
    END IF;
    IF p_commission_pct IS NULL OR NOT app_util.is_number(p_commission_pct) THEN
      v_error_statement := 'Error, commission pct null/not numeric';
      RAISE ex_data_invalid;
    END IF;

    -- first process parent refund_batch row
    g_statement := 28;

    IF p_update_delete_insert_flag = 'I' THEN
      BEGIN
        INSERT INTO refund_batch
          (refund_document_no,
           doc_type_code,
           issue_date,
           dispute_adm_ind,
           dispute_date,
           entry_date,
           entry_user_id,
           amended_date,
           amended_user_id,
           pseudo_city_code,
           source_ind,
           bsp_trans_id,
           iata_no,
           refund_reason_code,
           reason_free_text)
        VALUES
          (p_refund_document_no,
           p_doc_type_code,
           p_issue_date,
           p_dispute_adm_ind,
           p_dispute_date,
           SYSDATE,
           p_entry_user_id,
           NULL,
           NULL,
           p_pseudo_city_code,
           p_source_ind,
           NULL,
           p_iata_no,
           p_refund_reason_code,
           p_refund_free_text

           );
        g_statement := 32;

      EXCEPTION
        WHEN dup_val_on_index THEN
          -- allowed , since a pnr can have multiple children
          dbms_output.put_line('Dup refundbatch');
          g_statement := 35;

      END;

      BEGIN

        g_statement := 45;

        INSERT INTO refund_ticket
          (refund_document_no,
           ticket_no,
           airline_num,
           seat_amt,
           tax_amt,
           fare_used_amt,
           airline_penalty_amt,
           tax_used_amt,
           ccy_code,
           commission_pct,
           commission_amt,
           amended_user_id,
           amended_date,
           entry_user_id,
           entry_date)
        VALUES
          (p_refund_document_no,
           p_ticket_no,
           p_airline_num,
           p_seat_amt,
           p_tax_amt,
           p_fare_used_amt,
           p_airline_penalty_amt,
           p_tax_adj_amt,
           decode(p_ccy_code, NULL, 'GBP', p_ccy_code),
           p_commission_pct,
           p_commission_amt,
           NULL,
           NULL,
           p_entry_user_id,
           SYSDATE);

      EXCEPTION
        WHEN dup_val_on_index THEN
          ROLLBACK;
          RETURN 'Error, duplicate refund ticket when trying to insert';
      END;

    ELSIF p_update_delete_insert_flag = 'U' THEN
      -- update the existing ticket/batch
      -- assume the refund_document row exists already
      g_statement := 53;

      UPDATE refund_batch rb
         SET issue_date         = p_issue_date,
             dispute_adm_ind    = p_dispute_adm_ind,
             dispute_date       = p_dispute_date,
             amended_date       = SYSDATE,
             amended_user_id    = p_entry_user_id,
             pseudo_city_code   = p_pseudo_city_code,
             source_ind         = p_source_ind,
             iata_no            = p_iata_no,
             refund_reason_code = p_refund_reason_code,
             reason_free_text   = p_refund_free_text,
             doc_type_code      = p_doc_type_code -- fix in refund to allow to update doc type
       WHERE rb.refund_document_no = p_refund_document_no;

      IF SQL%ROWCOUNT = 0 THEN
        ROLLBACK;
        RETURN 'Error, refund doc ' || p_refund_document_no || ' does not exist to be updated';
      END IF;

      g_statement := 54;

      UPDATE refund_ticket rt
         SET rt.airline_num         = p_airline_num,
             rt.seat_amt            = p_seat_amt,
             rt.tax_amt             = p_tax_amt,
             rt.fare_used_amt       = p_fare_used_amt,
             rt.airline_penalty_amt = p_airline_penalty_amt,
             rt.ccy_code            = decode(p_ccy_code,
                                             NULL,
                                             'GBP',
                                             p_ccy_code),
             rt.commission_amt      = p_commission_amt,
             rt.commission_pct      = p_commission_pct,
             rt.tax_used_amt        = p_tax_adj_amt,
             rt.amended_user_id     = p_entry_user_id,
             rt.amended_date        = SYSDATE

       WHERE rt.refund_document_no = p_refund_document_no
         AND rt.ticket_no = p_ticket_no;

      IF SQL%ROWCOUNT = 0 THEN
        ROLLBACK;
        RETURN 'Error, ticket ' || p_refund_document_no || '/' || p_ticket_no || ' does not exist to be updated';
      END IF;

    ELSE
      -- error, cannot be any other, flag should only be D U or I
      RETURN 'Error, logic error in code, flag was :' || p_update_delete_insert_flag;

    END IF; -- if I for insert

    g_statement := 55;
    -- now need to reset reconcile status of pnr so that all reconcilation processes
    -- rerun for this ticket/pnr
    UPDATE pnr p
       SET p.booking_reconciled_ind = 'N'
     WHERE EXISTS (SELECT NULL
              FROM ticket t
             WHERE t.pnr_id = p.pnr_id
               AND t.ticket_no = p_ticket_no);

    IF SQL%ROWCOUNT = 0 THEN
      -- must be problem in primary key vs where clause
      RETURN 'Error, could not find PNR to reset status';
    ELSIF SQL%ROWCOUNT > 1 THEN
      -- must be problem in primary key vs where clause
      RETURN 'Error, too many PNRs to reset status';
    END IF;

    g_statement := 999;
    dbms_output.put_line('Src:' || p_source_ind || ' Proc:' || v_proc_name ||
                         ' ended:' ||
                         to_char(SYSDATE, 'dd-mon-yyyy hh24:mi:ss'));

    COMMIT;
    RETURN NULL;

  EXCEPTION
    WHEN ex_data_invalid THEN
      ROLLBACK;
      RETURN v_error_statement;

    WHEN OTHERS THEN

      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      dbms_output.put_line('Excp others, state:' || g_statement);

      v_error_statement := 'Statement:' || to_number(g_statement);
      dbms_output.put_line(v_error_statement);
      dbms_output.put_line(g_sqlcode);
      IF g_sqlcode = -2291 THEN
        -- foreign key raised, give decent error message to users
        -- buld up table of values to be passed to users via user-friendly error message

        p_error.add_error_parameter(v_error_tab, 'val_src', p_source_ind);
        p_error.add_error_parameter(v_error_tab,
                                    'val_doctype',
                                    p_doc_type_code);
        p_error.add_error_parameter(v_error_tab,
                                    'val_airline',
                                    p_airline_num);
        -- get user-friendly error message
        v_error_statement := '(fk) ' ||
                             p_error.get_constraint_error(g_sqlerrm,
                                                          v_error_tab);
      ELSE
        -- other error, just send back sqlerrm
        v_error_statement := v_error_statement || ' ' || g_sqlerrm;
      END IF;

      dbms_output.put_line(v_error_statement);

      RETURN 'Error, ' || v_error_statement;

  END insert_refund;

  -------------------------------------------------------------
  -------------------------------------------------------------

  FUNCTION update_exception_reason_code(p_reconcile_type  IN CHAR,
                                        p_record_id       IN NUMBER,
                                        p_reason_code     IN CHAR,
                                        p_user_name       IN CHAR,
                                        p_old_reason_code IN CHAR)
    RETURN CHAR IS

    /* function to be called from exceptions screen to
    move pnr onwards in the reconciliation process/workflow

    also called for bsp reconciliations */

    v_error_statement VARCHAR2(150) := '';
    ex_data_invalid EXCEPTION;
    v_matched_ind            CHAR(1) := 'N';
    v_matched_boolean        BOOLEAN := FALSE;
    v_old_reason_code        pnr.booking_reason_code%TYPE := '';
    v_reason_application_key reason.application_key%TYPE;

    v_data_warehouse_seat_cost pnr_reconciliation_history.data_warehouse_seat_cost%TYPE := 0;
    v_data_warehouse_tax_cost  pnr_reconciliation_history.data_warehouse_seat_cost%TYPE := 0;
    v_stella_seat_cost         pnr_reconciliation_history.data_warehouse_seat_cost%TYPE := 0;
    v_stella_tax_cost          pnr_reconciliation_history.data_warehouse_seat_cost%TYPE := 0;
    v_unmatched_amt            pnr_reconciliation_history.data_warehouse_seat_cost%TYPE := 0;

    v_bsp_ticket_no          bsp_transaction.ticket_no%TYPE;
    v_bsp_refund_document_no bsp_transaction.refund_document_no%TYPE;

  BEGIN
    g_statement := 10;

    -- added I for Itour
    IF p_reconcile_type NOT IN ('D', 'B', 'I') THEN
      v_error_statement := 'Error, reconcile type not valid (' ||
                           p_reconcile_type || ')';
      RAISE ex_data_invalid;
    END IF;

    IF p_record_id IS NULL THEN
      v_error_statement := 'Error, blank exception record reference passed';
      RAISE ex_data_invalid;
    END IF;

    -- find out if we should set it to matched status as well according to the
    -- reason code passed
    g_statement := 20;
    BEGIN
      SELECT nvl(b.treat_as_matched_ind, 'N'), r.application_key
        INTO v_matched_ind, v_reason_application_key
        FROM reason r, business_rule b
       WHERE r.reason_code = p_reason_code
         AND r.business_rule_code = b.business_rule_code;

    EXCEPTION
      WHEN no_data_found THEN
        v_error_statement := 'Error, reason code ' || p_reason_code ||
                             ' is not valid';
        RAISE ex_data_invalid;
    END;
    -- update the pnr status accordingly
    IF nvl(v_matched_ind, 'N') = 'Y' THEN
      v_matched_boolean := TRUE;
    ELSE
      v_matched_boolean := FALSE;
    END IF;

    -- For Datawarehouse and Itour application key in reason table will be same STLDWHSR
    IF (p_reconcile_type = 'D') OR (p_reconcile_type = 'I') THEN
      -- data warehouse / booking reconciliation record

      IF TRIM(v_reason_application_key) <> 'STLDWHSR' THEN
        v_error_statement := 'Error, reason code ' || p_reason_code ||
                             ' is not valid for dwhse exception';
        RAISE ex_data_invalid;
      END IF;

      -- first check that the reason code has not been changed by anyone else
      BEGIN

        SELECT p.booking_reason_code
          INTO v_old_reason_code
          FROM pnr p
         WHERE p.pnr_id = p_record_id;

        IF v_old_reason_code <> p_old_reason_code THEN
          v_error_statement := 'Error, this PNR has already been moved to a different status (' ||
                               v_old_reason_code ||
                               ') by a different user/process. Refresh exceptions.';
          RAISE ex_data_invalid;
        END IF;
      EXCEPTION
        WHEN no_data_found THEN
          v_error_statement := 'Error, PNR cannot be found in table. Contact help desk (pnrid:' ||
                               p_record_id || ')';
          RAISE ex_data_invalid;
      END;

      g_statement := 30;

      p_stella_reconciliation.update_recon_status(p_record_id,
                                                  v_matched_boolean,
                                                  p_reason_code,
                                                  SYSDATE);
      g_statement := 40;

      -- insert a row in the history / audit trail

      -- first get the amounts from the last entry in the history to carry forward in the history
      SELECT prh.data_warehouse_seat_cost,
             prh.data_warehouse_tax_cost,
             prh.stella_seat_cost,
             prh.stella_tax_cost,
             prh.unmatched_amt
        INTO v_data_warehouse_seat_cost,
             v_data_warehouse_tax_cost,
             v_stella_seat_cost,
             v_stella_tax_cost,
             v_unmatched_amt
        FROM pnr_reconciliation_history prh
       WHERE prh.pnr_id = p_record_id
         AND prh.process_date =
             (SELECT MAX(prh2.process_date)
                FROM pnr_reconciliation_history prh2
               WHERE prh2.pnr_id = prh.pnr_id);
      g_statement := 50;

      p_stella_reconciliation.update_recon_history(p_record_id,
                                                   'STELEXCP',
                                                   SYSDATE,
                                                   p_reason_code,
                                                   p_user_name,
                                                   v_data_warehouse_seat_cost,
                                                   v_data_warehouse_tax_cost,
                                                   v_stella_seat_cost,
                                                   v_stella_tax_cost,
                                                   v_unmatched_amt);

      g_statement := 60;

    ELSE
      -- must be a B reconcile type
      g_statement := 70;

      IF TRIM(v_reason_application_key) <> 'STLBSPR' THEN
        v_error_statement := 'Error, reason code ' || p_reason_code ||
                             ' is not valid for bsp exception';
        RAISE ex_data_invalid;
      END IF;

      -- first check that the reason code has not been changed by anyone else
      BEGIN

        SELECT p.reason_code
          INTO v_old_reason_code
          FROM bsp_transaction p
         WHERE p.bsp_trans_id = p_record_id;

        g_statement := 80;
        IF v_old_reason_code <> p_old_reason_code THEN
          v_error_statement := 'Error, this BSP transaction has already been moved to a different status (' ||
                               v_old_reason_code ||
                               ') by a different user/process. Refresh exceptions.';
          RAISE ex_data_invalid;
        END IF;
      EXCEPTION
        WHEN no_data_found THEN
          v_error_statement := 'Error, bsp transaction cannot be found in table. Contact help desk (bsptransid:' ||
                               p_record_id;
          RAISE ex_data_invalid;
      END;

      -- now actually update the bsp transaction with new reason code
      g_statement := 90;
      UPDATE bsp_transaction b
         SET b.reconciled_ind       = v_matched_ind,
             b.last_reconciled_date = SYSDATE,
             b.reason_code          = p_reason_code
       WHERE b.bsp_trans_id = p_record_id RETURNING b.ticket_no,
       b.refund_document_no INTO v_bsp_ticket_no,
       v_bsp_refund_document_no;

      IF SQL%ROWCOUNT = 0 THEN
        v_error_statement := 'Error, bsp transaction cannot be found in table for update. Contact help desk (bsptransid:' ||
                             p_record_id || ')';
        RAISE ex_data_invalid;
      END IF;
      IF SQL%ROWCOUNT > 1 THEN
        -- cannot happen if primary key used in update properly
        v_error_statement := 'Error, bsp transaction cannot be found in table -- too many rows. Contact help desk (bsptransid:' ||
                             p_record_id;
        RAISE ex_data_invalid;
      END IF;

      g_statement := 100;
      -- mutually exclusive -- either ticket number null , or refund document null
      IF v_bsp_ticket_no IS NOT NULL THEN
        UPDATE ticket t
           SET t.bsp_trans_id = p_record_id
         WHERE t.ticket_no = v_bsp_ticket_no;

        IF SQL%ROWCOUNT = 0 THEN
          v_error_statement := 'Error, bsp transaction cannot be found in table for update. Contact help desk (bsptransid:' ||
                               p_record_id;
          RAISE ex_data_invalid;
        END IF;
        IF SQL%ROWCOUNT > 1 THEN
          -- cannot happen if primary key used in update properly
          v_error_statement := 'Error, bsp transaction cannot be found in table -- too many rows. Contact help desk (bsptransid:' ||
                               p_record_id;
          RAISE ex_data_invalid;
        END IF;

      ELSE
        -- refund doc no must not be null
        g_statement := 110;
        UPDATE refund_batch rt
           SET rt.bsp_trans_id = p_record_id
         WHERE rt.refund_document_no = v_bsp_refund_document_no;
      END IF;

    END IF; -- if D record type

    RETURN NULL;

  EXCEPTION

    WHEN ex_data_invalid THEN
      ROLLBACK;
      RETURN v_error_statement;
    WHEN OTHERS THEN

      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      dbms_output.put_line('Excp others, state:' || g_statement);

      v_error_statement := 'Statement:' || to_number(g_statement) ||
                           g_sqlerrm;
      dbms_output.put_line(v_error_statement);
      dbms_output.put_line(g_sqlcode);

      RETURN 'Error, ' || v_error_statement;

  END update_exception_reason_code;

  -------------------------------------------------------------
  -------------------------------------------------------------

  FUNCTION insert_airline_user_alloc(p_airline_num     NUMBER,
                                     p_user_name       CHAR,
                                     p_amended_user_id CHAR) RETURN VARCHAR2 IS

    v_error_statement VARCHAR2(150) := '';

  BEGIN
    g_statement := 10;
    IF p_airline_num IS NULL THEN
      RETURN 'Error, airline number is blank';
    END IF;

    IF p_user_name IS NULL THEN
      RETURN 'Error, user name is blank';
    END IF;

    IF p_amended_user_id IS NULL THEN
      RETURN 'Error, amended user id is blank';
    END IF;
    g_statement := 20;

    BEGIN
      INSERT INTO airline_user_allocation
        (airline_num, user_name, amended_date, amended_user_id)
      VALUES
        (p_airline_num, p_user_name, SYSDATE, p_amended_user_id);

    EXCEPTION
      WHEN dup_val_on_index THEN
        g_statement := 30;
        RETURN 'Error, that airline already allocated to that user';
    END;

    g_statement := 40;

    COMMIT;
    RETURN NULL;

  EXCEPTION

    WHEN OTHERS THEN

      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      dbms_output.put_line('Excp others, state:' || g_statement);

      v_error_statement := 'Statement:' || to_number(g_statement) ||
                           g_sqlerrm;
      dbms_output.put_line(v_error_statement);
      dbms_output.put_line(g_sqlcode);

      RETURN 'Error, ' || v_error_statement;

  END insert_airline_user_alloc;
  -------------------------------------------------------------
  -------------------------------------------------------------

  FUNCTION delete_airline_user_alloc(p_airline_num NUMBER,
                                     p_user_name   CHAR)

   RETURN VARCHAR2 IS

    v_error_statement VARCHAR2(150) := '';

  BEGIN

    -- airline can be null if we want to delete all airlines
    g_statement := 10;
    -- also allow to ll mappings for a pclear out a for one airline
    IF p_user_name IS NULL AND p_airline_num IS NULL THEN
      RETURN 'Error, both user name and airline blank';
    END IF;
    g_statement := 20;

    IF p_user_name IS NULL THEN
      -- clear out all allocations for that airline
      DELETE FROM airline_user_allocation
       WHERE airline_num = p_airline_num;

    ELSIF p_airline_num IS NULL THEN
      g_statement := 25;
      -- clear out all allocations for that user
      DELETE FROM airline_user_allocation WHERE user_name = p_user_name;

    ELSE
      -- must both be supplied so delete just one allocation
      g_statement := 30;

      DELETE FROM airline_user_allocation
       WHERE airline_num = p_airline_num
         AND user_name = p_user_name;

    END IF;

    IF SQL%ROWCOUNT = 0 THEN
      RETURN 'Error, that airline/user/combination does not exist';
    END IF;

    g_statement := 35;
    COMMIT;
    RETURN NULL;

  EXCEPTION

    WHEN OTHERS THEN

      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      dbms_output.put_line('Excp others, state:' || g_statement);

      v_error_statement := 'Statement:' || to_number(g_statement) ||
                           g_sqlerrm;
      dbms_output.put_line(v_error_statement);
      dbms_output.put_line(g_sqlcode);

      RETURN 'Error, ' || v_error_statement;

  END delete_airline_user_alloc;

  -------------------------------------------------------------
  -------------------------------------------------------------

  FUNCTION delete_ticket(p_ticket_no CHAR) RETURN VARCHAR2 IS

    v_count           NUMBER := 0;
    v_pnr_id          pnr.pnr_id%TYPE;
    v_error_statement VARCHAR2(150) := '';
    v_ticket_no       NUMBER := 0;

  BEGIN

    dbms_output.put_line('tkt:' || p_ticket_no);
    g_statement := 10;

    IF NOT app_util.is_number(p_ticket_no) THEN
      RETURN 'Error, ticket number (' || p_ticket_no || ') is not numeric';
    END IF;

    v_ticket_no := p_ticket_no;

    SELECT COUNT(*)
      INTO v_count
      FROM refund_batch rb
     WHERE rb.refund_document_no IN
           (SELECT DISTINCT refund_document_no
              FROM refund_ticket rt
             WHERE rt.ticket_no = v_ticket_no);

    g_statement := 20;

    IF v_count > 0 THEN
      RETURN 'Error, that ticket has related refunds. Cannot delete';
    END IF;

    -- now check to see if the pnr would be left without any rows

    BEGIN
      g_statement := 30;
      SELECT pnr_id
        INTO v_pnr_id
        FROM ticket p
       WHERE pnr_id =
             (SELECT pnr_id FROM ticket rt WHERE rt.ticket_no = v_ticket_no);

    EXCEPTION
      WHEN no_data_found THEN
        g_statement := 40;
        RETURN 'Error, that ticket no. (' || p_ticket_no || ') not found. Could not delete';
      WHEN too_many_rows THEN
        -- more than one ticket on that pnr so don't delete the parent pnr row
        g_statement := 50;
        dbms_output.put_line(g_statement);

        DELETE FROM ticket WHERE ticket_no = v_ticket_no;
        RETURN NULL;
    END;

    g_statement := 60;
    dbms_output.put_line(g_statement);

    DELETE FROM ticket WHERE ticket_no = v_ticket_no;

    -- delete the related pnr as well
    g_statement := 70;
    dbms_output.put_line(g_statement);

    DELETE FROM pnr_reconciliation_history p WHERE p.pnr_id = v_pnr_id;

    g_statement := 80;
    dbms_output.put_line(g_statement);

    DELETE FROM pnr p WHERE p.pnr_id = v_pnr_id;

    COMMIT;

    RETURN NULL;

  EXCEPTION
    WHEN OTHERS THEN
      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      dbms_output.put_line('Excp others, state:' || g_statement);

      v_error_statement := 'Statement:' || to_number(g_statement) ||
                           g_sqlerrm;
      dbms_output.put_line(v_error_statement);
      dbms_output.put_line(g_sqlcode);

      RETURN 'Error, ' || v_error_statement;

  END delete_ticket;

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  ---refund letter stuff below
  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  /* return the address of the bsp accounts office from application_registry
  */

  FUNCTION get_office_address RETURN VARCHAR2 IS

    v_address application_registry.parameter_value%TYPE;

  BEGIN

    v_address := app_util.get_registry_property('STELLA',
                                                'LIVE',
                                                'ALL',
                                                'BSPAddress');

    RETURN v_address;

  END get_office_address;

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  /* return comma-spearated list of ticket numbers mentioned in a refund letter
  parameter: refund_leeter_id - mandatory */
  FUNCTION get_letter_ticket_list(p_refund_letter_id IN NUMBER)
    RETURN VARCHAR2 IS

    CURSOR c_tkt IS
      SELECT ticket_no
        FROM refund_letter_ticket rt
       WHERE rt.refund_letter_id = p_refund_letter_id;

    v_out VARCHAR2(2000) := '';

  BEGIN

    FOR x IN c_tkt LOOP
      v_out := v_out || to_char(x.ticket_no) || ',';
    END LOOP;

    IF substr(v_out, length(v_out), 1) = ',' THEN
      -- last char is  comma, remove it
      v_out := substr(v_out, 1, length(v_out) - 1);
    END IF;
    RETURN v_out;

  END get_letter_ticket_list; -- function get_letter_tickets

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_template_text(p_text_template_code IN NUMBER) RETURN VARCHAR2 IS

    v_text text_template.text%TYPE := NULL;

  BEGIN

    SELECT t.text
      INTO v_text
      FROM text_template t
     WHERE t.text_template_code = p_text_template_code;

    RETURN v_text;

  EXCEPTION
    WHEN no_data_found THEN
      RETURN 'Error, no text found for key:' || p_text_template_code;

  END get_template_text; -- end function

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_single_refund_letter(p_refund_letter_id IN NUMBER)
    RETURN p_stella_get_data.return_refcursor IS

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT DISTINCT rl.refund_letter_id,
                      rl.airline_num,
                      rl.entry_user_id,
                      rl.entry_date,
                      rl.requested_amt,
                      rl.free_text,
                      p_stella_get_data.get_letter_ticket_list(rl.refund_letter_id) ticket_list,
                      rl.amended_user_id,
                      rl.amended_date,
                      p.booking_reference_no,
                      p.season_type,
                      substr(p.season_year, 3, 2) season_year,
                      rl.entry_user_id,
                      rl.entry_date

        FROM refund_letter rl, refund_letter_ticket rlt, ticket t, pnr p
       WHERE rl.refund_letter_id = p_refund_letter_id
         AND rlt.refund_letter_id = rl.refund_letter_id
         AND t.ticket_no = rlt.ticket_no
         AND t.pnr_id = p.pnr_id;

    RETURN(v_result);

  END get_single_refund_letter; -- end function

  --==============================================================
  --==============================================================

  FUNCTION get_next_refund_letter_id RETURN NUMBER IS

    v_seq NUMBER;
  BEGIN

    SELECT refund_letter_seq.NEXTVAL INTO v_seq FROM dual;

    RETURN v_seq;

  END;

  --==============================================================
  --==============================================================

  FUNCTION insert_refund_letter(p_refund_letter_id IN NUMBER, -- should have come from refund_letter_seq which was previously retrieved by
                                p_airline_num      IN NUMBER,
                                p_entry_user_id    IN VARCHAR2,
                                p_requested_amt    IN NUMBER,
                                p_free_text        IN VARCHAR2,
                                p_ticket_list      IN VARCHAR2 -- make this an array??
                                ) RETURN VARCHAR2 IS

    v_error_statement    VARCHAR2(150) := '';
    v_ticket_no          refund_letter_ticket.ticket_no%TYPE := NULL;
    v_check              NUMBER := 0;
    v_error_tab          p_error.g_param_t; -- used in exception handling
    v_length             NUMBER;
    v_temp_ticket_string VARCHAR2(100);
    v_cur_char           CHAR(1);
    v_ins_count          NUMBER := 0;
    v_ticket_list        VARCHAR2(100);

  BEGIN

    g_statement := 10;
    IF p_refund_letter_id IS NULL THEN
      RETURN 'Error, SYSTEM ERROR refund letter id is blank';
    END IF;

    IF p_airline_num IS NULL THEN
      RETURN 'Error, airline number is blank';
    END IF;

    IF p_entry_user_id IS NULL THEN
      RETURN 'Error, entry user id is blank';
    END IF;

    IF p_ticket_list IS NULL THEN
      RETURN 'Error, ticket list is blank';
    END IF;

    IF p_requested_amt IS NULL THEN
      RETURN 'Error, requested letter amount is blank';
    END IF;

    IF p_free_text IS NULL THEN
      RETURN 'Error, free text is blank';
    END IF;

    IF length(p_free_text) < 20 THEN
      RETURN 'Error, free text is less than 20 chars in length';
    END IF;

    IF length(p_free_text) > 2000 THEN
      RETURN 'Error, free text is more than 2000 chars in length';
    END IF;

    IF length(p_ticket_list) < 8 THEN
      RETURN 'Error, invalid ticket list. Less than 8 chars in length';
    END IF;

    IF substr(p_ticket_list, 1, 1) IN (',', ' ') THEN
      RETURN 'Error, invalid ticket list. Cannot start with a comma or space';
    END IF;

    IF substr(p_ticket_list, length(p_ticket_list), 1) IN (',', ' ') THEN
      RETURN 'Error, invalid ticket list. Cannot end with a comma or space';
    END IF;

    -- test numeracy
    IF NOT app_util.is_number(p_refund_letter_id) THEN
      RETURN 'Error, refund letter id is not numeric:' || p_refund_letter_id;
    END IF;

    IF NOT app_util.is_number(p_airline_num) THEN
      RETURN 'Error, airline number is not numeric:' || p_airline_num;
    END IF;

    IF NOT app_util.is_number(p_requested_amt) THEN
      RETURN 'Error, amount is not numeric:' || p_requested_amt;
    END IF;

    BEGIN
      g_statement := 50;
      INSERT INTO refund_letter rl
        (refund_letter_id,
         airline_num,
         entry_user_id,
         entry_date,
         requested_amt,
         amended_user_id,
         amended_date,
         free_text)
      VALUES
        (p_refund_letter_id, -- should have come from refund_letter_seq which was previously retrieved by front end
         p_airline_num,
         p_entry_user_id,
         SYSDATE,
         p_requested_amt,
         NULL,
         NULL,
         p_free_text);

    EXCEPTION
      WHEN dup_val_on_index THEN
        ROLLBACK;
        RETURN 'Error, SYSTEM ERROR that refund letter id (' || p_refund_letter_id || ') already exists';

    END; -- insert block

    -- now insert the refund_letter_ticket rows
    -- go through each ticket number in list and process
    g_statement := 70;

    -- check that the ticket exists in main ticket table
    v_ticket_list := p_ticket_list || ',';
    v_length      := length(v_ticket_list);
    dbms_output.put_line('tkt:' || v_ticket_list);

    v_temp_ticket_string := '';

    FOR v_pos IN 1 .. v_length LOOP
      -- go through each char . if comma found then process ticket number that we must have
      v_cur_char  := substr(v_ticket_list, v_pos, 1);
      g_statement := 75;

      IF v_cur_char = ',' THEN

        -- process ticket
        IF NOT app_util.is_number(v_temp_ticket_string) THEN
          ROLLBACK;
          RETURN 'Error, ticket number is not numeric:' || v_temp_ticket_string;
        END IF;

        v_ticket_no := v_temp_ticket_string;

        BEGIN
          g_statement := 80;
          SELECT 1
            INTO v_check
            FROM ticket t
           WHERE t.ticket_no = v_ticket_no;
        EXCEPTION
          WHEN no_data_found THEN
            g_statement := 85;
            ROLLBACK;
            RETURN 'Error, that ticket (' || v_ticket_no || ') does not exist in ticket table. Letter id was:' || p_refund_letter_id;
        END;

        BEGIN
          g_statement := 83;
          SELECT 1
            INTO v_check
            FROM ticket t
           WHERE t.ticket_no = v_ticket_no
             AND t.airline_num = p_airline_num;
        EXCEPTION
          WHEN no_data_found THEN
            g_statement := 84;
            ROLLBACK;
            RETURN 'Error, that ticket (' || v_ticket_no || ') does not exist in ticket table FOR THAT AIRLINE (' || p_airline_num || '). Letter id was:' || p_refund_letter_id;
        END;

        g_statement := 90;
        BEGIN
          dbms_output.put_line('ins:' || v_ticket_no);
          INSERT INTO refund_letter_ticket
            (refund_letter_id, ticket_no)
          VALUES
            (p_refund_letter_id, v_ticket_no);
          v_ins_count := v_ins_count + 1;
        EXCEPTION
          WHEN dup_val_on_index THEN
            g_statement := 95;
            ROLLBACK;
            RETURN 'Error, duplicate ticket in the letter (' || v_ticket_no || '). Letter id was:' || p_refund_letter_id;

        END; -- insert ticket block

        v_temp_ticket_string := ''; -- reset current ticket string
      ELSE
        v_temp_ticket_string := v_temp_ticket_string || v_cur_char; -- build up ticket string
      END IF;

    END LOOP;
    IF v_ins_count < 1 THEN
      ROLLBACK;
      RETURN 'Error, no tickets were inserted';
    END IF;

    COMMIT;

    RETURN NULL;

  EXCEPTION
    WHEN OTHERS THEN

      g_sqlerrm := substr(SQLERRM, 1, 150);
      g_sqlcode := SQLCODE;
      ROLLBACK;
      dbms_output.put_line('Excp others, state:' || g_statement);

      v_error_statement := 'Statement:' || to_number(g_statement);
      dbms_output.put_line(v_error_statement);
      dbms_output.put_line(g_sqlcode);
      IF g_sqlcode = -2291 THEN
        -- foreign key raised, give decent error message to users
        -- buld up table of values to be passed to users via user-friendly error message

        p_error.add_error_parameter(v_error_tab,
                                    'val_airline',
                                    p_airline_num);
        p_error.add_error_parameter(v_error_tab, 'val_ticket', v_ticket_no);
        p_error.add_error_parameter(v_error_tab,
                                    'val_refletid',
                                    p_refund_letter_id);

        -- get user-friendly error message
        v_error_statement := '(fk) ' ||
                             p_error.get_constraint_error(g_sqlerrm,
                                                          v_error_tab);
      ELSE
        -- other error, just send back sqlerrm
        v_error_statement := v_error_statement || ' ' || g_sqlerrm;
      END IF;

      dbms_output.put_line(v_error_statement);

      RETURN 'Error, ' || v_error_statement;

  END insert_refund_letter;

  --==============================================================
  --==============================================================
  FUNCTION get_refund_reasons RETURN p_stella_get_data.return_refcursor IS

    /* return full list of enabled reason codes and descriptions
      used in refund doc entry
    */

    v_result p_stella_get_data.return_refcursor;

  BEGIN

    OPEN v_result FOR

      SELECT refund_reason_code,
             description,
             enabled_ind,
             amended_user_id,
             amended_date,
             allow_additional_free_text_ind

        FROM refund_reason r
       WHERE enabled_ind <> 'N'
       ORDER BY r.refund_reason_code;

    RETURN(v_result);

  END get_refund_reasons;

  ----------------------------------------------------------------
  ----------------------------------------------------------------


  FUNCTION sp_populate_travelink_ref(p_pnr_no            IN pnr.pnr_no%TYPE,
                                     p_pnr_creation_date IN CHAR) RETURN CHAR IS

    v_booking_ref VARCHAR2(15) := '0';

  BEGIN
    SELECT booking_ref
      INTO v_booking_ref
      FROM tlink_view tv1
     WHERE upper(tv1.pnr_no) = upper(p_pnr_no)
       AND to_date(trunc(tv1.booking_date), 'yymmdd') =
           to_date(p_pnr_creation_date, 'yymmdd')
       AND tv1.last_amendment_date =
           (SELECT MAX(tv2.last_amendment_date)
              FROM tlink_view tv2
             WHERE tv2.pnr_no = tv1.pnr_no
               AND tv2.booking_date = tv1.booking_date);

    RETURN v_booking_ref;

  EXCEPTION
    WHEN no_data_found THEN
      RETURN '0';
    WHEN OTHERS THEN
      RETURN 'Error while populating Traveling Ref in sp_populate_travelink_ref() procedure :' || SQLCODE || substr(SQLERRM,
                                                                                                                 1,
                                                                                                                 150);

  END sp_populate_travelink_ref;




  ----------------------------------------------------------------
  ----------------------------------------------------------------
  --  V.11 This function is written for specialist business
  -- to populate branch code from  the passed branch_group_code A,L,S,M,C
  -- called in insert_ticket

  FUNCTION get_specialist_branchcode(p_group       IN branch_group_allocation.group_code%TYPE,
                                     p_season_year season.season_year%TYPE,
                                     p_season_type season.season_type%TYPE)
    RETURN CHAR IS
    -- p_stella_get_data.return_refcursor IS

    --  v_result p_stella_get_data.return_refcursor;
    v_branch_code branch.branch_code%TYPE;

  BEGIN

    --    OPEN v_result FOR

    SELECT branch_code
      INTO v_branch_code
      FROM branch_group_allocation
     WHERE group_code = p_group
       AND season_type = p_season_type
       AND season_year = p_season_year
       AND rownum = 1; -- to make sure we always get one row

    RETURN v_branch_code;



  END get_specialist_branchcode;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------

  FUNCTION get_specialist_branchlist
    RETURN p_stella_get_data.return_refcursor IS

    v_result p_stella_get_data.return_refcursor;

  BEGIN
    OPEN v_result FOR
      SELECT DISTINCT b.branch_code, b.description
        FROM branch b, branch_group_allocation bg
       WHERE b.branch_code = bg.branch_code
         AND b.season_type = bg.season_type
         AND b.season_year = bg.season_year
         AND bg.group_code IN ('A', 'L', 'S', 'M', 'C', 'P', 'W', 'U', 'J', 'X', 'Y', 'Z');

    RETURN(v_result);
  END get_specialist_branchlist; -- end function

END p_stella_get_data; -- end of package body

/
