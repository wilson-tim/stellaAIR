--------------------------------------------------------
--  DDL for Package P_STELLA_GET_DATA
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "STELLA"."P_STELLA_GET_DATA" IS
  -- Author  : leigh ashton
  -- Created : 03/01/03
  -- Purpose : Defines the procedures and functions for the Stella system
  --         : data extracts. These stored procedures / functions are used by java programs to
  --         : read data from Oracle

  -- V1.08   : JR, Airload changes, for Amadeus
  --         : Added  3 new parameters  in insert_ticket function , values are optional for last two
  --
  -- V1.10   : JR, Voided Air Changes for air tickets
  --         : Added nvl in update ticket sql for ticket_issue date, ticket agent inititals, tour code and fare basis code
  --         : to keep their original values when tickets are voided
  --
  -- V.11    : JR, added 3 functions for Specialist Business release
  -- V1.12   : removed any reference to jutil.security_user table (get_user_reason,et_user_exceptions).. LDAP security uses notes user.
  -- V 1.13  : added doc_type_code in insert_refund finction to allow tp update refund_doc_type

  -- Public type declarations
  TYPE return_refcursor IS REF CURSOR;

  g_version   NUMBER := 1.11;
  g_statement NUMBER := 0;
  g_sqlerrm   VARCHAR2(500);
  g_sqlcode   CHAR(20);

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- LA v1.05 added refund letter stuff
  -- LA v1.07 added reasons for refund docs
  ----------------------------------------------------------------

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- get first departure date of any flight on this pnr
  FUNCTION get_first_departure_date(p_pnr_id IN NUMBER) RETURN DATE;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- take branch code and return the appropriate season_type code based on the
  -- company code linked to this branch .e.g. summer H and Jarvis shows as C
  FUNCTION convert_season_type(p_in_season_type CHAR,
                               p_in_season_year CHAR,
                               p_in_branch_code CHAR) RETURN VARCHAR2;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  FUNCTION get_airline_user_allocations(p_user_name   IN CHAR,
                                        p_airline_num IN NUMBER)
    RETURN p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------

  FUNCTION get_user_reasons(p_user_name  IN CHAR,
                            p_user_roles IN VARCHAR2)
    RETURN p_stella_get_data.return_refcursor;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  FUNCTION get_all_reasons    
  
   RETURN p_stella_get_data.return_refcursor;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  /* return full list of all current exceptions for a particular user's associated airlines
    and where that user is allowed to see that exception
    used in exception screen
     V1.08 new parameter is added as part of Itour reconciliation , JR
  */

  FUNCTION get_user_exceptions(p_user_name             IN CHAR,
                               p_show_bsp_exceptions   IN CHAR,
                               p_show_dwhse_exceptions IN CHAR,
                               p_show_itour_exceptions IN CHAR,
                               p_show_tlink_exceptions IN CHAR,
                               p_specialist_branch     IN VARCHAR2,
                               p_user_roles            IN VARCHAR2)
    RETURN p_stella_get_data.return_refcursor;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  /* return full list of the reconciliation history for a particular pnr
  */
  FUNCTION get_reconciliation_history(p_pnr_id IN NUMBER)
    RETURN p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------

  /* return result set of refund batch and child tickets details
  used in refund screen*/
  FUNCTION get_refund_tickets(p_document_no NUMBER)
    RETURN p_stella_get_data.return_refcursor;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  /* return all details for a single ticket as a result set */
  FUNCTION get_single_ticket_details(p_in_ticket_no NUMBER)
    RETURN p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- **********************************************************************
  -- GET_ALL_AIRLINES()
  -- **********************************************************************
  -- This function returns the requested airline data from the
  -- airline table in the form of a result set.
  /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values */
  FUNCTION get_all_airlines RETURN p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- get single airline's full details,, return as result set
  -- function get_airline looks up a single airline using airline_Num and returns the airline name or -1 if none found
  /* RETURN ONE row from the table */
  FUNCTION get_airline(p_airline_num NUMBER)
    RETURN p_stella_get_data.return_refcursor; -- returns airline name if valid, returns "-1" if no airline found

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- get all branches and their details
  /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values*/
  FUNCTION get_all_branches RETURN p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- get all iata numbers from table, return as result set
  /* RETURN all rows from the table, no where clause - used to build complete lists of all possible values*/
  FUNCTION get_all_iata_details RETURN p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- get all rows from doc_type table
  /* RETURN all rows from the table UNLESS a specific doc type is passed
  in which case it only returns that one
  - used to build complete lists of all possible values*/
  FUNCTION get_all_doc_types(p_doc_category IN CHAR)
    RETURN p_stella_get_data.return_refcursor;

  ----------------------------------------------------------------
  ----------------------------------------------------------------
  -- stored proc to insert a ticket and pnr - used from either GUI or from batch MIR load
  /* all params mandatory UNLESS it is a void ticket (shown in passenger name field as VOID)
     returns null if successful, returns 'Error,.........' if failure
  
     V1.08 , JR added three new parameters
     p_pnr_creation_date,p_othertaxes,p_ticket_type
  
  */

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
                         p_othertaxes               CHAR DEFAULT NULL,
                         p_ticket_type              CHAR DEFAULT NULL,
                         p_group                    CHAR DEFAULT NULL
                         
                         
                         ) RETURN CHAR;
  ----------------------------------------------------------------
  ----------------------------------------------------------------

  -- insert a refund batch and a refund ticket row
  /*
     returns null if successful, returns 'Error,.........' if failure
  */

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
                         
                         ) RETURN CHAR;

  -------------------------------------------------------------
  -------------------------------------------------------------
  /* function to be called from exceptions screen to
  move exception onwards in the reconciliation process/workflow */
  /* all params mandatory
     returns null if successful, returns 'Error,.........' if failure
     p_reconcile_type
  this must be B for BSP exceptions, D for data warehouse/booking exceptions (the old ones)
  
  the parameter p_record_id must be populated
  with the bsp_Trans_id for bsp exceptions and the pnr_id for data warehouse/booking exceptions
  
  */

  FUNCTION update_exception_reason_code(p_reconcile_type  IN CHAR,
                                        p_record_id       IN NUMBER,
                                        p_reason_code     IN CHAR,
                                        p_user_name       IN CHAR,
                                        p_old_reason_code IN CHAR)
    RETURN CHAR;

  -------------------------------------------------------------
  -------------------------------------------------------------
  /*  add a new entry to the airline_user_allocation table*/
  /* all params mandatory
     returns null if successful, returns 'Error,.........' if failure
  */
  FUNCTION insert_airline_user_alloc(p_airline_num     NUMBER,
                                     p_user_name       CHAR,
                                     p_amended_user_id CHAR) RETURN VARCHAR2;

  -------------------------------------------------------------
  -------------------------------------------------------------
  /* delete an entry from the airline_user_allocation table*/
  /* username params mandatory
     airline num not mandatory - if null then all rows for that user will be deleted
     returns null if successful, returns 'Error,.........' if failure
  */
  FUNCTION delete_airline_user_alloc(p_airline_num NUMBER,
                                     p_user_name   CHAR) RETURN VARCHAR2;

  /* delete a ticket and all associated data */
  /* ticket no param is mandatory */
  FUNCTION delete_ticket(p_ticket_no CHAR) RETURN VARCHAR2;

  -----------------------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------
  /* return the address of the bsp accounts office from application_registry
  */

  FUNCTION get_office_address RETURN VARCHAR2;

  /* return comma-spearated list of ticket numbers mentioned in a refund letter
  parameter: refund_leeter_id - mandatory */
  FUNCTION get_letter_ticket_list(p_refund_letter_id IN NUMBER)
    RETURN VARCHAR2;

  /* return full details of a refund letter and associated tickets */
  /* params are mandatory */
  FUNCTION get_single_refund_letter(p_refund_letter_id IN NUMBER)
    RETURN p_stella_get_data.return_refcursor;

  /* insert a refund letter and  associated tickets
  all params mandatory
  returns null if ok, returns "Error,....." if an error has occurred
  */

  FUNCTION insert_refund_letter(p_refund_letter_id IN NUMBER, -- should have come from refund_letter_seq which was previously retrieved by
                                p_airline_num      IN NUMBER,
                                p_entry_user_id    IN VARCHAR2,
                                p_requested_amt    IN NUMBER,
                                p_free_text        IN VARCHAR2,
                                p_ticket_list      IN VARCHAR2 -- make this an array??
                                ) RETURN VARCHAR2;

  /* get next sequence of refund letter id for use as primary key of that table */
  FUNCTION get_next_refund_letter_id RETURN NUMBER;

  /* get text to be used as template in a refund letter */
  FUNCTION get_template_text(p_text_template_code IN NUMBER) RETURN VARCHAR2;

  /* return full list of enabled reason codes and descriptions
    used in refund doc entry
  */
  FUNCTION get_refund_reasons RETURN p_stella_get_data.return_refcursor;


  /* This function is called from Airload program for H&J and other specialist brands
     where it can not read Travelink bookingref from air file it tries to search in the table
     tlink_view using pnr_no and pnr_creation date
  
     Note : As per Henry booking_date in Travelink will not be same as pnr_creation date
     as in scheduled allocations it creates pnr in Amadeus which never goes back into travelink
  
  */

  FUNCTION sp_populate_travelink_ref(p_pnr_no            IN pnr.pnr_no%TYPE,
                                     p_pnr_creation_date IN CHAR) RETURN CHAR;



  /* retuns brnach code (USAC/HAJS/SOVE/MEON/CITA etc) from prefix passed as A,L,S,M,C for specialist companies */
  FUNCTION get_specialist_branchcode(p_group       IN branch_group_allocation.group_code%TYPE,
                                     p_season_year season.season_year%TYPE,
                                     p_season_type season.season_type%TYPE)
    RETURN CHAR;
  --p_stella_get_data.return_refcursor;
  ----------------------------------------------------------------
  ----------------------------------------------------------------
  --returns list of specialist branches , return as result set
  FUNCTION get_specialist_branchlist
    RETURN p_stella_get_data.return_refcursor;

  TYPE g_list_element_type IS RECORD(
    list_element          VARCHAR2(4000),
    list_element_length   NUMBER(9, 0),
    list_element_position NUMBER(9, 0));

  TYPE g_list_tab_t IS TABLE OF g_list_element_type;

END p_stella_get_data; -- end of package HEADER

/
