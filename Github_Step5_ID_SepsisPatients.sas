/*Data management to ID Sepsis Patients for telephone outreach*/
libname sepsis '/data/dart/2021/Data/inpatient'; 
libname edis '/data/dart/2021/Data/EDIS';
libname vital '/data/dart/2021/Data/vitals';
libname proc '/data/dart/2021/Data/procedures';
libname labs '/data/dart/2021/Data/labs';
libname meds '/data/dart/2021/Data/meds';
libname pulseox '/data/dart/2021/Data/pulse ox';
libname phone '/data/dart/2021/Data/Telephone Cohort';
/*set today's date*/
%let todaysdate=%sysfunc(today(), yymmddn8.); %put &todaysdate;

/*merge all EDIS, meds, vitals, pulseox, etc from step 4 to step 2 (sepsis.VAtoVA_daily_&todaysdate)*/
/*get earliest hospital admission for each hospitalization*/
proc sort data=sepsis.VAtoVA_daily_&todaysdate 
 out=earliest_hospadmit (compress=yes keep=patienticn unique_hosp_count_id specialtytransferdatetime);
by unique_hosp_count_id specialtytransferdatetime ;
run;

proc sort data=earliest_hospadmit nodupkey;
by unique_hosp_count_id ;
run;

/*EDIS: Has an EDIS record in the 12 hours prior to hospital admission*/
PROC SQL;
	CREATE TABLE VAPD_v1  (compress=yes)  AS
	SELECT A.*, c.specialtytransferdatetime as earliesthospitaladmission, b.PatientArrivalDateTime as earliest_EDISArrivalTime_hosp
	FROM  sepsis.VAtoVA_daily_&todaysdate   A
	left join earliest_hospadmit c on a.unique_hosp_count_id=c.unique_hosp_count_id
	LEFT JOIN edis.Edis_Clean&todaysdate  B	ON A.patienticn=B.patienticn ;
QUIT; RUN;

data vapd_v1_edis (compress=yes keep=patienticn EDIS_admit_hosp unique_hosp_count_id hour_diff keep earliest_EDISArrivalTime_hosp earliesthospitaladmission) ;
set VAPD_v1;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,earliesthospitaladmission); 
if  0=<hour_diff<=12 then keep=1; /*keep if 12 hours prior to hospital admission*/
if keep=1;
EDIS_admit_hosp=1;
run;

proc sort data=vapd_v1_edis;
by unique_hosp_count_id earliest_EDISArrivalTime_hosp;
run;

proc sort data=vapd_v1_edis nodupkey;
by unique_hosp_count_id ;
run;

/*merge earliesthospitaladmission, EDIS_admit_hosp and EDISArrivalTime back to sepsis.VAtoVA_daily_&todaysdate */
PROC SQL;
	CREATE TABLE V1_VAtoVA_daily_&todaysdate  (compress=yes)  AS
	SELECT A.*, c.specialtytransferdatetime as earliesthospitaladmission, b.earliest_EDISArrivalTime_hosp, b.EDIS_admit_hosp
	FROM  sepsis.VAtoVA_daily_&todaysdate   A
	left join earliest_hospadmit c on a.unique_hosp_count_id=c.unique_hosp_count_id
	LEFT JOIN vapd_v1_edis B ON A.unique_hosp_count_id =B.unique_hosp_count_id;
QUIT; RUN;
/****************************************************************************************************/

/*2+ SIRS criteria on presentation during the 72-hour window*/
/*2 or more of the following criteria present during the 72-hour window (24 prior to ED arrival through 48 hours after ED arrival)
•	White blood cell (WBC) <4k or >12k
•	Heart Rate > 90; 
•	Body Temp >38C or <36C; 
•	Respiratory Rate>20*/

/*merge in vitals data*/
/*WBC*/
/*for each patient, merge in the labs, one to many merge*/
PROC SQL;
	CREATE TABLE labs_wbc (compress=yes)  AS 
	SELECT A.*, B.LabChemSpecimenDateTime, b.LabSpecimenDate, b.LabChemResultNumericValue as wbc_value
	FROM V1_VAtoVA_daily_&todaysdate A
	LEFT JOIN  labs.WBC_Clean&todaysdate   B ON A.patienticn=B.patienticn;
QUIT; RUN;

/*creat if labs & vitals are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables*/
DATA wbc_fromED_72hr_keep (compress=yes); 
SET labs_wbc;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,LabChemSpecimenDateTime); 
if  -24=<hour_diff<=48 then fromED_72hr_keep=1;  /*keep the labs within the 72 hours window*/
if fromED_72hr_keep=1;
RUN;

/*each ED arrival can have multiple labs within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=wbc_fromED_72hr_keep;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE wbc_fromED_72hr&todaysdate (compress=yes)  AS   
SELECT *, min(wbc_value) as lo_wbc_72hrED, max(wbc_value) as hi_wbc_72hrED
FROM wbc_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=wbc_fromED_72hr&todaysdate  nodupkey; 
BY  unique_hosp_count_id lo_wbc_72hrED hi_wbc_72hrED;
RUN;

DATA wbc_fromED_72hr&todaysdate (compress=yes);
SET  wbc_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_wbc_72hrED hi_wbc_72hrED;
RUN;

/*RESPIRATION*/
/*for each patient, merge in the vitals, one to many merge*/
PROC SQL;
	CREATE TABLE vitals_resp  (compress=yes)  AS 
	SELECT A.*, B.VitalSignTakenDateTime, b.vital_date, b.VitalResultNumeric as resp_value
	FROM V1_VAtoVA_daily_&todaysdate  A
	LEFT JOIN vital.RESPIRATION_Clean&todaysdate  B	ON A.patienticn=B.patienticn;
QUIT; RUN;

/*creat if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables*/
DATA resp_fromED_72hr_keep (compress=yes); 
SET vitals_resp;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp, VitalSignTakenDateTime);  
if  -24=<hour_diff<=48 then keep=1; /*keep the vitals within the 72 hours window*/
if keep=1;
RUN;

/*each ED arrival can have multiple vitals within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=resp_fromED_72hr_keep;
BY unique_hosp_count_id VitalSignTakenDateTime;
RUN;

/*get the worst (lowest) vitals value per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE Resp_fromED_72hr&todaysdate (compress=yes)  AS  
SELECT *, min(resp_value) as lo_resp_72hrED, max(resp_value) as hi_resp_72hrED
FROM resp_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=Resp_fromED_72hr&todaysdate nodupkey; 
BY  unique_hosp_count_id  lo_resp_72hrED  hi_resp_72hrED;
RUN;

DATA Resp_fromED_72hr&todaysdate (compress=yes);
SET Resp_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_resp_72hrED  hi_resp_72hrED;
RUN;

/*Temperature*/
/*for each patient, merge in the vitals, one to many merge*/
PROC SQL;
	CREATE TABLE vitals_temp  (compress=yes)  AS 
	SELECT A.*, B.VitalSignTakenDateTime, b.vital_date, b.VitalResultNumeric as temp_value
	FROM  V1_VAtoVA_daily_&todaysdate  A
	LEFT JOIN vital.TEMPERATURE_Clean&todaysdate B ON A.patienticn=B.patienticn;
QUIT; RUN;

/*creat if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables*/
DATA temp_fromED_72hr_keep (compress=yes); 
SET vitals_temp;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,VitalSignTakenDateTime);  
if  -24=<hour_diff<=48 then keep=1; /*keep the vitals within the 72 hours window*/
if keep=1;
RUN;

/*each ED arrival can have multiple vitals within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=temp_fromED_72hr_keep;
BY unique_hosp_count_id VitalSignTakenDateTime;
RUN;

/*get the worst (lowest) vitals value per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE temp_fromED_72hr&todaysdate (compress=yes)  AS  
SELECT *, min(temp_value) as lo_temp_72hrED, max(temp_value) as hi_temp_72hrED
FROM temp_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=temp_fromED_72hr&todaysdate nodupkey; 
BY  unique_hosp_count_id  lo_temp_72hrED  hi_temp_72hrED;
RUN;

DATA temp_fromED_72hr&todaysdate (compress=yes);
SET temp_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_temp_72hrED  hi_temp_72hrED;
RUN;

/*Pulse*/
/*for each patient, merge in the vitals, one to many merge*/
PROC SQL;
	CREATE TABLE vitals_pulse  (compress=yes)  AS 
	SELECT A.*, B.VitalSignTakenDateTime, b.vital_date, b.VitalResultNumeric as pulse_value
	FROM  V1_VAtoVA_daily_&todaysdate  A
	LEFT JOIN vital.Pulse_Clean&todaysdate B ON A.patienticn=B.patienticn;
QUIT; RUN;

/*creat if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables*/
DATA pulse_fromED_72hr_keep (compress=yes); 
SET vitals_pulse ;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,VitalSignTakenDateTime); 
if  -24=<hour_diff<=48 then keep=1; /*keep the vitals within the 72 hours window*/
if keep=1;
RUN;

/*each ED arrival can have multiple vitals within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=pulse_fromED_72hr_keep ;
BY unique_hosp_count_id VitalSignTakenDateTime;
RUN;

/*get the worst (lowest) vitals value per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE pulse_fromED_72hr&todaysdate (compress=yes)  AS  
SELECT *, min(pulse_value) as lo_pulse_72hrED, max(pulse_value) as hi_pulse_72hrED
FROM pulse_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=pulse_fromED_72hr&todaysdate nodupkey; 
BY  unique_hosp_count_id  lo_pulse_72hrED  hi_pulse_72hrED;
RUN;

DATA pulse_fromED_72hr&todaysdate (compress=yes);
SET pulse_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_pulse_72hrED  hi_pulse_72hrED;
RUN;

/*merge all the labs & vitals back to VAPD*/
PROC SQL;
	CREATE TABLE  V2_VAtoVA_daily_&todaysdate (compress=yes)  AS 
	SELECT A.*, B.lo_pulse_72hrED, b.hi_pulse_72hrED,  c.lo_temp_72hrED, c.hi_temp_72hrED, 
           d.lo_resp_72hrED, d.hi_resp_72hrED,  e.lo_wbc_72hrED, e.hi_wbc_72hrED
	FROM  V1_VAtoVA_daily_&todaysdate  A
	LEFT JOIN pulse_fromED_72hr&todaysdate B ON A.patienticn =B.patienticn and A.unique_hosp_count_id =B.unique_hosp_count_id
    LEFT JOIN temp_fromED_72hr&todaysdate  c ON A.patienticn =c.patienticn and A.unique_hosp_count_id =c.unique_hosp_count_id
    LEFT JOIN resp_fromED_72hr&todaysdate  d ON A.patienticn =d.patienticn and A.unique_hosp_count_id =d.unique_hosp_count_id
    LEFT JOIN WBC_FROMED_72HR&todaysdate  e ON A.patienticn =e.patienticn and A.unique_hosp_count_id =e.unique_hosp_count_id;
QUIT; RUN;

/*Define SIRS+: SIRS is defined as 2 or more:
•	Temp > 100.4 or < 96.8,
•	Heart Rate > 90 ,
•	Respiratory Rate > 20 ,
•	White Blood Cells > 12K or < 4K */
/*revised with the 72 hour window, this "day" values doesn't matter anymore. 24hrs before ED presentation to 48hrs after ED presentation.*/
DATA ED_only_SIRS (compress=yes);
SET  V2_VAtoVA_daily_&todaysdate;
if EDIS_admit_hosp=1;
if (lo_temp_72hrED < 96.8 and lo_temp_72hrED NE .)  or (hi_temp_72hrED > 100.4 and hi_temp_72hrED NE . )  
     then SIRS_temp=1; else SIRS_temp=0;
if (hi_pulse_72hrED > 90 and hi_pulse_72hrED NE . ) 
     then SIRS_pulse=1; else SIRS_pulse=0;
if (hi_resp_72hrED > 20 and hi_resp_72hrED NE . ) 
     then SIRS_rr=1; else SIRS_rr=0;
if (hi_wbc_72hrED > 12 and hi_wbc_72hrED NE . ) or (lo_wbc_72hrED < 4 and lo_wbc_72hrED NE .) 
     then SIRS_wbc=1; else SIRS_wbc=0;
RUN;

/*Count Sum of SIRs indicators*/
PROC SQL;
CREATE TABLE ED_only_SIRS_V1 (compress=yes)  AS 
SELECT *, sum(SIRS_temp,SIRS_pulse,SIRS_rr, SIRS_wbc) as sum_SIRS_count
FROM ED_only_SIRS;
QUIT; RUN;

/* SIRS is defined as 2 or more*/
DATA ED_only_SIRS_V2 (compress=yes);
SET ED_only_SIRS_V1;
if sum_SIRS_count >=2 then newSIRS_hosp_ind=1; else newSIRS_hosp_ind=0;
keep unique_hosp_count_id SIRS_temp SIRS_pulse SIRS_rr SIRS_wbc sum_SIRS_count admityear newSIRS_hosp_ind;
RUN;

PROC SORT DATA=ED_only_SIRS_V2  nodupkey; /*get hosp-level SIRS counts*/ 
BY unique_hosp_count_id SIRS_temp SIRS_pulse SIRS_rr SIRS_wbc sum_SIRS_count;
RUN;

PROC FREQ DATA=ED_only_SIRS_V2;
TABLE admityear;
RUN;

/*merge the SIRS indicators back to VAPD*/
PROC SQL;
	CREATE TABLE V3_VATOVA_DAILY_&todaysdate   (compress=yes)  AS 
	SELECT A.*, B.newSIRS_hosp_ind
	FROM  ED_only_SIRS_V1 A
	LEFT JOIN ED_only_SIRS_V2 B ON A.unique_hosp_count_id =B.unique_hosp_count_id ;
QUIT; RUN;

data SIRS_ED_only (compress=yes);
set V3_VATOVA_DAILY_&todaysdate ;
if newSIRS_hosp_ind=1;
run;

/****************************************************************************************/
/*1+ acute organ dysfunction during the 72-hour window*/
/*At least 1 of the following acute organ dysfunction is present during 72-hour window (24 prior to ED arrival through 48 hours after ED arrival) */

data ED_only_SIRS_cohort (compress=yes);
set SIRS_ED_only;
keep unique_hosp_count_id patienticn new_admitdate3 new_dischargedate3 earliest_EDISArrivalTime_hosp earliesthospitaladmission;
run;
proc sort data=ED_only_SIRS_cohort nodupkey;
by unique_hosp_count_id ;
run;

data ED_only_SIRS_cohort_daily (compress=yes);
set SIRS_ED_only;
EDISArrivalDate=datepart(earliest_EDISArrivalTime_hosp);
EDISArrivalDatePlus1=EDISArrivalDate+1;
format EDISArrivalDate mmddyy10. EDISArrivalDatePlus1 mmddyy10.;
if datevalue=EDISArrivalDatePlus1 then DayAfterEDarrival=1;
if datevalue=EDISArrivalDate then DayofEDArrvial=1;
keep unique_hosp_count_id patienticn patientsid EDISArrivalDate new_admitdate3 new_dischargedate3 datevalue earliest_EDISArrivalTime_hosp
earliesthospitaladmission EDISArrivalDatePlus1 DayAfterEDarrival DayofEDArrvial;
run;

/*(1)Shock (Receipt of any of the following vasopressors by intravenous route: Dopamine, Norepinephrine, Epinephrine, Phenylephrine, Vasopressin)*/
/*merge the Pressors back to VAPD to get 72 hr window*/
PROC SQL;
	CREATE TABLE all_pressors  (compress=yes)  AS 
	SELECT A.*, B.ActionDateTime as pressorDateTime, b.BCMA_pressor_daily
	FROM  ED_only_SIRS_cohort A
	LEFT JOIN  meds.BCMA_PressorClean&todaysdate B ON A.patienticn=B.patienticn;
QUIT; 

/*creat if pressors are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables*/
DATA all_pressors72Hr (compress=yes);
SET  all_pressors;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,pressorDateTime); 
if  -24=<hour_diff<=48 then keep=1; /*keep the vitals within the 72 hours window*/
if keep=1 and BCMA_pressor_daily=1;
Pressor_FROMED_72HR=1;
RUN;

/*each ED arrival can have multiple pressor orders within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=all_pressors72Hr;
BY unique_hosp_count_id pressorDateTime;
RUN;

/*get the earliest CPRS Pressors order per hospitalization within that 72 hour window of ED arrival*/
PROC SORT DATA=all_pressors72Hr  nodupkey  OUT=Pressor_FROMED_72HR&todaysdate  (compress=yes); 
BY  unique_hosp_count_id;
RUN;


/*******************************************************************************************/
/*(2)	Acute kidney/renal dysfunction: Meets all 3 criteria:*/

/*get diagnosis codes back for ICD10 for end-stage renal disease*/
data diag (compress=yes);
set sepsis.VAtoVA_dailyDiag_&todaysdate;
if icd10code1 = 'N18.6' or icd10code2 = 'N18.6' or icd10code3 = 'N18.6' or icd10code4 = 'N18.6' or icd10code5 = 'N18.6' or icd10code6 = 'N18.6' or 
icd10code7 = 'N18.6' or icd10code8 = 'N18.6' or icd10code9 = 'N18.6' or icd10code10 = 'N18.6' or icd10code11 = 'N18.6' or icd10code12 = 'N18.6' or 
icd10code13 = 'N18.6' or icd10code14 = 'N18.6' or icd10code15 = 'N18.6' or icd10code16 = 'N18.6' or icd10code17 = 'N18.6' or icd10code18 = 'N18.6' or 
icd10code19 = 'N18.6' or icd10code20 = 'N18.6' or icd10code21 = 'N18.6' or icd10code22 = 'N18.6' or icd10code23 = 'N18.6' or icd10code24 = 'N18.6' or icd10code25 = 'N18.6'
 then renal_diag=1; 
if renal_diag=1;
run;

proc sort data=diag nodupkey;
by patienticn new_admitdate3 new_dischargedate3;
run;

PROC SQL;
	CREATE TABLE diag_v2 (compress=yes)  AS
	SELECT A.*, B.renal_diag
	FROM ED_only_SIRS_cohort A
	LEFT JOIN  diag B ON A.patienticn=B.patienticn and a.new_admitdate3=b.new_admitdate3 and a.new_dischargedate3=b.new_dischargedate3;
QUIT; RUN;

data no_renal_cohort (compress=yes);
set  diag_v2;
if renal_diag NE 1;
run;

/*get creat labs*/
PROC SQL;
	CREATE TABLE labs_creat  (compress=yes)  AS 
	SELECT A.*, B.LabChemSpecimenDateTime, b.LabSpecimenDate, b.LabChemResultNumericValue as creat_value
	FROM  ED_only_SIRS_cohort  A
	LEFT JOIN  labs.CREATININE_CLEAN&todaysdate  B ON A.patienticn=B.patienticn;
QUIT; RUN;

/*create if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables: Serum creatinine >= 1.2 mg/dL*/
DATA creat_fromED_72hr_keep (compress=yes); 
SET labs_creat;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,LabChemSpecimenDateTime);  
if  -24=<hour_diff<=48 then fromED_72hr_keep=1;   /*keep the labs within the 72 hours window*/
if fromED_72hr_keep=1 ; 
RUN;

/*each ED arrival can have multiple labs within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=creat_fromED_72hr_keep;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE creat_fromED_72hr&todaysdate  (compress=yes)  AS   
SELECT *, min(creat_value) as lo_creat_72hrED, max(creat_value) as hi_creat_72hrED
FROM creat_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=creat_fromED_72hr&todaysdate  nodupkey; 
BY  unique_hosp_count_id lo_creat_72hrED hi_creat_72hrED;
RUN;

DATA creat_fromED_72hr&todaysdate  (compress=yes);
SET creat_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_creat_72hrED hi_creat_72hrED;
RUN;

/*baseline=lowest creatinine value during 24 hours prior to ED arrival through day of discharge */
DATA creat_hosp (compress=yes); 
SET labs_creat;
EDISarrivaltime24hrpre=intnx('second',earliest_EDISArrivalTime_hosp,-86400); /*convert 24 hr to seconds to be exact*/
format EDISarrivaltime24hrpre datetime20.; 
if (EDISarrivaltime24hrpre <=LabChemSpecimenDateTime) and (LabSpecimenDate<=new_dischargedate3) then hosp_keep=1;
if hosp_keep=1 ;
RUN;

PROC SORT DATA=creat_hosp;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE creat_fromhosp_&todaysdate  (compress=yes)  AS   
SELECT *, min(creat_value) as lo_creat_hosp, max(creat_value) as hi_creat_hosp
FROM creat_hosp
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=creat_fromhosp_&todaysdate  nodupkey; 
BY  unique_hosp_count_id lo_creat_hosp hi_creat_hosp;
RUN;

DATA creat_fromhosp_&todaysdate  (compress=yes);
SET creat_fromhosp_&todaysdate;
keep patienticn unique_hosp_count_id lo_creat_hosp hi_creat_hosp;
RUN;

/*merge in 72 hr window and hosp creat labs to no_renal_cohort*/
PROC SQL;
	CREATE TABLE labs_creat_v1  (compress=yes)  AS 
	SELECT A.*, B.lo_creat_hosp, b.hi_creat_hosp, c.lo_creat_72hrED, c.hi_creat_72hrED
	FROM  no_renal_cohort  A
	LEFT JOIN  creat_fromhosp_&todaysdate  B ON A.unique_hosp_count_id=B.unique_hosp_count_id
    LEFT JOIN  creat_fromED_72hr&todaysdate   C ON A.unique_hosp_count_id=C.unique_hosp_count_id;
QUIT; 

data aod_kidney (compress=yes);
set labs_creat_v1;
/*baseline=lowest creatinine value during 24 hours prior to ED arrival through day of discharge*/
 baseline =lo_creat_hosp;
/*code baseline50*/
baseline50=baseline*1.5;
/*create aod_liver*/
if (hi_creat_72hred NE . and hi_creat_72hred >= 1.2) AND (hi_creat_72hred NE . and hi_creat_72hred >= baseline50) 
       then aod_kidney=1; else aod_kidney=0;
if aod_kidney=1; 
run;

proc sort data=aod_kidney nodupkey;
by unique_hosp_count_id;
run;

/******************************************************************************************/
/*(3)	Acute liver dysfunction: Meets both criteria: 	*/

/*merge in bilirubin labs*/
PROC SQL;
	CREATE TABLE all_bili  (compress=yes)  AS 
	SELECT A.*, B.LabChemSpecimenDateTime, b.LabSpecimenDate, b.LabChemResultNumericValue as bili_value
	FROM  ED_only_SIRS_cohort A
	LEFT JOIN labs.BILIRUBIN_CLEAN&todaysdate  B ON A.patienticn=B.patienticn;
QUIT; 

/*bilie if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables: Serum biliinine >= 1.2 mg/dL*/
DATA bili_fromED_72hr_keep (compress=yes); 
SET all_bili ;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp, LabChemSpecimenDateTime);  /*positive value=after ED, negative value=prior ED*/
if  -24=<hour_diff<=48 then fromED_72hr_keep=1;   /*keep the labs within the 72 hours window*/
if fromED_72hr_keep=1 ; 
RUN;

/*each ED arrival can have multiple labs within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=bili_fromED_72hr_keep;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE bili_fromED_72hr&todaysdate  (compress=yes)  AS   
SELECT *, min(bili_value) as lo_bili_72hrED, max(bili_value) as hi_bili_72hrED
FROM bili_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=bili_fromED_72hr&todaysdate nodupkey; 
BY  unique_hosp_count_id lo_bili_72hrED hi_bili_72hrED;
RUN;

DATA bili_fromED_72hr&todaysdate (compress=yes);
SET bili_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_bili_72hrED hi_bili_72hrED;
RUN;

/*baseline=lowest biliinine value during 24 hours prior to ED arrival through day of discharge */
DATA bili_hosp (compress=yes); 
SET all_bili ;
EDISarrivaltime24hrpre=intnx('second',earliest_EDISArrivalTime_hosp,-86400); /*convert 24 hr to seconds to be exact*/
format EDISarrivaltime24hrpre datetime20.; 
if (EDISarrivaltime24hrpre <=LabChemSpecimenDateTime) and (LabSpecimenDate<=new_dischargedate3) then hosp_keep=1;
if hosp_keep=1 ;
RUN;

PROC SORT DATA=bili_hosp;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE bili_fromhosp_&todaysdate  (compress=yes)  AS   
SELECT *, min(bili_value) as lo_bili_hosp, max(bili_value) as hi_bili_hosp
FROM bili_hosp
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=bili_fromhosp_&todaysdate  nodupkey; 
BY  unique_hosp_count_id lo_bili_hosp hi_bili_hosp;
RUN;

DATA bili_fromhosp_&todaysdate  (compress=yes);
SET bili_fromhosp_&todaysdate;
keep patienticn unique_hosp_count_id lo_bili_hosp hi_bili_hosp;
RUN;

/*merge in 72 hr window and hosp bilirubin labs to  ED_only_SIRS_cohort*/
PROC SQL;
	CREATE TABLE labs_bili_v1  (compress=yes)  AS 
	SELECT A.*, B.lo_bili_hosp, b.hi_bili_hosp, c.lo_bili_72hrED, c.hi_bili_72hrED
	FROM   ED_only_SIRS_cohort A
	LEFT JOIN  bili_fromhosp_&todaysdate  B ON A.unique_hosp_count_id=B.unique_hosp_count_id
    LEFT JOIN  bili_fromED_72hr&todaysdate C ON A.unique_hosp_count_id=C.unique_hosp_count_id;
QUIT;

data aod_liver (compress=yes);
set labs_bili_v1;
/*baseline= lowest bilirubin value during 24 hours prior to ED arrival through day of discharge*/
baseline =lo_bili_hosp;
/*code baseline2x*/
baseline2x=baseline*2;
/*create aod_liver*/
if (hi_bili_72hred NE . and hi_bili_72hred >= 2) AND (hi_bili_72hred NE . and hi_bili_72hred >= baseline2x) 
       then aod_liver=1; else aod_liver=0;
if aod_liver=1;
run;

proc sort data=aod_liver nodupkey ; 
by unique_hosp_count_id;
run;

/*****************************************************************************************/
/*(4)	Acute hematologic/platelet dysfunction: (Meets both criteria: */
PROC SQL;
	CREATE TABLE all_PLATELETS  (compress=yes)  AS 
	SELECT A.*, B.LabChemSpecimenDateTime, b.LabSpecimenDate, b.LabChemResultNumericValue as PLATELETS_value
	FROM  ED_only_SIRS_cohort A
	LEFT JOIN labs.PLATELETS_CLEAN&todaysdate  B ON A.patienticn=B.patienticn;
QUIT; 

/*PLATELETS if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables: Serum PLATELETSinine >= 1.2 mg/dL*/
DATA PLATELETS_fromED_72hr_keep (compress=yes); 
SET all_PLATELETS ;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp, LabChemSpecimenDateTime);   /*positive value=after ED, negative value=prior ED*/
if  -24=<hour_diff<=48 then fromED_72hr_keep=1;   /*keep the labs within the 72 hours window*/
if fromED_72hr_keep=1 ; 
RUN;

/*each ED arrival can have multiple labs within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=PLATELETS_fromED_72hr_keep;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE PLATELETS_fromED_72hr&todaysdate  (compress=yes)  AS   
SELECT *, min(PLATELETS_value) as lo_PLATELETS_72hrED, max(PLATELETS_value) as hi_PLATELETS_72hrED
FROM PLATELETS_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=PLATELETS_fromED_72hr&todaysdate nodupkey; 
BY  unique_hosp_count_id lo_PLATELETS_72hrED hi_PLATELETS_72hrED;
RUN;

DATA PLATELETS_fromED_72hr&todaysdate (compress=yes);
SET PLATELETS_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_PLATELETS_72hrED hi_PLATELETS_72hrED;
RUN;

/*baseline=lowest PLATELETSinine value during 24 hours prior to ED arrival through day of discharge */
DATA PLATELETS_hosp (compress=yes); 
SET all_PLATELETS ;
EDISarrivaltime24hrpre=intnx('second',earliest_EDISArrivalTime_hosp,-86400); /*convert 24 hr to seconds to be exact*/
format EDISarrivaltime24hrpre datetime20.; 
if (EDISarrivaltime24hrpre <=LabChemSpecimenDateTime) and (LabSpecimenDate<=new_dischargedate3) then hosp_keep=1;
if hosp_keep=1 ;
RUN;

PROC SORT DATA=PLATELETS_hosp;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE PLATELETS_fromhosp_&todaysdate  (compress=yes)  AS   
SELECT *, min(PLATELETS_value) as lo_PLATELETS_hosp, max(PLATELETS_value) as hi_PLATELETS_hosp
FROM PLATELETS_hosp
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=PLATELETS_fromhosp_&todaysdate  nodupkey; 
BY  unique_hosp_count_id lo_PLATELETS_hosp hi_PLATELETS_hosp;
RUN;

DATA PLATELETS_fromhosp_&todaysdate  (compress=yes);
SET PLATELETS_fromhosp_&todaysdate;
keep patienticn unique_hosp_count_id lo_PLATELETS_hosp hi_PLATELETS_hosp;
RUN;

/*merge in 72 hr window and hosp PLATELETS labs to  ED_only_SIRS_cohort*/
PROC SQL;
	CREATE TABLE labs_PLATELETS_v1  (compress=yes)  AS 
	SELECT A.*, B.lo_PLATELETS_hosp, b.hi_PLATELETS_hosp, c.lo_PLATELETS_72hrED, c.hi_PLATELETS_72hrED
	FROM   ED_only_SIRS_cohort A
	LEFT JOIN  PLATELETS_fromhosp_&todaysdate  B ON A.unique_hosp_count_id=B.unique_hosp_count_id
    LEFT JOIN  PLATELETS_fromED_72hr&todaysdate C ON A.unique_hosp_count_id=C.unique_hosp_count_id;
QUIT;

data aod_heme (compress=yes);
set labs_PLATELETS_v1;
/*baseline=highest platelet count during 24 hours prior to ED arrival through day of discharge*/
 baseline=hi_PLATELETS_hosp;
/*code 50% of baseline*/
baseline50=baseline*0.5;
/*create aod_heme*/
if (lo_PLATELETS_72hred NE . and lo_PLATELETS_72hred<100) AND (lo_PLATELETS_72hred NE . and lo_PLATELETS_72hred < baseline50) 
       then aod_heme=1; else aod_heme=0;
if aod_heme=1;
run;

proc sort data=aod_heme nodupkey; 
by unique_hosp_count_id;
run;

/********************************************************************************************/
/*(5)	Lactate elevation: Lactate >= 2.0 mmol/L*/
/*merge in LACTATE labs*/
PROC SQL;
	CREATE TABLE all_LACTATE  (compress=yes)  AS 
	SELECT A.*, B.LabChemSpecimenDateTime, b.LabSpecimenDate, b.LabChemResultNumericValue as LACTATE_value
	FROM  ED_only_SIRS_cohort A
	LEFT JOIN labs.LACTATE_CLEAN&todaysdate  B ON A.patienticn=B.patienticn;
QUIT; 

/*LACTATE if labs are within 24 hrs prior to ED arrival and 48 hours after ED arrival variables: Serum LACTATEinine >= 1.2 mg/dL*/
DATA LACTATE_fromED_72hr_keep (compress=yes); 
SET all_LACTATE ;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,LabChemSpecimenDateTime);   /*positive value=after ED, negative value=prior ED*/
if  -24=<hour_diff<=48 then fromED_72hr_keep=1;   /*keep the labs within the 72 hours window*/
if fromED_72hr_keep=1 ; 
RUN;

/*each ED arrival can have multiple labs within that 72 hour window, sort the data first by admit (unique_hosp_count_id)*/
PROC SORT DATA=LACTATE_fromED_72hr_keep;
BY unique_hosp_count_id LabChemSpecimenDateTime;
RUN;

/*get the hi/lo lab values per hospitalization within that 72 hour window of ED arrival*/
PROC SQL;
CREATE TABLE LACTATE_fromED_72hr&todaysdate  (compress=yes)  AS   
SELECT *, min(LACTATE_value) as lo_LACTATE_72hrED, max(LACTATE_value) as hi_LACTATE_72hrED
FROM LACTATE_fromED_72hr_keep
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT; RUN;

PROC SORT DATA=LACTATE_fromED_72hr&todaysdate nodupkey; 
BY  unique_hosp_count_id lo_LACTATE_72hrED hi_LACTATE_72hrED;
RUN;

DATA LACTATE_fromED_72hr&todaysdate (compress=yes);
SET LACTATE_fromED_72hr&todaysdate;
keep patienticn unique_hosp_count_id lo_LACTATE_72hrED hi_LACTATE_72hrED;
RUN;

/*merge in 72 hr window lactate labs to  ED_only_SIRS_cohort*/
PROC SQL;
	CREATE TABLE labs_lactate_v1  (compress=yes)  AS 
	SELECT A.*, c.lo_lactate_72hrED, c.hi_lactate_72hrED
	FROM   ED_only_SIRS_cohort A
    LEFT JOIN  lactate_fromED_72hr&todaysdate C ON A.unique_hosp_count_id=C.unique_hosp_count_id;
QUIT;

/*aod_lactate*/
/*Acute Organ Dysfunction (Lactate):Lactate >= 2.0 mmol/L*/
data aod_lactate (compress=yes);
set  labs_lactate_v1;
if (hi_lactate_72hred >=2 ) or (lo_lactate_72hred >=2 ) then aod_lactate=1; else aod_lactate=0;
if aod_lactate=1;
run;

proc sort data=aod_lactate nodupkey; 
by unique_hosp_count_id;
run;

/******************************************************************************/
/*(6)	Acute respiratory/lung dysfunction: Meets at least 1 criteria:
•	receipt of invasive mechanical ventilation during any of the following days: the day of ED arrival or day after ED arrival 
•	receipt of at least 4LNC supplemental oxygen ABOVE baseline during the 72-hour window */

/*Merge in mech vent daily*/
PROC SQL;
	CREATE TABLE proccode_mechvent  (compress=yes)  AS 
	SELECT A.*, c.procdate, c.proccode_mechvent_daily
	FROM  ED_only_SIRS_cohort_daily A
    LEFT JOIN  proc.MECHVENT_CLEAN&todaysdate C ON A.patienticn=C.patienticn and a.datevalue=c.procdate;
QUIT;

data proccode_mechvent_v2  (compress=yes);
set proccode_mechvent;
if (DayAfterEDarrival=1 or DayofEDArrvial=1) and proccode_mechvent_daily=1 then mechvent_ind_hosp=1;
if mechvent_ind_hosp=1;
run;

proc sort data=proccode_mechvent_v2  nodupkey;
by unique_hosp_count_id;
run;

/*merge in PulseOx*/
PROC SQL;
	CREATE TABLE pulseox (compress=yes)  AS 
	SELECT A.*, c.vitalSignTakenDateTime, c.O2_GE4LPM_ind
	FROM  ED_only_SIRS_cohort A
    LEFT JOIN  pulseox.PULSEOX_CLEAN&todaysdate C ON A.patienticn=C.patienticn ;
QUIT;

/* within 24 hrs prior to ED arrival and 48 hours after ED arrival*/
DATA pulseox_fromED_72hr_keep (compress=yes); 
SET pulseox ;
hour_diff = INTCK('hour',earliest_EDISArrivalTime_hosp,vitalSignTakenDateTime);   /*positive value=after ED, negative value=prior ED*/
if  -24=<hour_diff<=48 then fromED_72hr_keep=1;   /*keep the labs within the 72 hours window*/
if fromED_72hr_keep=1 and  O2_GE4LPM_ind=1; 
O2_GE4LPM_72hr_keep=1;
RUN;

proc sort data=pulseox_fromED_72hr_keep  nodupkey;
by unique_hosp_count_id;
run;

/*merge in O2_GE4LPM_72hr_keep & mechvent_ind_hosp back to ED_only_SIRS_cohort*/
PROC SQL;
	CREATE TABLE Acute_respiratory (compress=yes)  AS 
	SELECT A.*, c.O2_GE4LPM_72hr_keep, d.mechvent_ind_hosp
	FROM  ED_only_SIRS_cohort A
    LEFT JOIN  pulseox_fromED_72hr_keep C ON A.unique_hosp_count_id=C.unique_hosp_count_id
    LEFT JOIN  proccode_mechvent_v2 D ON A.unique_hosp_count_id=D.unique_hosp_count_id;
QUIT;

data Acute_respiratory;
set Acute_respiratory;
if O2_GE4LPM_72hr_keep=1 or mechvent_ind_hosp=1 then Acute_respiratory_hosp=1;
if Acute_respiratory_hosp=1;
run;

proc sort data=Acute_respiratory  nodupkey;
by unique_hosp_count_id;
run;


/*left join all the organ dysfunctions back to SIRS_ED_only*/
PROC SQL;
	CREATE TABLE AOD_v1  (compress=yes)  AS 
	SELECT A.*, B.aod_liver, c.aod_kidney, d.Pressor_FROMED_72HR, e.aod_heme, f.aod_lactate, g.Acute_respiratory_hosp
	FROM  SIRS_ED_only A
	LEFT JOIN  aod_liver  B ON A.unique_hosp_count_id=B.unique_hosp_count_id
    LEFT JOIN  aod_kidney c ON A.unique_hosp_count_id=c.unique_hosp_count_id
    LEFT JOIN  Pressor_FROMED_72HR&todaysdate d ON A.unique_hosp_count_id=d.unique_hosp_count_id
    LEFT JOIN aod_heme  e ON A.unique_hosp_count_id=e.unique_hosp_count_id
    LEFT JOIN  aod_lactate f ON A.unique_hosp_count_id=f.unique_hosp_count_id
    LEFT JOIN  Acute_respiratory g ON A.unique_hosp_count_id=g.unique_hosp_count_id;
QUIT;

data AOD_v2 (compress=yes) ;
set AOD_v1;
if aod_liver=1 or aod_kidney=1 or Pressor_FROMED_72HR=1
or aod_heme=1 or aod_lactate=1 or Acute_respiratory_hosp=1 then any_AOD=1;
if any_AOD=1;
run;

/*Initiated on antimicrobial on day of ED arrival or day after ED arrival (or COVID)*/
/*Must meet either of the following criteria:
Received a BCMA antimicrobial on day of ED arrival or day after ED arrrival (same list of antimicrobials as HAPPI)
Discharged with a primary diagnosis of COVID (J12.82, U07.1)*/

data AOD_v2_cohort_daily (compress=yes);
set AOD_v2;
EDISArrivalDate=datepart(earliest_EDISArrivalTime_hosp);
EDISArrivalDatePlus1=EDISArrivalDate+1;
format EDISArrivalDate mmddyy10. EDISArrivalDatePlus1 mmddyy10.;
if datevalue=EDISArrivalDatePlus1 then DayAfterEDarrival=1;
if datevalue=EDISArrivalDate then DayofEDArrvial=1;
if icd10code1 in ('J12.82', 'U07.1') then covid_diag_hosp=1;
keep unique_hosp_count_id patienticn patientsid EDISArrivalDate new_admitdate3 new_dischargedate3 datevalue earliest_EDISArrivalTime_hosp
earliesthospitaladmission EDISArrivalDatePlus1 DayAfterEDarrival DayofEDArrvial icd10code1 covid_diag_hosp;
run;

data covid_diag_hosp;
set AOD_v2_cohort_daily;
if covid_diag_hosp=1;
run;

proc sort data=covid_diag_hosp  nodupkey;
by unique_hosp_count_id;
run;

/*merge back the BCMA ABX daily*/
PROC SQL;
	CREATE TABLE startABX  (compress=yes)  AS 
	SELECT A.*, B.BCMA_ABX_daily
	FROM AOD_v2_cohort_daily A
	LEFT JOIN   meds.BCMA_ABXCLEAN&todaysdate B ON A.patienticn=B.patienticn and a.datevalue=b.ActionDate;
QUIT;

data startABX_v2 (compress=yes);
set startABX;
if BCMA_ABX_daily=1 and (DayofEDArrvial=1 or DayAfterEDarrival=1) then startedABX_hosp=1;
if startedABX_hosp=1;
run;

proc sort data=startABX_v2  nodupkey;
by unique_hosp_count_id;
run;

/*merge startedABX_hosp & covid_diag_hosp back to AOD_v2*/
PROC SQL;
	CREATE TABLE startABX_cohort  (compress=yes)  AS 
	SELECT A.*, B.startedABX_hosp, c.covid_diag_hosp
	FROM AOD_v2 A
	LEFT JOIN  startABX_v2 B ON A.unique_hosp_count_id=B.unique_hosp_count_id
    Left join covid_diag_hosp C on A.unique_hosp_count_id=c.unique_hosp_count_id;
QUIT;

data startABX_cohort_v2 (compress=yes) ;
set startABX_cohort;
if startedABX_hosp=1 or covid_diag_hosp=1;
run;

/*Abx continued for 4+ days (or COVID)*/
/*merge back the BCMA ABX daily*/
PROC SQL;
	CREATE TABLE ABX_4days  (compress=yes)  AS 
	SELECT A.*, B.BCMA_ABX_daily
	FROM startABX_cohort_v2  A
	LEFT JOIN  meds.BCMA_ABXCLEAN&todaysdate B ON A.patienticn=B.patienticn and a.datevalue=b.ActionDate;
QUIT;

data ABX_4days_v2 (compress=yes);
set ABX_4days;
if BCMA_ABX_daily=. then delete;
keep unique_hosp_count_id patienticn patientsid new_admitdate3 new_dischargedate3 datevalue earliest_EDISArrivalTime_hosp
earliesthospitaladmission  covid_diag_hosp startedABX_hosp BCMA_ABX_daily;
run;

proc sort data=ABX_4days_v2;
by unique_hosp_count_id datevalue;
run;

proc sort data=ABX_4days_v2 nodupkey out= time_first_abx_date (compress=yes);
by unique_hosp_count_id;
run;

/*for each hosp, get time_first_abx_date and time_first_abx4days*/
PROC SQL;
	CREATE TABLE ABX_4days_v3 (compress=yes)  AS 
	SELECT A.*, B.datevalue as time_first_abx_date
	FROM ABX_4days_v2  A
	LEFT JOIN  time_first_abx_date B ON A.unique_hosp_count_id=B.unique_hosp_count_id;
QUIT;

data ABX_4days_v4 (compress=yes);
set ABX_4days_v3;
time_first_abx4days=time_first_abx_date+3; /*first day is 1 so 4 days is plus 3*/
format  time_first_abx4days mmddyy10.; 
if (datevalue >time_first_abx4days) or (datevalue <time_first_abx_date) then delete;
run;

proc sort data=ABX_4days_v4;
by unique_hosp_count_id datevalue;
run;

/*see if sum of any_abx_daily_ind is 4 days for each hosps*/
PROC SQL;
CREATE TABLE have4days  AS 
SELECT *, sum(BCMA_ABX_daily) as sum_any_abx_daily_ind
FROM ABX_4days_v4
GROUP BY unique_hosp_count_id
ORDER BY unique_hosp_count_id;
QUIT;

data have4days2 (compress=yes);
set have4days;
if sum_any_abx_daily_ind=4 then have4ABXdays_hosp=1;
if  have4ABXdays_hosp=1;
run;

proc sort data=have4days2 nodupkey; 
by unique_hosp_count_id;
run;

/*merge have4ABXdays_hosp back the ABX_4days dataset*/
PROC SQL;
	CREATE TABLE have4ABXdays_hosp  (compress=yes)  AS 
	SELECT A.*, B.have4ABXdays_hosp
	FROM ABX_4days  A
	LEFT JOIN  have4days2 B ON A.unique_hosp_count_id=B.unique_hosp_count_id;
QUIT;

/*Received inpatient BCMA antimicrobial on day of ED arrival and each of the next 3 days
OR Received inpatient BCMA antimicrobial on day after ED arrival and each of the next 3 days
OR Discharged with a primary diagnosis of COVID (J12.82, U07.1)*/

data have4ABXdays_hosp_v2 (compress=yes);
set have4ABXdays_hosp;
if have4ABXdays_hosp=1 or covid_diag_hosp=1;
run;

proc sort data=have4ABXdays_hosp_v2;
by unique_hosp_count_id datevalue;
run;


proc sort data=have4ABXdays_hosp_v2 nodupkey out=phone.sepsis_hosps&todaysdate (compress=yes);
by unique_hosp_count_id;
run;

proc sort data=phone.sepsis_hosps&todaysdate nodupkey 
 out=SepsisCohortPatients&todaysdate (compress=yes keep=patienticn patientsid /*sta3n*/ sta6a new_admitdate3 new_dischargedate3);
by patienticn;
run;
