/*Author: Shirley Wang (xiaoqing.wang@va.gov)*/
/*Clean datasets pulled in on labs, vitals, etc.*/
%let todaysdate=%sysfunc(today(), yymmddn8.); %put &todaysdate;

/****************************************************************************************/
/*	EDIS arrival time*/
libname edis '/data/dart/2021/Data/EDIS';

data Edis_arrivaltime&todaysdate (compress=yes rename=patienticn2=patienticn);
set edis.Edis_arrivaltime&todaysdate;
patienticn2=input(patienticn, 10.);
drop patienticn;
PatientArrivalDate=datepart(PatientArrivalDateTime);
format PatientArrivalDate mmddyy10.;
ED_Admit=1;
keep ED_Admit patienticn2 PatientArrivalDateTime PatientArrivalDate;
run;

/*Clean EDIS dataset before merging with VAPD VA to VA */
PROC SORT DATA=Edis_arrivaltime&todaysdate  nodupkey; 
BY patienticn PatientArrivalDateTime;
RUN;

/*want the earliest EDIS arrival time per pat-day, sort first then undup by pat-day*/
PROC SORT DATA=Edis_arrivaltime&todaysdate; 
BY  patienticn PatientArrivalDate PatientArrivalDateTime;
RUN;

PROC SORT DATA=Edis_arrivaltime&todaysdate nodupkey ; 
BY  patienticn PatientArrivalDate;
RUN;

DATA edis.Edis_Clean&todaysdate (compress=yes);
retain patienticn PatientArrivalDate  PatientArrivalDateTime ED_Admit EDIS_daily  EDIS_hosp; 
SET  Edis_arrivaltime&todaysdate;
EDIS_daily = 1;
EDIS_hosp =1;
RUN;

/************************************************************************************/
/*	Vitals (temp, pulse, respiration)*/
libname vital '/data/dart/2021/Data/vitals';

/********************* PULSE ********************/
/*remove duplicates*/
proc sort data=vital.pulse&todaysdate  nodupkey out=pulse2 (compress=yes); 
by patienticn VitalSignTakenDateTime VitalResultNumeric;
run;

data vital.Pulse_Clean&todaysdate (compress=yes rename=patienticn2=patienticn); 
set pulse2;
if VitalResultNumeric >220 or VitalResultNumeric <20 then delete; 
vital_date=datepart(VitalSignTakenDateTime);
format vital_date mmddyy10.;
year=year(vital_date);
patienticn2=input(patienticn, 10.);
drop patienticn;
run;

/********************* RESPIRATION ********************/
/*remove duplicates*/
proc sort data=vital.respiration&todaysdate nodupkey out=RESPIRATION (compress=yes);
by patienticn VitalSignTakenDateTime VitalResultNumeric;
run;

data vital.RESPIRATION_Clean&todaysdate (compress=yes rename=patienticn2=patienticn); 
set RESPIRATION;
vital_date=datepart(VitalSignTakenDateTime);
format vital_date mmddyy10.;
year=year(vital_date);
patienticn2=input(patienticn, 10.);
drop patienticn;
if VitalResultNumeric >50 or VitalResultNumeric < 6 then delete;
run;

/********************* TEMPERATURE ********************/
/*remove duplicates*/
proc sort data=vital.TEMPERATURE&todaysdate nodupkey out=TEMPERATURE (compress=yes); 
by patienticn VitalSignTakenDateTime VitalResultNumeric;
run;

data vital.TEMPERATURE_Clean&todaysdate (compress=yes rename=patienticn2=patienticn); 
set TEMPERATURE;
patienticn2=input(patienticn, 10.);
drop patienticn;
vital_date=datepart(VitalSignTakenDateTime);
format vital_date mmddyy10.;
year=year(vital_date);
if VitalResultNumeric >106 or VitalResultNumeric <93 then delete;
run;


/*********************************************************************************************/
/*Clean Procedures to get Mechanical Ventilation*/
libname proc '/data/dart/2021/Data/procedures';

data procedures (compress=yes rename=patienticn2=patienticn);
set proc.PROC_MECHVENT&todaysdate;
patienticn2=input(patienticn, 10.);
drop patienticn;
procdate=datepart(ICDProcedureDateTime);
format procdate mmddyy10.;
if  ICD10ProcedureCode in ('5A1935Z', '5A1945Z', '5A1955Z') 
    then proccode_mechvent_daily=1; else proccode_mechvent_daily=0;
if proccode_mechvent_daily=1;
RUN;

PROC SORT DATA=procedures nodupkey 
  out=proc.MechVent_Clean&todaysdate (keep=patienticn patientsid Sta3n procdate proccode_mechvent_daily); 
BY  patienticn procdate proccode_mechvent_daily;
RUN;

/****************************************************************************/
/*	Labs (WBC, Lactate, platelet, Creatinine, bilirubin)*/
libname labs '/data/dart/2021/Data/labs';

data WBC (compress=yes)
     Lactate (compress=yes)
     Platelets (compress=yes)
     Creatinine (compress=yes)
     Bilirubin (compress=yes);
set labs.labs&todaysdate;
if LabGroup='WBC' then output WBC; if LabGroup='Lactate' then output Lactate;
if LabGroup='Platelets' then output Platelets;  if LabGroup='Creatinine' then output Creatinine;
if LabGroup='Bilirubin' then output Bilirubin;
run;

/*bilirubin*/
/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=Bilirubin  nodupkey;
BY  Patienticn LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*create new date values*/
data Bilirubin_v2 (compress=yes rename=patienticn2=patienticn);
set Bilirubin;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3);
patienticn2=input(patienticn, 10.);
drop patienticn  units2 units3 units;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
year=year(LabSpecimenDate);
format LabSpecimenDate mmddyy10.;
keep Sta3n year LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID 
Topography LabSpecimenDate LabChemSpecimenDateTime patienticn2 clean_unit;
run;

/*PROC FREQ DATA=Bilirubin_v2  order=freq;*/
/*TABLE topography  clean_unit;*/
/*RUN;*/

/*keep only those with result value >0, blood topography and acceptable units*/
DATA Bilirubin_v3 (compress=yes); 
SET Bilirubin_v2;
if Topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*','BLOOD.','serum',
'SER/PLAS','WS-PLASMA','WHOLE BLOOD') 
OR  clean_unit notin ('MG/DL') /*just one unit, no lab conversions*/
or LabChemResultNumericValue <0  then delete;
RUN;

/*check missing units*/
data missing_unit; /*295*/
set Bilirubin_v3;
if clean_unit='';
run;

/*no unit conversions needed, permissible range 0.3-70.2 mg/dL*/
data labs.bilirubin_Clean&todaysdate (compress=yes); 
set Bilirubin_v3;
if LabChemResultNumericValue<0.3 or LabChemResultNumericValue >70.2 then delete;
drop  TopographySID LOINCSID LabChemTestSID;
run;

/***** CREATININE *****/
/*remove duplicate labs by patient, time of specimen and result*/
PROC SORT DATA=Creatinine nodupkey; 
BY PatientICN LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
data Creatinine_v2 (compress=yes rename=patienticn2=patienticn); 
set Creatinine;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3);
patienticn2=input(patienticn, 10.);
drop patienticn  units2 units3 units;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
year=year(LabSpecimenDate);
format LabSpecimenDate mmddyy10.;
keep Sta3n year LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID 
Topography LabSpecimenDate LabChemSpecimenDateTime patienticn2 clean_unit;
run;

PROC FREQ DATA=Creatinine_v2  order=freq;
TABLE topography  clean_unit;
RUN;

/*check values for different units*/
data not_MGDL;
set Creatinine_v2;
if clean_unit not in ( 'MG/DL' ) then output not_MGDL; 
run;

PROC MEANS DATA=not_MGDL MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*drop*/
RUN;

data missings;
set Creatinine_v2;
if clean_unit in ( '' ) then output missings; 
run;

PROC MEANS DATA=missings MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*drop*/
RUN;

/*look at frequency and delete labs that are non-blood topography and with incorrect units*/
DATA Creatinine_v3 (compress=yes); 
SET Creatinine_v2;
if topography notin ('PLASMA','SERUM','BLOOD','SER/PLA','BLOOD*','VENOUS BLOOD',
'ARTERIAL BLOOD','BLOOD, VENOUS', 'PLASMA+SERUM',
'BLOOD.','VENOUS BLD','BLOOD VENOUS','serum','SER/PLAS','PLAS',
'WS-PLASMA','WHOLE BLOOD') 
OR  clean_unit notin ('MG/DL','MG?DL','MGDL','MG\DL') 
or LabChemResultNumericValue <0 
	then delete;
RUN;

/*permissible range 0.1-28.3 mg/dL*/
data labs.Creatinine_Clean&todaysdate (compress=yes);
set Creatinine_v3;
if LabChemResultNumericValue <0.1 or LabChemResultNumericValue>28.3 then delete;
drop  TopographySID LOINCSID LabChemTestSID;
run;

/***** Platelets  ******/
/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=Platelets nodupkey out=Platelets_v2 (compress=yes); 
BY PatientICN LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*creat new year and datevalue variable*/
data Platelets_v3 (compress=yes rename=patienticn2=patienticn);
set Platelets_v2;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3);
patienticn2=input(patienticn, 10.);
drop patienticn  units2 units3 units;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
year=year(LabSpecimenDate);
format LabSpecimenDate mmddyy10.;
keep Sta3n year LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID 
Topography LabSpecimenDate LabChemSpecimenDateTime patienticn2 clean_unit;
run;

PROC FREQ DATA=Platelets_v3 order=freq;
TABLE topography  clean_unit;
RUN;

/*keep only those with result value >0, blood topography and acceptable unit*/
DATA Platelets_v4 (compress=yes); 
SET Platelets_v3;
if topography notin ('BLOOD','WHOLE BLOOD','PLASMA','BLOOD,CAPILLARY','SERUM',
'WS-BLOOD','BLOOD - SM','BLOOD, WHOLE', 'serum',
'ARTERIAL BLOOD','BLOOD, VENOUS','PLASMA+SERUM','SER/PLA','SERUM/BLOOD') 
or clean_unit notin ('K/CMM','K/UL','K/MM3','10*3/UL',
'10E3/UL','X1000/UL','10E9/L','X10-3','K/MCL','X10-3/UL','K/CUMM','BILL/L',
'THOUS/CMM','10(3)/MCL','THOU/CUMM','1000/UL',
'THOU/UL','T/CMM','1.00E+04','X103','K/CCM','10X3/CMM','103/UL','K/MICROL',
'X10E3/UL','X(10)3',"10'3/UL",'X1000','10X3/UL',
'THOU','TH/MM3','1000/MCL','TH/UL','THOUS/UL','X10(9)/L','10*9/L','X10(3)/UL',
'K/CM','10*3UL','1000/MM3','10**3') 
or LabChemResultNumericValue <0
	then delete;
RUN;

PROC MEANS DATA= Platelets_v4 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median: 1*/
RUN;

/*Only keep those labs within permissible range 1-1500 (1 10^9/L=1000/MCL, no conversions) */
data labs.Platelets_Clean&todaysdate (compress=yes); 
set Platelets_v4;
if LabChemResultNumericValue <1 or LabChemResultNumericValue>1500 then delete;
drop  TopographySID LOINCSID LabChemTestSID;
run;

/******Lactate *******/
/*remove duplicate labs by patient, time of specimen and result*/
PROC SORT DATA=LACTATE out=LACTATE_v2 (compress=yes) nodupkey ; /*1475616*/
BY  PatientICN LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*convert LabChemSpecimenDateTime to LabSpecimenDate*/
data LACTATE_v3 (compress=yes rename=patienticn2=patienticn);
set LACTATE_v2;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3);
patienticn2=input(patienticn, 10.);
drop patienticn  units2 units3 units;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
year=year(LabSpecimenDate);
format LabSpecimenDate mmddyy10.;
keep Sta3n year LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID 
Topography LabSpecimenDate LabChemSpecimenDateTime patienticn2 clean_unit;
run;

PROC FREQ DATA=LACTATE_v3  order=freq;
TABLE topography  clean_unit;
RUN;

/*keep only those with result value >0, blood topography and acceptable unit*/
DATA LACTATE_v4 (compress=yes);  
SET LACTATE_v3;
if topography notin ('PLASMA','ARTERIAL BLOOD','BLOOD','VENOUS BLOOD','SERUM','BLOOD, VENOUS',
'VENOUS BLD','WHOLE BLOOD','BLOOD VENOUS','ARTERIAL BLD','BLOOD, ARTERIAL','PLAS','SER/PLA') 
or clean_unit notin ('MMOL/L','MEQ/L','MMOLE/L','MG/DL','MMOLS/L','MML/L','MMOLES/L','NMOL/L') 
or LabChemResultNumericValue <0
	then delete;
RUN;

PROC MEANS DATA=LACTATE_v4  MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median: 1.6*/
RUN;

/*Convert MG/DL to mmol/L  (9 mg/dL=1 mmol/L)*/
/*permissible range: 0-50 mmol/l*/  /*N=1455976*/
data LACTATE_v5 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue);
set LACTATE_v4;
if clean_unit='MG/DL' then new_lab_value=LabChemResultNumericValue/9;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $6;
if clean_unit='' then new_clean_unit=''; else new_clean_unit='MMOL/L';
if  new_lab_value>50 then delete;
drop clean_unit LabChemResultNumericValue;
run;

data labs.LACTATE_Clean&todaysdate (compress=yes);   
set LACTATE_v5;
drop  TopographySID LOINCSID LabChemTestSID;
run;

/********  White Blood Cell***********/
/*remove duplicate labs by patient, facility, time of specimen and result*/
PROC SORT DATA=WBC out=wbc_V2 (compress=yes) nodupkey; 
BY  PatientICN LabChemSpecimenDateTime LabChemResultNumericValue;
RUN;

/*create new date values*/
data wbc_V3 (compress=yes rename=patienticn2=patienticn);
set wbc_V2;
Units2=upcase(units); /*turn all units into uppercase*/
units3=compress(Units2,'.'); /*removes '.' in units*/
clean_unit = compress(units3);
patienticn2=input(patienticn, 10.);
drop patienticn  units2 units3 units;
LabSpecimenDate=datepart(LabChemSpecimenDateTime);
year=year(LabSpecimenDate);
format LabSpecimenDate mmddyy10.;
keep Sta3n year LabChemTestSID PatientSID LabChemResultNumericValue TopographySID LOINCSID 
Topography LabSpecimenDate LabChemSpecimenDateTime patienticn2 clean_unit;
run;

PROC FREQ DATA=wbc_V3  order=freq;
TABLE topography  clean_unit;
RUN;

data wbc_V4 (compress=yes); 
set wbc_V3; 
if topography notin ('BLOOD','WHOLE BLOOD','PLASMA','SERUM','WS-BLOOD','BLOOD - SM','BLOOD*',
'BLOOD, VENOUS','PLASMA+SERUM','SER/PLA') 
or clean_unit notin ('K/CMM','K/UL','K/MM3','10*3/UL','10E3/UL','X10-3/UL','X1000/UL','10E9/L',
'K/MCL','BILL/L','10X3CUMM','THOUS/CMM','10(3)/MCL','/UL','THOU/CUMM','10E3/MCL','1000/UL',
'THOU/UL','T/CMM','K/MM-3','K/CUMM','X103','K/CCM','103/UL','10X3/CMM','#/CMM','K/ML','X10E3/UL','#/UL',
'CUMM','10X3/CCM','X1000','/CUMM','/CUM',"10'3/UL",'UL','THOUCMM','/CMM','10X3/UL','CMM','THOU','TH/MM3',
'CELLS/UL','1000/MCL','TH/UL','THOUS/UL','X10(9)/L','WBC/CMM','X10(3)/UL','THO/MM3','THOUS/MM3',
'X10E9/L','/MM3','1000/MM3') or LabChemResultNumericValue <0 
   then delete;
run;

PROC MEANS DATA=wbc_V4 MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=8*/ 
RUN;

/*permissible range 0-300 X 10^9/L*/

/*conversions: 1 MCL=10^6 L, so it's 1000/MCL, so 0-300 thou/mcl is the range*/
/*mm3=10^6L=uL */

/*look at descriptive for /UL*/
data UL; 
set wbc_V4;
if clean_unit in ('UL');
run;

PROC MEANS DATA=UL   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=5900*/ /*need to divide by 1000*/
RUN;

data UL2; 
set wbc_V4;
if clean_unit in ('CELLS/UL');
run;

PROC MEANS DATA=UL2   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=6700*//*need to divide by 1000*/
RUN;

/*look at descriptives for /L*/
data L; 
set wbc_V4;
if clean_unit in ('10E9/L','BILL/L','X10(9)/L','X10E9/L');
run;

PROC MEANS DATA=L   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;/*median=7.4*/
RUN;

/*look at descriptives for thousand/ uL, MM3, MCL*/
data thousands; 
set wbc_V4;
if clean_unit in ('K/CMM','K/UL','K/MM3','10*3/UL','10E3/UL','X10-3/UL','X1000/UL',
'K/MCL','10X3CUMM','THOUS/CMM','10(3)/MCL','THOU/CUMM','10E3/MCL','1000/UL','THOU/UL',
'T/CMM','K/MM-3','K/CUMM','X103','K/CCM','103/UL','10X3/CMM','K/ML','X10E3/UL',
'10X3/CCM','X1000',"10'3/UL",'THOUCMM','10X3/UL','THOU','TH/MM3','1000/MCL',
'TH/UL','THOUS/UL','X10(3)/UL','THO/MM3','THOUS/MM3','1000/MM3');
run;

PROC MEANS DATA=thousands   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*median=7.4*/
RUN;

/*look at descriptives for CUMM, CCM, CUM, CMM, etc.*/
data cumm; 
set wbc_V4;
if clean_unit in ('#/CMM','CUMM','/CUMM','/CUM','/CMM','CMM','WBC/CMM','/MM3');
run;

PROC MEANS DATA=cumm   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ; /*mean=7377, median=5700*/ /*need to divide by 1000*/
RUN;

/*convert units*/
data wbc_V5 (rename=new_clean_unit=clean_unit rename=new_lab_value=LabChemResultNumericValue);
set wbc_V4;
if clean_unit in ('#/CMM','CUMM','/CUMM','/CUM','/CMM','CMM','WBC/CMM','/MM3','UL','CELLS/UL') 
	then new_lab_value=LabChemResultNumericValue/1000;
else new_lab_value=LabChemResultNumericValue;
length new_clean_unit $4;
if clean_unit='' then new_clean_unit=''; else new_clean_unit='K/uL';
if new_lab_value <0 or new_lab_value>300 then delete;
drop clean_unit LabChemResultNumericValue;
run;

PROC MEANS DATA=wbc_V5   MIN MAX MEAN MEDIAN Q1 Q3;
VAR LabChemResultNumericValue ;
RUN;

data labs.WBC_Clean&todaysdate (compress=yes);
set wbc_V5;
drop TopographySID LOINCSID LabChemTestSID;
run;

/******************************************************************************************************/
/*Clean BCMA ABX drugs*/
libname meds '/data/dart/2021/Data/meds';

data dispensed (compress=yes)  Additive (compress=yes)  Solution (compress=yes);
set meds.BCMA_ABX&todaysdate;
if Dispensed=1 then output dispensed;
if Additive=1 then output Additive;
if Solution=1 then output Solution;
run;

/***** Dispensed Drugs *****/
/*remove duplicates*/
PROC SORT DATA=dispensed  nodupkey out=BCMA_BCMADispensedDrug_v4; 
BY patienticn ActionDatetime LocalDrugSID  LocalDrugNameWithDose;
RUN;

DATA BCMA_BCMADispensedDrug_v4 (compress=yes); 
SET BCMA_BCMADispensedDrug_v4;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
length unitdosemedicationroute $2;
unitdosemedicationroute='';
drop Additive  Solution;
RUN;

/*IV Additive*/
/*remove duplicates*/
PROC SORT DATA=Additive nodupkey out=BCMA_Additive_v4 (compress=yes); 
BY patienticn ActionDatetime LocalDrugSID  LocalDrugNameWithDose;
run;

DATA BCMA_Additive_v4 (compress=yes);  
SET BCMA_Additive_v4 ;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
length unitdosemedicationroute $2;
unitdosemedicationroute='IV';
drop Dispensed  Solution;
RUN;

/*IV Solutions*/
/*remove duplicates*/
PROC SORT DATA=Solution nodupkey out=BCMA_Solution_v4 (compress=yes); 
BY patienticn ActionDatetime LocalDrugSID  LocalDrugNameWithDose;
run;

DATA BCMA_Solution_v4 (compress=yes); 
SET BCMA_Solution_v4;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
length unitdosemedicationroute $2;
unitdosemedicationroute='IV';
drop Dispensed  Additive;
RUN;

data combined_names_HAPPI (compress=yes); /*all 3 datasets have field: LocalDrugNameWithDose*/ 
set BCMA_BCMADispensedDrug_v4   BCMA_Additive_v4 BCMA_Solution_v4;
run;

/*delete study/test drugs*/
data combined_names2_HAPPI; 
set combined_names_HAPPI;
if localdrugnamewithdose='BOTH EYES' or localdrugnamewithdose='EACH EYE' or localdrugnamewithdose='EXTERNAL' or 
localdrugnamewithdose='EXTERNALLY' or localdrugnamewithdose='G TUBE' or localdrugnamewithdose='NASAL' or
localdrugnamewithdose='OPHTHALMIC' or localdrugnamewithdose='OPHTHALMIC (BOTH)' or 
localdrugnamewithdose='OPHTHALMIC (DROPS)' or localdrugnamewithdose='OPHTHALMIC (OINT)' or
localdrugnamewithdose='OPHTHALMIC BOTH' or localdrugnamewithdose='OPHTHALMIC TOPICAL' or
localdrugnamewithdose='OPHTHALMIC TOPICAL (BOTH)' or localdrugnamewithdose='OPTHALMIC' or
localdrugnamewithdose='ZZOPHTHALMIC' or localdrugnamewithdose='ZZOPHTHALMIC OINTMENT' or
localdrugnamewithdose='ZZOPHTHALMIC SPACE' or localdrugnamewithdose='ZZOPHTHALMIC TOPICAL' or
localdrugnamewithdose='ZZOPTHALMIC' or localdrugnamewithdose='ZZZOPTHALMIC' or
index(localdrugnamewithdose, "ACYCLOVIR/HYDROCORTISONE")>0 or 
index(localdrugnamewithdose, "ALLERGENIC EXTRACT,PENICILLIN")>0 or
index(localdrugnamewithdose, "AMOXICILLIN/CLARITHROMYCIN/LANSOPRAZOLE")>0 or 
index(localdrugnamewithdose, "BACITRACIN/HYDROCORTISONE/NEOMYCIN/POLYMYXIN B")>0 or 
index(localdrugnamewithdose, "BACITRACIN/NEOMYCIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "BACITRACIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "BENZOYL PEROXIDE/CLINDAMYCIN")>0  or 
index(localdrugnamewithdose, "BISMUTH SUBSALICYLATE/METRONIDAZOLE/TETRACYCLINE")>0  or 
index(localdrugnamewithdose, "BISMUTH/METRONIDAZOLE/TETRACYCLINE")>0  or 
index(localdrugnamewithdose, "CIPROFLOXACIN/DEXAMETHASONE")>0 or 
index(localdrugnamewithdose, "CIPROFLOXACIN/HYDROCORTISONE")>0  or 
index(localdrugnamewithdose, "CLINDAMYCIN PHOSPHATE/TRETINOIN")>0  or 
index(localdrugnamewithdose, "COLISTIN/HYDROCORTISONE/NEOMYCIN/THONZONIUM")>0  or
index(localdrugnamewithdose, "DEXAMETHASONE/NEOMYCIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "DEXAMETHASONE/TOBRAMYCIN")>0  or 
index(localdrugnamewithdose, "DIPHENHYDRAMINE/HYDROCORTISONE/NYSTATIN/TETRACYCLINE")>0  or
index(localdrugnamewithdose, "ERYTHROMYCIN/SULFISOXAZOLE")>0  or 
index(localdrugnamewithdose, "GENTAMICIN/PREDNISOLONE")>0  or 
index(localdrugnamewithdose, "GRAMICIDIN/NEOMYCIN/POLYMYXIN B")>0  or
index(localdrugnamewithdose, "HYDROCORTISONE/NEOMYCIN/POLYMYXIN B")>0  or
index(localdrugnamewithdose, "LOTEPREDNOL/TOBRAMYCIN")>0  or 
index(localdrugnamewithdose, "NEOMYCIN/POLYMYXIN B")>0 or
index(localdrugnamewithdose, "NEOMYCIN/POLYMYXIN B/PREDNISOLONE")>0  or 
index(localdrugnamewithdose, "OXYTETRACYCLINE")>0  or
index(localdrugnamewithdose, "OXYTETRACYCLINE/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "POLYMYXIN B/TRIMETHOPRIM")>0  or 
index(localdrugnamewithdose, "SILVER SULFADIAZINE")>0  or 
index(localdrugnamewithdose,"STUDY")>0 OR index(localdrugnamewithdose,"UNKNOWN")>0 or
index(localdrugnamewithdose,"TEST")>0 	
	then delete;
run;

proc sort data=combined_names2_HAPPI;
by patienticn ActionDate ActionDateTime;
run;

/*further drop*/
proc sql;
create table drop_v2 (compress=yes) as 
select *, case
when localDrugNameWithDose like '%GEL%' or localDrugNameWithDose like '%CREAM%' or localDrugNameWithDose like '%OINT%'
or localDrugNameWithDose like '%RECTAL%' or localDrugNameWithDose like '%VAG%' or localDrugNameWithDose like '%IRRG%'
or localDrugNameWithDose like '% OTIC%' or localDrugNameWithDose like '% OPH%' or localDrugNameWithDose like '%OPTH%'
or localDrugNameWithDose like '%OPHTH%' or localDrugNameWithDose like '%TOPICAL%' or localDrugNameWithDose like '%SPRAY%' 
or localDrugNameWithDose like ' % TOP % ' or localDrugNameWithDose like '%PLACEBO%' or localDrugNameWithDose like '%TOP SOL%'
or localDrugNameWithDose  like '% TOP.%' or localDrugNameWithDose  like '%SWAB,TOP%' or localDrugNameWithDose  like '%SWAB, TOP%'
or localDrugNameWithDose like '%NASAL%' or localDrugNameWithDose like '%LOTION%' or localDrugNameWithDose like '%INHL%'
or localDrugNameWithDose like '%EYE%' or localDrugNameWithDose like '%FORTIFIED EYE DROPS%'  or localDrugNameWithDose  like '%TOP CRM%'
or localDrugNameWithDose  like '%PWD%' or localDrugNameWithDose  like '%POWDER%' or localDrugNameWithDose like '%TUBE%'
or localDrugNameWithDose like '%STUDY%' or localDrugNameWithDose like '%INH SOLN%' or localDrugNameWithDose like '% NOSE %'
or localDrugNameWithDose like '%RTL SUPP%' or localDrugNameWithDose like '%RTL CRM%'
then 1 else 0
end as drop_v2
from combined_names2_HAPPI;
quit;

proc sort data=drop_v2;
by descending drop_v2;
run;

data combined_names2_HAPPI_v4 (compress=yes); 
set drop_v2;
if drop_v2=1 then delete;
run;

proc sort data=combined_names2_HAPPI_v4;
by patienticn ActionDate ActionDateTime;
run;

proc freq data=combined_names2_HAPPI_v4;
table localDrugNameWithDose;
run;

data combined_names2_HAPPI_v5; 
set combined_names2_HAPPI_v4;
by patienticn ActionDate;
if first.ActionDate then earliest_ABXactionDateTime =0;
  earliest_ABXactionDateTime +1;
run;

/*keep the earliest*/
data combined_names2_HAPPI_v6; 
set combined_names2_HAPPI_v5;
if earliest_ABXactionDateTime NE 1 then delete;
run;

data meds.BCMA_ABXClean&todaysdate (compress=yes); 
set combined_names2_HAPPI_v6;
drop drop_v2;
BCMA_ABX_daily=1;
run;

data meds.BCMA_ABXClean&todaysdate (compress=yes rename=patienticn2=patienticn); 
set meds.BCMA_ABXClean&todaysdate;
patienticn2=input(patienticn, 10.);
drop patienticn;
run;

/********************************************************************************************/
/*Clean BCMA Pressors*/
data dispensed_pressor (compress=yes)  Additive_pressor (compress=yes)  Solution_pressor (compress=yes);
set meds.BCMA_PRESSOR&todaysdate;
if Dispensed=1 then output dispensed_pressor;
if Additive=1 then output Additive_pressor;
if Solution=1 then output Solution_pressor;
run;

/***** Dispensed Drugs *****/
/*remove duplicates*/
PROC SORT DATA=dispensed_pressor  nodupkey out=BCMA_BCMADispensedDrug_v4; 
BY patienticn ActionDatetime LocalDrugSID  LocalDrugNameWithDose;
RUN;

DATA BCMA_BCMADispensedDrug_v4 (compress=yes); 
SET BCMA_BCMADispensedDrug_v4;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
length unitdosemedicationroute $2;
unitdosemedicationroute='';
drop Additive  Solution;
RUN;

/*IV Additive*/
/*remove duplicates*/
PROC SORT DATA=Additive_pressor nodupkey out=BCMA_Additive_v4 (compress=yes); 
BY patienticn ActionDatetime LocalDrugSID  LocalDrugNameWithDose;
run;

DATA BCMA_Additive_v4 (compress=yes);  
SET BCMA_Additive_v4 ;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
length unitdosemedicationroute $2;
unitdosemedicationroute='IV';
drop Dispensed  Solution;
RUN;

/*IV Solutions*/
/*remove duplicates*/
PROC SORT DATA=Solution_pressor nodupkey out=BCMA_Solution_v4 (compress=yes); 
BY patienticn ActionDatetime LocalDrugSID  LocalDrugNameWithDose;
run;

DATA BCMA_Solution_v4 (compress=yes); 
SET BCMA_Solution_v4;
ActionDate=datepart(ActionDateTime); /*create new action date*/
format ActionDate mmddyy10.;
length unitdosemedicationroute $2;
unitdosemedicationroute='IV';
drop Dispensed  Additive;
RUN;

data combined_names_HAPPI (compress=yes); /*all 3 datasets have field: LocalDrugNameWithDose*/ 
set BCMA_BCMADispensedDrug_v4   BCMA_Additive_v4 BCMA_Solution_v4;
run;


/*delete study/test drugs*/
data combined_names2_HAPPI; 
set combined_names_HAPPI;
if localdrugnamewithdose='BOTH EYES' or localdrugnamewithdose='EACH EYE' or localdrugnamewithdose='EXTERNAL' or 
localdrugnamewithdose='EXTERNALLY' or localdrugnamewithdose='G TUBE' or localdrugnamewithdose='NASAL' or
localdrugnamewithdose='OPHTHALMIC' or localdrugnamewithdose='OPHTHALMIC (BOTH)' or 
localdrugnamewithdose='OPHTHALMIC (DROPS)' or localdrugnamewithdose='OPHTHALMIC (OINT)' or
localdrugnamewithdose='OPHTHALMIC BOTH' or localdrugnamewithdose='OPHTHALMIC TOPICAL' or
localdrugnamewithdose='OPHTHALMIC TOPICAL (BOTH)' or localdrugnamewithdose='OPTHALMIC' or
localdrugnamewithdose='ZZOPHTHALMIC' or localdrugnamewithdose='ZZOPHTHALMIC OINTMENT' or
localdrugnamewithdose='ZZOPHTHALMIC SPACE' or localdrugnamewithdose='ZZOPHTHALMIC TOPICAL' or
localdrugnamewithdose='ZZOPTHALMIC' or localdrugnamewithdose='ZZZOPTHALMIC' or
index(localdrugnamewithdose, "ACYCLOVIR/HYDROCORTISONE")>0 or 
index(localdrugnamewithdose, "ALLERGENIC EXTRACT,PENICILLIN")>0 or
index(localdrugnamewithdose, "AMOXICILLIN/CLARITHROMYCIN/LANSOPRAZOLE")>0 or 
index(localdrugnamewithdose, "BACITRACIN/HYDROCORTISONE/NEOMYCIN/POLYMYXIN B")>0 or 
index(localdrugnamewithdose, "BACITRACIN/NEOMYCIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "BACITRACIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "BENZOYL PEROXIDE/CLINDAMYCIN")>0  or 
index(localdrugnamewithdose, "BISMUTH SUBSALICYLATE/METRONIDAZOLE/TETRACYCLINE")>0  or 
index(localdrugnamewithdose, "BISMUTH/METRONIDAZOLE/TETRACYCLINE")>0  or 
index(localdrugnamewithdose, "CIPROFLOXACIN/DEXAMETHASONE")>0 or 
index(localdrugnamewithdose, "CIPROFLOXACIN/HYDROCORTISONE")>0  or 
index(localdrugnamewithdose, "CLINDAMYCIN PHOSPHATE/TRETINOIN")>0  or 
index(localdrugnamewithdose, "COLISTIN/HYDROCORTISONE/NEOMYCIN/THONZONIUM")>0  or
index(localdrugnamewithdose, "DEXAMETHASONE/NEOMYCIN/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "DEXAMETHASONE/TOBRAMYCIN")>0  or 
index(localdrugnamewithdose, "DIPHENHYDRAMINE/HYDROCORTISONE/NYSTATIN/TETRACYCLINE")>0  or
index(localdrugnamewithdose, "ERYTHROMYCIN/SULFISOXAZOLE")>0  or 
index(localdrugnamewithdose, "GENTAMICIN/PREDNISOLONE")>0  or 
index(localdrugnamewithdose, "GRAMICIDIN/NEOMYCIN/POLYMYXIN B")>0  or
index(localdrugnamewithdose, "HYDROCORTISONE/NEOMYCIN/POLYMYXIN B")>0  or
index(localdrugnamewithdose, "LOTEPREDNOL/TOBRAMYCIN")>0  or 
index(localdrugnamewithdose, "NEOMYCIN/POLYMYXIN B")>0 or
index(localdrugnamewithdose, "NEOMYCIN/POLYMYXIN B/PREDNISOLONE")>0  or 
index(localdrugnamewithdose, "OXYTETRACYCLINE")>0  or
index(localdrugnamewithdose, "OXYTETRACYCLINE/POLYMYXIN B")>0  or 
index(localdrugnamewithdose, "POLYMYXIN B/TRIMETHOPRIM")>0  or 
index(localdrugnamewithdose, "SILVER SULFADIAZINE")>0  or 
index(localdrugnamewithdose,"STUDY")>0 OR index(localdrugnamewithdose,"UNKNOWN")>0 or
index(localdrugnamewithdose,"TEST")>0 	
	then delete;
run;

proc sort data=combined_names2_HAPPI;
by patienticn ActionDate ActionDateTime;
run;

/*further drop*/
proc sql;
create table drop_v2 (compress=yes) as 
select *, case
when localDrugNameWithDose like '%GEL%' or localDrugNameWithDose like '%CREAM%' or localDrugNameWithDose like '%OINT%'
or localDrugNameWithDose like '%RECTAL%' or localDrugNameWithDose like '%VAG%' or localDrugNameWithDose like '%IRRG%'
or localDrugNameWithDose like '% OTIC%' or localDrugNameWithDose like '% OPH%' or localDrugNameWithDose like '%OPTH%'
or localDrugNameWithDose like '%OPHTH%' or localDrugNameWithDose like '%TOPICAL%' or localDrugNameWithDose like '%SPRAY%' 
or localDrugNameWithDose like ' % TOP % ' or localDrugNameWithDose like '%PLACEBO%' or localDrugNameWithDose like '%TOP SOL%'
or localDrugNameWithDose  like '% TOP.%' or localDrugNameWithDose  like '%SWAB,TOP%' or localDrugNameWithDose  like '%SWAB, TOP%'
or localDrugNameWithDose like '%NASAL%' or localDrugNameWithDose like '%LOTION%' or localDrugNameWithDose like '%INHL%'
or localDrugNameWithDose like '%EYE%' or localDrugNameWithDose like '%FORTIFIED EYE DROPS%'  or localDrugNameWithDose  like '%TOP CRM%'
or localDrugNameWithDose  like '%PWD%' or localDrugNameWithDose  like '%POWDER%' or localDrugNameWithDose like '%TUBE%'
or localDrugNameWithDose like '%STUDY%' or localDrugNameWithDose like '%INH SOLN%' or localDrugNameWithDose like '% NOSE %'
or localDrugNameWithDose like '%RTL SUPP%' or localDrugNameWithDose like '%RTL CRM%'
then 1 else 0
end as drop_v2
from combined_names2_HAPPI;
quit;

proc sort data=drop_v2;
by descending drop_v2;
run;

data combined_pressors (compress=yes); 
set drop_v2;
if drop_v2=1 then delete;
BCMA_pressor_daily=1;
drop drop_v2;
run;

/*keep all pressors to look at the 72 hour window*/
proc sort data=combined_pressors nodupkey out=meds.BCMA_PressorClean&todaysdate (compress=yes);
by patienticn ActionDate ActionDateTime;
run;

data meds.BCMA_PressorClean&todaysdate (compress=yes rename=patienticn2=patienticn); 
set meds.BCMA_PressorClean&todaysdate;
patienticn2=input(patienticn, 10.);
drop patienticn;
run;


/*********************************************************************************************/
/*Clean PulseOx data*/
libname pulseox '/data/dart/2021/Data/pulse ox';

DATA NEWPULSEOX20132017 (compress=yes rename=patienticn2=patienticn); 
SET pulseox.PULSEOX&todaysdate;
patienticn2=input(patienticn, 10.);
drop patienticn;
RUN;

/*remove any duplicates*/
PROC SORT DATA=NEWPULSEOX20132017  nodupkey; 
BY  patienticn vitalSignTakenDateTime VitalResultNumeric SupplementalO2;
RUN;

PROC MEANS DATA=NEWPULSEOX20132017  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

/*check VitalType*/
PROC FREQ DATA=NEWPULSEOX20132017  order=freq;
TABLE  VitalType;
RUN;

DATA  all_data (compress=yes); 
SET NEWPULSEOX20132017;
if SupplementalO2 ='' then LPM=0;
RUN;

PROC FREQ DATA=all_data  order=freq; 
TABLE  LPM;
RUN;


/*Clean those 1919075 records where SupplementalO2  <> NULL:*/ 
DATA need_clean (compress=yes)    dont_clean (compress=yes) ;
SET  all_data;
vital_date=datepart(VitalSignTakenDateTime);
format vital_date mmddyy10.;
year=year(vital_date);
if LPM NE 0 then output need_clean; /*902218*/
else output dont_clean; /*1016857*/
RUN;

/*set up the dont_clean data*/
DATA dont_clean_cohort (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType  vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET dont_clean ;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=LPM;
keep patienticn PatientSID Sta3n VitalTypeSID VitalType  vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent; 
RUN;

PROC MEANS DATA=dont_clean_cohort   MIN MAX MEAN MEDIAN Q1 Q3;
VAR VitalResultNumeric ;
RUN;

proc sgplot data=dont_clean_cohort noautolegend;
 histogram VitalResultNumeric;
 density VitalResultNumeric;
run;

PROC FREQ DATA=dont_clean_cohort; /*Increasing trend*/
TABLE  year;
RUN;

PROC FREQ DATA=need_clean; /*Increasing trend*/
TABLE  year;
RUN;

DATA PulseOx20132017_v1 (compress=yes); 
SET need_clean;
obs=_N_;
drop LPM;
RUN;

/*Clean the PulseOX 2018-2020.01 data, no need to merge to VAPD dataset yet*/
/*how many obs have VitalResultNumeric > 100?*/
DATA check_value (compress=yes); /*N=0*/
SET PulseOx20132017_v1;
if  VitalResultNumeric>100;
RUN;

PROC MEANS DATA=PulseOx20132017_v1   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

/*clean the SupplementalO2 to take out extra space and turn everything into CAPS*/
DATA  PulseOx20132017_v2  (compress=yes rename=SupplementalO2_v3=SupplementalO2);  
SET  PulseOx20132017_v1;
SupplementalO2_v2=upcase(SupplementalO2); /*turn all units into uppercase*/
SupplementalO2_v3=compress(SupplementalO2_v2);  /*removes all blanks*/
drop SupplementalO2_v2 SupplementalO2;  /*drop the original SupplementalO2 field and rename SupplementalO2_v3 as SupplementalO2*/
RUN;

/******************************************************************************************************/
PROC SQL;
CREATE TABLE pulse_noLmin (compress=yes)  AS  
SELECT *
FROM PulseOx20132017_v2
WHERE   SupplementalO2 not like  '%L/MIN%';
QUIT;

PROC MEANS DATA=pulse_noLmin   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

PROC FREQ DATA=pulse_noLmin  order=freq;
TABLE  SupplementalO2;
RUN;

/*remove the % sign and change to numeric value and look at descriptive*/
DATA  pulse_noLmin2  (compress=yes); 
SET   pulse_noLmin;
supple_char=compress(SupplementalO2,'%'); 
supple_num=input(supple_char, 3.);
RUN;

/* check if supple_num NE VitalResultNumeric*/
data checking; 
set pulse_noLmin2 ;
if supple_num NE VitalResultNumeric;
run;

PROC FREQ DATA=pulse_noLmin2;
TABLE  supple_num;
RUN;

PROC MEANS DATA=pulse_noLmin2 MIN  MAX MEAN  MEDIAN Q1 Q3;
VAR  supple_num;
RUN;

/*use Jack's conversions to get SpO2 and O2 (L/MIN), call this cohort1_20132017.
create new fields: SpO2=VitalResultNumeric, O2_LPM, and incoherent*/
DATA cohort1_20132017_test (compress=yes); 
SET  pulse_noLmin2;
SpO2=VitalResultNumeric;
/*incoherent=0;*/
O2_LPM=((supple_num/100)-0.21)/0.03; /*turn into % first*/
if O2_LPM < 0 then delete; /*on 3/2/20 Jack said it's okay to delete*/
RUN;

PROC MEANS DATA=cohort1_20132017_test MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric SpO2 O2_LPM;
RUN;

PROC FREQ DATA=cohort1_20132017_test order=freq;
TABLE O2_LPM;
RUN;

/*there's cohor 1A and 1B: in general, SupplementalO2 should not equal VitalResultNumeric, Jack said this is incoherent on 6/17/20.*/
DATA cohort1A_20132017 (compress=yes) 
cohort1B_20132017 (compress=yes);
SET  cohort1_20132017_test;
if supple_num=  VitalResultNumeric then  incoherent=1;
 else incoherent=0;
if incoherent=0 then output cohort1A_20132017;
if incoherent=1 then output cohort1B_20132017;
keep patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
RUN;

DATA cohort1A_20132017 (compress=yes);
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
set cohort1A_20132017;
RUN;

DATA cohort1B_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
set cohort1B_20132017;
RUN;

/******************************************************************************************************************/
/*select everything else (none % only) not in pulse_noLmin dataset, and do a frequency to see what units it entail*/
PROC SQL;
CREATE TABLE  pulse_lmin  (COMPRESS=YES) AS 
SELECT A.* FROM PulseOx20132017_v2 AS A
WHERE A.obs not IN (SELECT obs FROM pulse_noLmin);
QUIT;

PROC FREQ DATA=pulse_lmin  order=freq;
TABLE SupplementalO2 ;
RUN;

/*******************/
/*look at those only with L/MIN units*/
PROC SQL;
CREATE TABLE pulse_lmin_only (compress=yes)  AS  
SELECT *
FROM pulse_lmin
WHERE   SupplementalO2  like  '%L/MIN' or SupplementalO2  like  'L/MIN';
QUIT;

/*check*/
PROC FREQ DATA=pulse_lmin_only  order=freq;
TABLE  SupplementalO2;
RUN;

/*there were some typs for LPM, remove 'L/MIN', turn into numeric to look at descriptives*/
DATA pulse_lmin_only2 (compress=yes); 
SET  pulse_lmin_only ;
if SupplementalO2='3EL/MIN' then SupplementalO2='3L/MIN';
if SupplementalO2='5LTL/MIN' then SupplementalO2='5L/MIN';
if SupplementalO2='4LITERSL/MIN' then SupplementalO2='4L/MIN';
if SupplementalO2='3LPML/MIN' then SupplementalO2='3L/MIN';
if SupplementalO2='4LPML/MIN' then SupplementalO2='4L/MIN';
if SupplementalO2='2L.NCL/MIN' then SupplementalO2='2L/MIN';
if SupplementalO2='2LPML/MIN' then SupplementalO2='2L/MIN';
if SupplementalO2='3LTL/MIN' then SupplementalO2='3L/MIN';
if SupplementalO2='RAL/MIN' then SupplementalO2='0L/MIN';
SupplementalO2_char=compress(SupplementalO2,'L/MIN'); /*removes '.' in units*/
SupplementalO2_num=input(SupplementalO2_char, 3.);
RUN;

PROC FREQ DATA=pulse_lmin_only2  order=freq;
TABLE  SupplementalO2;
RUN;

PROC MEANS DATA=pulse_lmin_only2   MIN  MAX MEAN  MEDIAN  Q1 Q3;
VAR SupplementalO2_num VitalResultNumeric;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this  cohort2.*/
DATA cohort2_20132017_test (compress=yes); 
SET pulse_lmin_only2;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=SupplementalO2_num;
if SupplementalO2='RAL/MIN' then O2_LPM=0; /*recode RAL/MIN =0, don't exclude*/
if O2_LPM =. then delete;  /*3/2/20: Jack said it's ok to delete. N=14*/
RUN;

/*check*/
PROC MEANS DATA=cohort2_20132017_test  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric O2_LPM SpO2;
RUN;
PROC FREQ DATA=cohort2_20132017_test  order=freq;
TABLE  incoherent O2_LPM;
RUN;

DATA cohort2_20132017 (compress=yes);  
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET   cohort2_20132017_test;
drop SupplementalO2_char SupplementalO2_num;
RUN;

/**********************************************************************************************************/
/*look at those only with 'L/MIN%' unit*/
PROC SQL;
CREATE TABLE pulse_lminpercent_only (compress=yes)  AS  
SELECT *
FROM pulse_lmin
WHERE   SupplementalO2  like  'L/MIN%';
QUIT;

PROC FREQ DATA= pulse_lminpercent_only  order=freq;
TABLE  SupplementalO2;
RUN;

/* get the descriptive on X%*/
/*first, get the last 4 digits*/
DATA last_4char_pulse_lminpercent  (compress=yes); 
SET  pulse_lminpercent_only ;
last_4=substr(SupplementalO2,length(SupplementalO2)-3,4);
RUN;

PROC FREQ DATA=last_4char_pulse_lminpercent  order=freq;
TABLE  last_4;
RUN;

/*from last character values, compress "MIN%" characters*/
DATA last_4char_pulse_lminpercent2  (compress=yes); 
SET  last_4char_pulse_lminpercent  ;
last_4char_v2=compress(last_4,'M');
last_4char_v3=compress(last_4char_v2,'I');
last_4char_v4=compress(last_4char_v3,'N');
last_4char_v5=compress(last_4char_v4,'%');
last_4num=input(last_4char_v5,3.);
RUN;

PROC FREQ DATA=last_4char_pulse_lminpercent2  order=freq;
TABLE  last_4num last_4char_v5;
RUN;

DATA cohort3232 (compress=yes); 
SET last_4char_pulse_lminpercent2;
if last_4num NE .;
if last_4num = VitalResultNumeric then equal=1; else equal=0;
RUN;

PROC FREQ DATA=cohort3232  order=freq;
TABLE equal; 
RUN;

PROC MEANS DATA=cohort3232  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  last_4num VitalResultNumeric;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this cohort3A*/
DATA cohort3a_20132017_test (compress=yes);
SET last_4char_pulse_lminpercent2;
if SupplementalO2='L/MIN%';
SpO2=VitalResultNumeric;
O2_LPM=0;
incoherent=0;
RUN;

/*check Saturation_LPM cohorts*/
PROC MEANS DATA=cohort3a_20132017_test  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  SpO2 O2_LPM;
RUN;
PROC FREQ DATA=cohort3a_20132017_test  order=freq;
TABLE O2_LPM SupplementalO2;
RUN;


DATA cohort3a_20132017 (compress=yes);  
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET cohort3a_20132017_test;
drop last_4 last_4char_v2-last_4char_v5 last_4num;
RUN;

/*Jack decided to label these as incoherent*/
DATA cohort3b_20132017_test   (compress=yes); 
SET last_4char_pulse_lminpercent2;
if SupplementalO2 NE 'L/MIN%';
SpO2=VitalResultNumeric;
O2_LPM=.; /*turn into % first*/
incoherent=1;
run;


/*Jack said he wants to look at distribution of the nn% */
DATA  cohort3b_20132017  (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET  cohort3b_20132017_test ;
/*drop last_4 last_4char_v2-last_4char_v5 last_4num;*/
RUN;

PROC MEANS DATA=cohort3b_20132017   MIN MAX MEAN MEDIAN Q1 Q3;
VAR last_4num ;
RUN;

proc sgplot data=cohort3b_20132017 noautolegend;
 histogram last_4num;
 density last_4num;
run;

/**************************************************************************************/
/*combine pulse_lminpercent_only+pulse_lmin_only+pulse_noLmin2 and see what is not in those datasets, do frequency check of those units*/
DATA all (compress=yes); 
SET pulse_lminpercent_only pulse_lmin_only pulse_noLmin2;
RUN;

PROC SQL;
CREATE TABLE  whatisleft  (COMPRESS=YES) AS 
SELECT A.* FROM PulseOx20132017_v2 AS A
WHERE A.obs not IN (SELECT obs FROM work.all);
QUIT;

PROC FREQ DATA=whatisleft  order=freq;
TABLE  SupplementalO2;
RUN;


/*based on Jack's notes: if XX% not equal vitalresultsnumeric, then pull their TIU notes for validation, for "whatisleft" cohort*/
/*1) separate out the ##%*/ /*first, get the last 4 digits*/
DATA last_4charV1 (compress=yes); 
SET whatisleft;
last_4=substr(SupplementalO2,length(SupplementalO2)-3,4);
RUN;

/*look at list*/
PROC FREQ DATA=last_4charV1  order=freq;
TABLE  last_4;
RUN;

/*2/3/20: look at descriptive for this 126,091 cohort with only nnL/MIN%*/
DATA LMINpercentonly_v1 (compress=yes);
SET last_4charV1 ;
if last_4 = 'MIN%';
RUN;

PROC MEANS DATA=LMINpercentonly_v1  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

/*get the first 4 characters*/
DATA LMINpercentonly_v2  (compress=yes); 
SET  LMINpercentonly_v1 ;
first_4=substr(SupplementalO2,1,4);
RUN;

PROC FREQ DATA=LMINpercentonly_v2  order=freq;
TABLE  first_4;
RUN;

/*compress L/M*/
DATA  LMINpercentonly_v3;
SET  LMINpercentonly_v2;
first_4char_v2=compress(first_4,'M');
first_4char_v3=compress(first_4char_v2,'/');
first_4char_v4=compress(first_4char_v3,'L');
first_4num=input(first_4char_v4,3.);
RUN;

PROC MEANS DATA= LMINpercentonly_v3  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  first_4num VitalResultNumeric;
RUN;

/************/
/*Question: some last 4 characters have PRN%, NLT%, TO2%, exclude them?*/
PROC FREQ DATA=last_4charV1  order=freq;
TABLE  last_4;
RUN;


/*compress 1 step at a time*/
DATA  last_4charV2;  
SET  last_4charV1;
/*if Jack is ok with excluding the weird last 4 characters. Jack ok with deleting these on 3/2/20*/
if last_4 in ('NNC%','INO%','NLT%','PRN%','TO2%') then delete;
last_4char_v2=compress(last_4,'M');
last_4char_v3=compress(last_4char_v2,'I');
last_4char_v4=compress(last_4char_v3,'N');
last_4char_v5=compress(last_4char_v4,'%');
last_4num=input(last_4char_v5,3.);
RUN;

PROC FREQ DATA=last_4charV2   order=freq; 
TABLE  last_4char_v5;
RUN;

PROC MEANS DATA=last_4charV2   MIN MAX MEAN MEDIAN Q1 Q3; 
VAR  last_4num;
RUN;

PROC FREQ DATA=last_4charV2;
TABLE last_4num;
RUN;

/*to see if there are any last_4num = VitalResultNumeric*/
DATA cohort_20132017_test; 
SET last_4charV2;
if (last_4num NE . ) and (last_4num = VitalResultNumeric);
RUN;

/*2/3/20: look at nn L/Min descriptive*/
DATA cohort_20132017_test2  (compress=yes); 
SET cohort_20132017_test;
first_4=substr(SupplementalO2,1,4);
RUN;

PROC FREQ DATA=cohort_20132017_test2 order=freq;
TABLE  first_4;
RUN;

DATA cohort_20132017_test3 (compress=yes); 
SET cohort_20132017_test2;
first_4char_v2=compress(first_4,'M');
first_4char_v3=compress(first_4char_v2,'/');
first_4char_v4=compress(first_4char_v3,'L');
first_4num=input(first_4char_v4,3.);
RUN;

PROC MEANS DATA=cohort_20132017_test3   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  first_4num VitalResultNumeric;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this  cohort5.*/
DATA  cohort5_20132017_test  (compress=yes); 
SET  cohort_20132017_test3;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=first_4num;
RUN;

/*check*/
PROC MEANS DATA=cohort5_20132017_test  MIN MAX MEAN MEDIAN Q1 Q3;
VAR O2_LPM SpO2;
RUN;

DATA  cohort5_20132017  (compress=yes);
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET  cohort5_20132017_test;
drop last_4  last_4char_v2-last_4char_v5  last_4num first_4  first_4char_v2-first_4char_v4 first_4num;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this  cohort4.*/
DATA cohort4_20132017_test (compress=yes); 
SET LMINpercentonly_v3;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=first_4num;
RUN;

/*check*/
PROC MEANS DATA=cohort4_20132017_test MIN MAX MEAN MEDIAN Q1 Q3;
VAR  SpO2 O2_LPM;
RUN;
PROC FREQ DATA=cohort4_20132017_test  order=freq;
TABLE  incoherent SpO2;
RUN;

DATA cohort4_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET  cohort4_20132017_test ;
drop last_4  first_4  first_4num  first_4char_v2-first_4char_v4;
RUN;

/****************************************************************************************/
/*look at those that  last_4num not equal to vitalresultnumeric*/
DATA  val_cohort (compress=yes); 
SET last_4charV2;
if (last_4num NE . ) and (last_4num NE VitalResultNumeric);
RUN;


/*use Jack's conversions to get Saturation (L/MIN), call this  cohort6.*/
DATA cohort6_20132017_test (compress=yes); 
SET val_cohort;
SpO2=VitalResultNumeric;
incoherent=1;
O2_LPM=.;
RUN;

/*check*/
PROC MEANS DATA=cohort6_20132017_test   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  O2_LPM SpO2;
RUN;
PROC FREQ DATA=cohort6_20132017_test  order=freq;
TABLE  incoherent ;
RUN;

DATA cohort6_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET cohort6_20132017_test;
drop last_4  last_4char_v2-last_4char_v5 last_4num;
RUN;

/*combine all cohorts (cleaned and don't clean) and make sure the totals add up, drop the inconsistant cohorts*/
DATA PulseOx_cleaned (compress=yes);  /*1908985*/
SET DONT_CLEAN_COHORT COHORT1A_20132017  COHORT2_20132017 COHORT3A_20132017  COHORT4_20132017 COHORT5_20132017;
RUN;

/*Only keep those with 4 or more O2_LPM*/
data pulseox.Pulseox_clean&todaysdate (compress=yes); 
set PulseOx_cleaned;
if O2_LPM >=4;
O2_GE4LPM_ind=1;
keep patienticn vital_date O2_LPM O2_GE4LPM_ind SpO2 vitalSignTakenDateTime SupplementalO2 VitalResultNumeric;
run;

proc freq data=pulseox.Pulseox_clean&todaysdate order=freq;
table SupplementalO2;
run;

proc sort data=pulseox.Pulseox_clean&todaysdate nodupkey;
by patienticn O2_LPM vitalSignTakenDateTime;
run;

