/*Author: Shirley Wang (xiaoqing.wang@va.gov)*/
/*Download the CDW dflt table from step 1 into a SAS dataset*/
libname sepsis '/data/dart/2021/Data/inpatient'; 

proc datasets lib=DIM_RB02;
proc datasets lib=PRE45DFT;
run;

/*SAS Macro of today's date*/
%let todaysdate=%sysfunc(today(), yymmddn8.);
%put &todaysdate;

data OS_cohort_&todaysdate (compress=yes rename=patienticn2=patienticn); /*insert dataset name from SQL*/
set sepsis.OS_cohort_&todaysdate; /*insert dataset name from SQL*/
patienticn2=input(patienticn, 10.);
drop patienticn;
run;

%let year=2021;
options compress=yes;

/*run the code for single-site VAPD*/
/*check*/
proc sort data=OS_cohort_&todaysdate  nodup out=Checking_obs  (compress=yes); /*0 dups*/
by patienticn;
run;

data obs (compress=yes); /*all acute=1 indicated, correct*/
set OS_cohort_&todaysdate;
if specialty in ('MEDICAL OBSERVATION','SURGICAL OBSERVATION','ED OBSERVATION','NEUROLOGY OBSERVATION');
run;

data vapd_inpat&year. (compress=yes);
set OS_cohort_&todaysdate ;
if specialty in ('CARDIAC INTENSIVE CARE UNIT', 'MEDICAL ICU', 'SURGICAL ICU') then icu=1;
else icu=0;
admityear=year(datepart(admitdatetime));
run;

/*Identify hospitalizations with erroneous DOD*/
data vapd_inpat&year. (compress=yes);
set vapd_inpat&year.;
if not missing(dod) then do;
disdate1=datepart(intnx ('dtday', dischargedatetime, -1));
end;
format disdate1 date9.;
run;

data error_dod (compress=yes);
set vapd_inpat&year. (keep=inpatientsid dischargedatetime disdate1 dod);
if not missing(dod) then do;
if dod<disdate1 then output;
end;
run;

/*Remove hospitalizations with erroneous DOD*/
proc sql;
create table vapd_inpat&year._V2 (compress=yes) AS
select a.* 
from vapd_inpat&year. a
left join error_dod b on a.inpatientsid=b.inpatientsid
where b.inpatientsid is null;
quit;

PROC FREQ DATA=vapd_inpat&year._V2  order=freq;
TABLE icd10code;
RUN;

data missing_diag; 
set vapd_inpat&year._V2;
if icd10code='' or icd9code='';
run;

/*Indicate whether the diagnosis codes are in ICD-9 or ICD-10*/
data vapd_inpat&year._V3 (compress=yes);
set vapd_inpat&year._V2;
if (icd10code ne "*Unknown at this time*" or icd10code ne '')  and icd9code="*Unknown at this time*" then icdtype="ICD10";
else if (icd9code ne "*Unknown at this time*" or icd9code ne '') and icd10code="*Unknown at this time*" then icdtype="ICD9";
run;

data check; 
set vapd_inpat&year._V3;
if icdtype='';
run;

/*Sort by patient, admission, ordinal number.*/
proc sort data=vapd_inpat&year._V3; 
by patienticn inpatientsid specialtytransferdatetime OrdinalNumber; 
run;

/*Output the ICD-9 and ICD-10 diagnoses into seperate datasets*/
data icd9 icd10;
set vapd_inpat&year._V3;
if icdtype="ICD9" then output icd9;
if icdtype="ICD10" then output icd10;
run;

proc sort data=icd9 nodupkey; by InpatientSID specialtytransferdatetime ordinalNumber; run;
proc sort data=icd10 nodupkey; by inpatientsid specialtytransferdatetime ordinalnumber; run;

/*transpose diagnoses long to wide*/
proc transpose data=icd9 out=icd9_wide prefix=icd9code;
by inpatientsid specialtytransferdatetime;
var icd9code;
run;

proc sort data=icd9_wide nodupkey; 
by inpatientsid specialtytransferdatetime; 
run;

data icd9_wide;
set icd9_wide;
icdtype='ICD9';
run;

proc transpose data=icd10 out=icd10_wide prefix=icd10code;
by inpatientsid specialtytransferdatetime;
var icd10code;
run;

proc sort data=icd10_wide nodupkey; 
by inpatientsid specialtytransferdatetime; 
run;

data icd10_wide;
set icd10_wide;
icdtype='ICD10';
run;

data both (compress=yes);
set icd10_wide icd9_wide;
drop _name_ _label_;
run;                                  

data vapd_inpat&year._V3 (compress=yes); 
set vapd_inpat&year._V3;
drop ordinalnumber icd10code icd9code disdate1;
run;

/*select distinct observations (one row for each dx becomes one row for each specialty stay)*/
proc sort data=vapd_inpat&year._V3 out=vapd_inpat nodup; 
by patienticn inpatientsid specialtytransferdatetime;
run;

/*join the diagnosis codes*/
proc sql;
create table vapd_inpat&year. (compress=yes) as  
select a.*, b.*
from vapd_inpat a
left join both b on a.inpatientsid=b.inpatientsid and a.specialtytransferdatetime=b.specialtytransferdatetime;
quit;

/*Distinct hospitalizations by year*/
proc sql;
select admityear, count(distinct inpatientsid)
from vapd_inpat&year.
where acute=1
group by admityear;
quit;

/******************************************************************************************************************************/

/*This program adjusts the admission and discharge dates to reflect time spent in an acute setting
at a particular hospital. 
Additionally, this program calculates 30-day readmission, in-hospital mortality
Finally, this program creates a row for each patient-facility-day*/

data obs2_check (compress=yes); /*all acute=1 indicated, correct*/
set vapd_inpat&year.;
if specialty in ('MEDICAL OBSERVATION','SURGICAL OBSERVATION','ED OBSERVATION','NEUROLOGY OBSERVATION');
run;

data inp&year.; 
set vapd_inpat&year.;
run;

DATA  check_missing_sta6a (compress=yes); 
SET  vapd_inpat&year.;
if sta6a in ('*Missing*','','*Unknown at this time*');
RUN;

/*We will create new admission and new discharge dates, which will reflect the 
acute portions of stays. So we identify the cdw admission and discharge dates
for future reference if needed*/
data inp&year. (compress=yes);
set inp&year.;
	cdw_admitdatetime=admitdatetime;
	cdw_dischargedatetime=dischargedatetime;
	newdischargedatetime=dischargedatetime;
	newadmitdatetime=admitdatetime;
	specialtytransferdate=datepart(specialtytransferdatetime);
	newadmitdate=datepart(newadmitdatetime);
	newdischargedate=datepart(newdischargedatetime);
format cdw_admitdatetime cdw_dischargedatetime newadmitdatetime 
newdischargedatetime specialtytransferdatetime datetime22.
specialtytransferdate newadmitdate newdischargedate date9.;
run;

/*Identify ICU admit date for ICU stays*/
data inp&year.; 
set inp&year.;
if icu=1 then do;
admitdatetime_ICU=specialtytransferdatetime;
admitdate_ICU=datepart(admitdatetime_ICU);
end;
format admitdatetime_ICU DATETIME22. admitdate_ICU date9.;
run;

/*remove errors in the specialty tranfer date relative to the discharge date*/
data inp&year. (compress=yes);
set inp&year.;
	if newdischargedate<specialtytransferdate then delete;
run;

/*Create an indicator for the first specialty visit (and last specialty visit) during an episode of care.
	If admitted to the hospital on the same day as specialty transfer, then first specialty visit*/
proc sort data=inp&year.; 
by patienticn sta6a newadmitdatetime specialtytransferdatetime; 
run;  

/*enumerate the specialty visit for each hospitalization*/
data inp&year. (compress=yes);
set inp&year.;
	by patienticn sta6a newadmitdatetime;
		specialtyvisit+1;
		if first.newadmitdatetime then specialtyvisit=1;
		if first.newadmitdatetime then firstvisit=1;
		if last.newadmitdatetime then lastvisit=1;	
run;

/*create an indicator for problems with admissiondate ne specialtytransferdate for the first visit*/
data inp&year. (compress=yes);  
set inp&year.;
	issue=0;
	if firstvisit=1 then do;
		if newadmitdate~=specialtytransferdate then issue=1;
	end;
run; 

/*by how much?*/
proc sort data=inp&year.; 
by patienticn newadmitdate specialtytransferdate; 
run;

data inp&year.;
set inp&year.;
by patienticn newadmitdate;
retain error 0;
	if firstvisit=1 and issue=0 then error=0;
	if firstvisit=1 and issue=1 then error=specialtytransferdate-newadmitdate;
run;

data error;
set inp&year.;
where error ne 0;
run;

data error;
set error;
where acute=1;
run;

/*For visits with an error ne 0, remove the entire hospitalization*/
data inp&year.;
set inp&year.;
	where error = 0;
run; 

proc sort data=inp&year. nodupkey; 
by patienticn specialtytransferdatetime sta6a specialty descending icu; 
run;

proc sort data=inp&year. nodupkey dupout=dups;
by patienticn specialtytransferdatetime sta6a specialty; 
run;

/*Address issue where a bed is held in non-acute setting
--same discharge dates, but different admission dates*/
proc sort data=inp&year.; 
by patienticn newdischargedatetime newadmitdatetime; 
run;

data inp&year. (compress=yes);
set inp&year. ;
by patienticn newdischargedatetime;
		previous_admit=ifn(first.newdischargedatetime=0, lag(newadmitdatetime),.);
	format  newadmitdatetime newdischargedatetime specialtytransferdatetime previous_admit datetime22.;
	informat newadmitdatetime newdischargedatetime specialtytransferdatetime previous_admit datetime22.;
run;

proc sort data=inp&year.; 
by patienticn newdischargedatetime descending newadmitdatetime; 
run;

data inp&year. (compress=yes);
set inp&year. ;
by patienticn newdischargedatetime;
		next_admit=ifn(first.newdischargedatetime=0, lag(newadmitdatetime),.);
	format  next_admit datetime22.;
	informat next_admit datetime22.;
run;

proc sort data=inp&year.; by patienticn newdischargedatetime newadmitdatetime; run;

data inp&year.;
set inp&year. ;
by patienticn newdischargedatetime;
retain flag2;
if first.newdischargedatetime then flag2=.;
	if not missing(previous_admit) and newadmitdatetime ne previous_admit then flag2=1;
	if not missing(next_admit) and newadmitdatetime ne next_admit then flag2=1;
run;

data bedheld inp&year. (compress=yes);
set inp&year.;
	if flag2=1 then output bedheld;
	if flag2=. then output inp&year.;
run;

proc sort data=bedheld; by patienticn newadmitdatetime; run;

data bedheld;
set bedheld;
	by patienticn newdischargedatetime;
		specialtyvisit+1;
		if first.newdischargedatetime then specialtyvisit=1;
		if first.newdischargedatetime then firstvisit=1;
		if last.newdischargedatetime then lastvisit=1;	
run;

data bedheld (compress=yes);
set bedheld;
if firstvisit=1 and lastvisit=1 then specialtytransfer=0;
	else specialtytransfer=1;
run;

proc sort data=bedheld; by patienticn newdischargedatetime newadmitdatetime; run;

data bedheld (compress=yes);
set bedheld; 
	by patienticn newdischargedatetime newadmitdatetime;
	retain specialtydischargedatetime;
		if newadmitdatetime=next_admit or newadmitdatetime=previous_admit then specialtydischargedatetime=newdischargedatetime;
		else specialtydischargedatetime=.;
		format specialtydischargedatetime datetime22.;
		informat specialtydischargedatetime datetime22.;	
run;

data bedheld (compress=yes);
set bedheld; 
	if specialtydischargedatetime=. then do;
		specialtydischargedatetime=next_admit;
	end;
run;

data bedheld (compress=yes);
set bedheld; 
	if specialtydischargedatetime=. then do;
		specialtydischargedatetime=newdischargedatetime;
	end;
run;

/*Adjust the discharge date times accordingly*/
data bedheld (compress=yes);
set bedheld;
newdischargedatetime=specialtydischargedatetime;
run;

/*Add the adjusted bedheld dataset back*/
data inp&year. (compress=yes);
set inp&year. bedheld;
run;

/*Add specialtydischargedatetime*/
proc sort data=inp&year. nodupkey; by patienticn newadmitdatetime descending specialtytransferdatetime; run;

data inp&year. (compress=yes);
set inp&year.;
	by patienticn newadmitdatetime;
		specialtydischargedatetime=ifn(first.newadmitdatetime=0, lag(specialtytransferdatetime), newdischargedatetime); 
			/*next specialty transfer date*/
	run;

/*Is the veteran ever transfered between specialties?*/
data inp&year. (compress=yes);
set inp&year.;
	if firstvisit=1 and lastvisit=1 then specialtytransfer=0;
		else specialtytransfer=1;
run;

data inp&year. (compress=yes);
set inp&year.;
	specialtydischargedate=datepart(specialtydischargedatetime);
run;

	/*no*/
		data notransfers;
		set inp&year.;
			where specialtytransfer=0 ; 
		run;

		/*keep only acute inpatient*/
		data notransfers;
		set notransfers;
			where acute=1;
		run;

	/*yes*/
		data transfers;
		set inp&year.;
			where specialtytransfer=1; 
		run;

proc sort data=transfers nodupkey; by patienticn newadmitdatetime specialtytransferdatetime; run;


/*reenumerate*/
data transfers;
set transfers;
	drop specialtyvisit firstvisit lastvisit;
run;

proc sort data=transfers ; by patienticn newadmitdatetime specialtytransferdatetime newdischargedatetime;
run;

data transfers (compress=yes);
set transfers;
	by patienticn newadmitdatetime;
		specialtyvisit+1;
			if first.newadmitdatetime then specialtyvisit=1;
			if first.newadmitdatetime then firstvisit=1;
			if last.newadmitdatetime then lastvisit=1;	
run;

/*Does the inpatient stay contain both acute and non-acute specialties?*/
proc sort data=transfers;
	by patienticn newadmitdatetime specialtytransferdatetime; run;

/*Calculate the mean value for the acute value.
		if mean=1 then all specialties were acute. if mean=0 then all specialties were non-acute*/
proc sql;
	create table mean_inpat as
	select patienticn, newadmitdatetime, mean(acute) as mean_acute
	from transfers
	group by patienticn, newadmitdatetime;
quit;

/*join this mean to our transfer dataset*/
proc sql;
	create table transfers1 as
	select a.*, b.mean_acute
	from transfers a
	left join mean_inpat b
	on a.patienticn=b.patienticn and a.newadmitdatetime=b.newadmitdatetime;
quit;

/*Does the inpatient stay contain both acute and non-acute specialties?*/

/*Yes*/
data transfers;
set transfers1;
where mean_acute not in (0,1);
run;

/*No--Keep only the acute visits*/
data allacute;
set transfers1;
	where mean_acute=1;
run;



/*********Collapsing hospitalization with acute and non-acute transfers *********/

/*Drop "non-acute" portions of the inpatient stay. 
If there is >1 day between "acute" portions, then this is considered a new stay*/
proc sort data=transfers nodupkey; 
by patienticn newadmitdatetime descending specialtytransferdatetime icu; 
run;

/*Create the next specialty admission date*/
data transfers (compress=yes);
set transfers;
	nextadmdate=specialtydischargedate;
	format nextadmdate date9.;
run;

/*create the previous discharge date*/
proc sort data=transfers; by patienticn newadmitdatetime specialtytransferdatetime; run;

data transfers (compress=yes);
set transfers;
	by patienticn newadmitdatetime;
		previousdisdate=ifn(first.patienticn=0 and first.newadmitdatetime=0, lag(specialtydischargedate),.); 
			/*previous specialty discharge date*/
	format previousdisdate date9.;
run;

/*Step 2--Remove all non-acute inpatient*/
data transfers;
set transfers;
	where acute=1;
run;

proc sort data=transfers; by patienticn newadmitdatetime; run;

data transfers; set transfers; drop specialtyvisit firstvisit lastvisit; run;

data transfers (compress=yes);
set transfers;
		by patienticn newadmitdatetime;
			specialtyvisit+1;
				if first.newadmitdatetime then specialtyvisit=1;
				if first.newadmitdatetime then firstvisit=1;
				if last.newadmitdatetime then lastvisit=1;	
	run;

/*Step 3--create nextspecadm and previousspecdis variables*/
proc sort data=transfers; by patienticn specialtytransferdatetime; run;

data transfers (compress=yes);
set transfers;
	by patienticn specialtytransferdate;
		previousspecdis=ifn(first.patienticn=0, lag(specialtydischargedate),.); /*previous specialty discharge date*/
format previousspecdis specialtydischargedate date9.;
run;

proc sort data=transfers; by patienticn descending specialtytransferdate; run;

data transfers;
set transfers;
	by patienticn;
		nextspecadm=ifn(first.patienticn=0, lag(specialtytransferdate),.); 
			/*next specialty transfer date*/
	format nextspecadm date9.;
	run;

/*If the next hospital admission is within one day of next inpat admission that counts as same hospitalization
	Otherwise, we have a new hospitalization
	Create a new indicator for firstvist and renumerate the specialtyvisit*/

proc sort data=transfers; by patienticn specialtytransferdate; run; 

data transfers;
set transfers;
	drop firstvisit lastvisit specialtyvisit; run;

data transfers (compress=yes);
set transfers;
	by patienticn;
	if first.patienticn then firstvisit=1;
		else if specialtytransferdate>previousspecdis+1 then firstvisit=1;/*If the current specialty transfer is 
		more than one day later than the previous specialty discharge, then consider new hospitalization*/
run;

data transfers (compress=yes);
set transfers;
		specialtyvisit+1;
		by patienticn;
			if first.patienticn then specialtyvisit=1;
			else if specialtytransferdate>previousspecdis+1 then specialtyvisit=1;
run;

/*Change the admission dates*/
data transfers (compress=yes);
set transfers;
	by patienticn;
	retain newadmitdatetime2;
		if firstvisit=1 then newadmitdatetime2=specialtytransferdatetime;
	format newadmitdatetime2 datetime22.3;
	run;

data transfers (compress=yes);
set transfers;
if not missing(newadmitdatetime2) then newadmitdatetime=newadmitdatetime2;
drop newadmitdatetime2;
format newadmitdatetime datetime22.3;
run;

data transfers (compress=yes);
set transfers;
newadmitdate=datepart(newadmitdatetime);
format newadmitdate date9.;
run;

proc sort data=transfers; by patienticn newadmitdate specialtyvisit; run;

/*create and indicator for last visit*/
data transfers (compress=yes);
set transfers;
	by patienticn newadmitdate;
		if last.newadmitdate then lastvisit=1;
	run;

/*Change the discharge dates*/
proc sort data=transfers;
	by patienticn newadmitdate descending specialtyvisit;
run; 

data transfers (compress=yes);
set transfers;
	by patienticn newadmitdate;
	retain newdischargedatetime2;
		if lastvisit=1 then newdischargedatetime2=specialtydischargedatetime;
		format newdischargedatetime2 datetime22.3;
	run;

data transfers (compress=yes);
set transfers;
if not missing(newdischargedatetime2) then newdischargedatetime=newdischargedatetime2;
newdischargedate=datepart(newdischargedatetime);
drop newdischargedatetime2;
run;

data transfers (compress=yes);
set transfers;
	newadmitdate=datepart(newadmitdatetime);
	newdischargedate=datepart(newdischargedatetime);
	format newadmitdate newdischargedate date9.;
run;

/*Now merge back together to capture all acute hospitalizations for year &year.*/
data allacute&year._12182018;
set notransfers allacute transfers;
run; 

data allacute&year._12182018 (compress=yes);  
set allacute&year._12182018;
format newadmitdate mmddyy10.  newdischargedate mmddyy10. specialtydischargedate mmddyy10. specialtydischargedate mmddyy10.;
run;


/**** Noticed that Med, Surg, ED, & Neuro Observations were mostly coded as separate hospitalizations under cdw_admitdatetime & cdw_dischargedatetime.
Therefore, whenever the patient have an Observation stay, the hospitalizations date were not rolled up like it should.
To further code in order to roll up the hospitalization dates ****/

/*1.assign each patienticn, newadmitdate & newdischargedate a unique hosp id*/;
/*create unique patient hosp count*/
PROC SORT DATA=allacute&year._12182018  nodupkey  OUT=final_copy_undup2 (compress=yes); 
BY  patientsid sta6a newadmitdate newdischargedate;
RUN;

DATA final_copy_undup2 (compress=yes); 
SET final_copy_undup2 ;
unique_hosp=_N_; 
RUN;

/*match unique_hosp back to original dataset allacute&year._12182018*/
PROC SQL;
	CREATE TABLE  final_copy2  (compress=yes)  AS  
	SELECT A.*, B.unique_hosp
	FROM  allacute&year._12182018  A
	LEFT JOIN final_copy_undup2  B ON A.patientsid =B.patientsid and a.sta6a=b.sta6a 
             and a.newadmitdate=b.newadmitdate and a.newdischargedate=b.newdischargedate;
QUIT;

/*use specialtytransferdatetime & specialtydischargedatetime*/
PROC SORT DATA=final_copy2; 
BY patientsid unique_hosp sta6a specialtytransferdatetime  specialtydischargedatetime;
RUN;

DATA final_copy3 (compress=yes); 
SET  final_copy2;
by patientsid;
if first.patientsid  then do;
	lag_specialtydischargedate=specialtydischargedate;  end;  /*create a lag_specialtydischargedate for first unique patient, because they shouldn't have a lag_specialtydischargedate, so it is = specialtydischargedate */
lag_specialtydischargedate2=lag(specialtydischargedate); /*create a lag_specialtydischargedate2*/
format lag_specialtydischargedate mmddyy10.  lag_specialtydischargedate2 mmddyy10.;
RUN;

/*if lag_specialtydischargedate is missing, then replace it with lag_specialtydischargedate2*/
DATA final_copy4 (compress=yes); 
SET final_copy3;
if lag_specialtydischargedate NE . then lag_specialtydischargedate2= .;
if lag_specialtydischargedate = . then lag_specialtydischargedate=lag_specialtydischargedate2;
drop lag_specialtydischargedate2;
diff_days=specialtytransferdate-lag_specialtydischargedate; /*calculate date difference from last specialty discharge*/
RUN;

/*sta6a should be within each unique hosp not patienticn*/
/*by unique_hosp, get lag_sta6a=sta6a  first for the first admit date, should be the same*/
PROC SORT DATA=final_copy4   OUT=final_copy5 (compress=yes) ;
BY  unique_hosp;
RUN;

data final_copy6 (compress=yes);
set final_copy5;
by unique_hosp;
if first.unique_hosp then do;
	lag_sta6a=sta6a;  end;
lag_sta6a2=lag(sta6a);
run;

/*if lag_newadmitdatetime and lag_sta6a is missing, then replace it with lag_newadmitdatetime_v2*/
DATA final_copy8 (compress=yes); 
SET  final_copy6 ;
if lag_sta6a NE '' then lag_sta6a2= '';
if lag_sta6a = '' then lag_sta6a=lag_sta6a2;
drop lag_sta6a2 ;
run;

/*create first patienticn indicator, if first.patientinc is true, then it's a new hosp*/
DATA  final_copy8b (compress=yes);
SET final_copy8 ;
by patientsid ;
if first.patientsid  then first_pat=0;
 first_pat+1;
RUN;

/*if diff_days >1 or  diff_days<0 then it's a new hosp, also check if it's the same facility*/
DATA final_copy9 (compress=yes);
SET final_copy8b;
if (first_pat=1 and diff_days=0)  /*on a sorted dataset, if it's a first unique patient, then it is a new hosp and diff_days should be 0*/
OR
((diff_days >1 or diff_days<0) and (lag_sta6a=sta6a))  /*in same facility, if diff_days >1 then it is a new hosp, also if admit-previous discharge<0 then it is new hosp because admission < discharge date if fist hosp*/
	then new_hosp_ind=1; else new_hosp_ind=0;
RUN;

/*check to see previous step works before only selecting new_hosp_ind=1*/
DATA  final_copy10 (compress=yes); 
SET final_copy9;
if new_hosp_ind=1;
RUN;

/*assign each unique_hosp and new_hosp_ind a unique ID*/
PROC SORT DATA=final_copy10  nodupkey  OUT=Unique_hosp_ind (compress=yes); 
BY  patientsid sta6a unique_hosp new_hosp_ind;
RUN;

DATA Unique_hosp_ind (compress=yes);  
SET  Unique_hosp_ind;
Unique_hosp_ind=_n_;
RUN;

/*left join Unique_hosp_ind back to original dataset final_copy9*/
PROC SQL;
	CREATE TABLE  final_copy11 (compress=yes)  AS 
	SELECT A.*, B.Unique_hosp_ind
	FROM  final_copy9 A
	LEFT JOIN Unique_hosp_ind  B ON A.patientsid =B.patientsid and a.sta6a=b.sta6a and a.unique_hosp=b.unique_hosp;
QUIT;

/*fill down in a table for Unique_hosp_ind*/
data  final_copy12 (drop=filledx compress=yes);  
set final_copy11;
retain filledx; /*keeps the last non-missing value in memory*/
if not missing(Unique_hosp_ind) then filledx=Unique_hosp_ind; /*fills the new variable with non-missing value*/
Unique_hosp_ind=filledx;
run;

PROC SORT DATA=final_copy12;
BY  patienticn patientsid sta6a specialtytransferdatetime specialtydischargedatetime cdw_admitdatetime;
RUN;

/*use max and min group by Unique_ICU_specialty to get new speicaltytransferdate and specialtydischargedates*/
PROC SQL;
CREATE TABLE  final_copy13 (compress=yes) AS  
SELECT *, min(specialtytransferdate) as new_admitdate2, max(specialtydischargedate) as new_dischargedate2
FROM final_copy12
GROUP BY Unique_hosp_ind;
QUIT;

DATA final_copy13 (compress=yes); 
SET  final_copy13;
format new_admitdate2 mmddyy10. new_dischargedate2 mmddyy10.;
RUN;

/*check where new_admitdate2 NE newadmitdate or new_dischargedate2 NE newdischargedate*/
data check (compress=yes); 
retain patienticn patientsid sta3n sta6a  specialty specialtytransferdatetime  specialtydischargedatetime cdw_admitdatetime cdw_dischargedatetime 
newdischargedatetime  newadmitdatetime newadmitdate newdischargedate new_admitdate2 new_dischargedate2 Unique_hosp_ind unique_hosp;
set final_copy13;
if (new_admitdate2 NE newadmitdate) or (new_dischargedate2 NE newdischargedate);
keep patienticn patientsid sta3n sta6a  specialty specialtytransferdatetime  specialtydischargedatetime cdw_admitdatetime cdw_dischargedatetime 
newdischargedatetime  newadmitdatetime newadmitdate newdischargedate new_admitdate2 new_dischargedate2 Unique_hosp_ind unique_hosp;
run;

PROC SORT DATA=check;
BY patienticn sta3n sta6a specialtytransferdatetime;
RUN;

PROC FREQ DATA=check  order=freq;
TABLE  specialty;
RUN;

DATA allacute&year._01022019 (compress=yes); /*n=1,965,310*/
SET  final_copy13;
RUN;

proc sort data=allacute&year._01022019  nodupkey; 
	by patienticn specialty specialtytransferdatetime specialtydischargedatetime; run;
/*specialty was added to this above code on 6/21/19, pointed out by Brenda*/


/*run the code for VA to VA transfer VAPD*/
/*********************************DAILY VAPD 2019-2020********************************************/
/*roll specialty stays out into daily dataset*/
data allacute&year. (compress=yes);
set allacute&year._01022019;
format specialtytransferdate specialtydischargedate date9.;
run;

/*Create a row for each calendar day at a given STA6A*/
data vapd_daily&year. (compress=yes); 
set allacute&year.;
do datevalue=specialtytransferdate to specialtydischargedate;
	datevalue=datevalue; output;
end;
format datevalue mmddyy10.;
run;

/*** Turn single-site VAPD into VA to VA transfer VAPD ***/
DATA  vatova_v1 (compress=yes); 
SET vapd_daily&year. ;
RUN;

/*Shirley's new code to roll up the VA to VA transfers in VAPD*/
/*1. sort the dataset by patient and admit/discharge dates*/
/*create unique patient hosp count*/
PROC SORT DATA=vatova_v1 nodupkey out=testb; 
BY  patienticn new_admitdate2 new_dischargedate2;
RUN;

DATA testb (compress=yes); 
SET testb ;
unique_hosp=_N_; 
RUN;

PROC SORT DATA=testb;
BY patienticn new_admitdate2 new_dischargedate2;
RUN;

/*label first patieticn, if first.patienticn then lag_discharge=discharge*/
DATA test2 (compress=yes);
SET testb;
by patienticn;
if first.patienticn then do;
	lag_new_dischargedate=new_dischargedate2;  end;  /*create a lag_new_dischargedate for first unique patient, because they shouldn't have a lag_new_dischargedate, so it is =new_dischargedate2 */
    lag_new_dischargedate2=lag(new_dischargedate2); /*create a lag_new_dischargedate2*/
format lag_new_dischargedate mmddyy10.  lag_new_dischargedate2 mmddyy10.;
RUN;

/*if lag_new_dischargedate is missing, then replace it with lag_new_dischargedate2*/
DATA test3 (compress=yes); 
SET test2;
if lag_new_dischargedate NE . then lag_new_dischargedate2= .;
if lag_new_dischargedate = . then lag_new_dischargedate=lag_new_dischargedate2;
drop lag_new_dischargedate2;
diff_days=new_admitdate2 -lag_new_dischargedate; /*calculate date difference from last hosp discharge*/
RUN;

/*create first patienticn indicator, if first.patientinc is true, then it's a new hosp*/
DATA  test3 (compress=yes); 
SET test3 ;
by patienticn;
if first.patienticn then first_pat=0;
 first_pat+1;
RUN;

DATA test4 (compress=yes);
SET test3;
if (first_pat=1 ) OR (diff_days not in (1,0) )/*regardless of sta6a, if diff_days =0 then it is a new hosp*/
	then new_hosp_ind=1; else new_hosp_ind=0;
RUN;

/*check to see previous step works before only selecting new_hosp_ind=1*/
DATA  test5 (compress=yes);
SET test4;
if new_hosp_ind=1;
RUN;

/*assign each unique_hosp and new_hosp_ind a unique ID*/
PROC SORT DATA= test5 nodupkey  OUT=Unique_hosp_ind (compress=yes); 
BY  patienticn unique_hosp new_hosp_ind;
RUN;

DATA Unique_hosp_ind (compress=yes);
SET  Unique_hosp_ind;
Unique_hosp_ind2=_n_;
RUN;

/*left join Unique_hosp_ind back to original dataset final_copy9*/
PROC SQL;
	CREATE TABLE test6 (compress=yes)  AS 
	SELECT A.*, B.Unique_hosp_ind2
	FROM test4 A
	LEFT JOIN Unique_hosp_ind  B ON A.patienticn =B.patienticn and a.unique_hosp=b.unique_hosp;
QUIT;

/*fill down in a table for Unique_hosp_ind*/
data test7 (drop=filledx compress=yes); 
set test6;
retain filledx; /*keeps the last non-missing value in memory*/
if not missing(Unique_hosp_ind2) then filledx=Unique_hosp_ind2; /*fills the new variable with non-missing value*/
Unique_hosp_ind2=filledx;
run;

PROC SORT DATA=test7;
BY  patienticn new_admitdate2 new_dischargedate2;
RUN;

/*use max and min group by Unique_ICU_specialty to get new speicaltytransferdate and specialtydischargedates*/
PROC SQL;
CREATE TABLE test8 (compress=yes) AS 
SELECT *, min(new_admitdate2) as new_admitdate3, max(new_dischargedate2) as new_dischargedate3
FROM test7
GROUP BY Unique_hosp_ind2;
QUIT;

DATA test8 (compress=yes); 
SET  test8;
format new_admitdate3 mmddyy10. new_dischargedate3 mmddyy10.;
RUN;

PROC SORT DATA=test8;
BY  patienticn new_admitdate2 new_dischargedate2;
RUN;

/*check where new_admitdate2 NE new_admitdate3 or new_dischargedate2 NE new_admitdate3*/
data check_data (compress=yes); 
set test8;
if (new_admitdate2 NE new_admitdate3) or (new_dischargedate2 NE new_dischargedate3);
keep patienticn sta6a new_admitdate2 new_admitdate3 new_dischargedate2 new_dischargedate3 
specialty /*specialtytransferdatetime specialtydischargedatetime*/ unique_hosp_ind2;
run;

PROC SORT DATA=check_data;
BY Unique_hosp_ind2 new_admitdate2 new_dischargedate2;
RUN;

DATA check_data;
SET check_data;
by Unique_hosp_ind2;
IF FIRST.Unique_hosp_ind2 THEN keep = 1; else keep=0;
RUN;

DATA test9 (compress=yes); 
SET test8;
admityear=year(new_admitdate3);
RUN;

PROC SORT DATA=test9  nodupkey  OUT= test9_unique_hosp; 
BY patienticn new_admitdate3 new_dischargedate3;
RUN;

PROC FREQ DATA=test9_unique_hosp  order=freq;
TABLE  admityear;
RUN;

PROC FREQ DATA=vatova_v1  order=freq;
TABLE  admityear;
RUN;

/*left join new_admitdate3 & new_dischargedate3 to updated VAPD*/
PROC SQL;
	CREATE TABLE  vapd_daily&year._20190516  (compress=yes)  AS 
	SELECT A.*, B.new_admitdate3, b.new_dischargedate3
	FROM  vatova_v1  A
	LEFT JOIN  test9 B
	ON A.patienticn=B.patienticn and a.new_admitdate2=b.new_admitdate2 and a.new_dischargedate2=b.new_dischargedate2;
QUIT;

/**************** Save a diagnosis dataset to look at end-stage renal disease in step 5  **************/
data sepsis.VAtoVA_dailyDiag_&todaysdate (compress=yes);
set vapd_daily&year._20190516;
keep patienticn new_admitdate3 new_dischargedate3 icd10code1-icd10code25;
run;
/*************************************************/

/*create new admityear based on new_admitdate3*/
DATA  vapd_daily_VAtoVA  (compress=yes); 
retain patienticn sta3n sta6a dod inhosp_mort specialtytransferdatetime specialtydischargedatetime specialty icu 
icdtype icd10code1 admityear new_admitdate3  new_dischargedate3 datevalue hosp_LOS;
SET  vapd_daily&year._20190516 ;
admityear=year(new_admitdate3);
hosp_LOS =(new_dischargedate3-new_admitdate3)+1;
if not missing(dod) then do;
	deathdaysafterdischarge=datdif(new_dischargedate3, dod, 'act/act');  
end;
if not missing(dod) and abs(deathdaysafterdischarge)<=1 then inhosp_mort=1;
	else inhosp_mort=0;
keep patienticn patientsid  sta3n sta6a dod inhosp_mort specialtytransferdatetime specialtydischargedatetime specialty icu 
icdtype icd10code1 admityear new_admitdate3  new_dischargedate3 datevalue hosp_LOS; 
RUN;

/*remove duplicate datevalues, sort so earliest specialtytransferdatetime is on top/kept*/
proc sort data=vapd_daily_VAtoVA;
by patienticn datevalue specialtytransferdatetime new_admitdate3  new_dischargedate3;
run;

proc sort data=vapd_daily_VAtoVA nodupkey out=vapd_daily_VAtoVA_b (compress=yes);
by patienticn datevalue  new_admitdate3  new_dischargedate3;
run;

/*create new hospital_day and unique_hosp_count_id for VA to VA transfer VAPD*/
PROC SORT DATA=vapd_daily_VAtoVA_b;  
BY patienticn  new_admitdate3 new_dischargedate3 datevalue;
RUN;

data VAtoVA_copy (compress=yes); /* 359249  pat-days*/
set vapd_daily_VAtoVA_b;
by patienticn  new_admitdate3;
if first.new_admitdate3 then hospital_day=0;
hospital_day+1;
run;

/*look at how many unique hosps*/
proc sort data=VAtoVA_copy nodupkey out=VAtoVA_copy_hosp (compress=yes); /*69043 hosps*/
by patienticn   new_admitdate3  new_dischargedate3;
run;

PROC SQL;
CREATE TABLE  days_check AS  /*should be 359249 days, yes matches up as above VAtoVA_copy  dataset*/
SELECT *, sum(hosp_LOS) as sum_hospital_day
FROM VAtoVA_copy_hosp;
QUIT;

/*assign unique hospitalization count ID*/;
DATA hosp; /*69043*/
SET VAtoVA_copy_hosp;
unique_hosp_count_id= _N_;
RUN;

PROC SQL;
	CREATE TABLE  unique_hosp_count_id (compress=yes)  AS   /* 359249 pat-days*/
	SELECT A.*, B.unique_hosp_count_id 
	FROM  VAtoVA_copy A
	LEFT JOIN hosp  B ON A.patienticn =B.patienticn and a.new_admitdate3=b.new_admitdate3 and a.new_dischargedate3=b.new_dischargedate3;
QUIT;

/*******************************************************************************/
/*create day 0s */
/*select day 1 of each hospitalization, so SpecialtyTransferDateTime is earliest for day 0*/
/*this step is different from the original code*/
DATA day1_only (compress=yes); /*69043 hosps*/
SET unique_hosp_count_id;
if hospital_day =1; /*N=69043, equals # of unique hospitalzations*/
RUN;

DATA  day0_revise  (compress=yes);
SET day1_only;
datevalue=(new_admitdate3-1); /*create new datevalue*/
hospital_day=0; /*overwrite hospital_day=1 to 0*/
format datevalue mmddyy10.;
RUN;

/*359249 + 69043=428,292 pat-fac-days*/
/*method 1: use append to combine the datasets*/
proc append base=unique_hosp_count_id  data=day0_revise force; 
run;

PROC SORT DATA=unique_hosp_count_id; 
BY unique_hosp_count_id datevalue;
RUN;

/*PROC SORT DATA=unique_hosp_count_id; */
/*BY new_dischargedate3;*/
/*RUN;*/



/***************************************************************************************************/
/* Once a new discharge date (new_dischargedate3) has been determined from step 2, 
can select patients who are alive on day after discharge AND discharged 
between 10/24/21-11/7/21 (this is the time frame for discharged within previous 2 weeks) AND 
hosp_LOS is between 3-42 days. */

/*SAS Macro of discharge date ranges*/
%let date1=%sysfunc(intnx(day,%sysfunc(today()),-2),date9.); %put &date1;
%let date2=%sysfunc(intnx(day,%sysfunc(today()),-16),date9.); %put &date2;

data sepsis.VAtoVA_daily_&todaysdate (compress=yes); 
set unique_hosp_count_id;
if inhosp_mort=0 and 
("&date2"d  <= new_dischargedate3 <= "&date1"d) and 
(3<=hosp_LOS<=42);
run;

proc sort data=sepsis.VAtoVA_daily_&todaysdate;
by descending new_dischargedate3;
run;

proc freq data=sepsis.VAtoVA_daily_&todaysdate;
table hosp_LOS;
run;


