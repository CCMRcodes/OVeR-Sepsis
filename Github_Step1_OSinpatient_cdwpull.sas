/*Step 1. SAS Pass Thru Code to pull in all inpatient speciality stays from CDW*/

libname sepsis '/data/dart/2021/Data/inpatient'; 

proc datasets lib=DIM_RB02;
proc datasets lib=PRE45DFT; /*will be different for each study*/
run;

/*SAS Macro of today's date*/
%let todaysdate=%sysfunc(today(), yymmddn8.);
%let DATASRCs=ORD_STUDYNAME;

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE sepsis.OS_cohort_&todaysdate AS
SELECT  *
FROM CONNECTION TO TUNNEL ( 

/*indicator for acute specialty*/
DROP TABLE IF EXISTS #acutespecialty;

select distinct treatingspecialtysid, specialtysid, treatingspecialtyname, specialty, 
case when specialty in ('ANESTHESIOLOGY', 
					'CARDIAC INTENSIVE CARE UNIT', 'CARDIAC SURGERY','CARDIAC-STEP DOWN UNIT',
					'CARDIOLOGY','DERMATOLOGY','EAR, NOSE, THROAT (ENT)','ENDOCRINOLOGY',
					'EPILEPSY CENTER','GASTROENTEROLOGY','GEM ACUTE MEDICINE','GEN MEDICINE (ACUTE)',
					'GENERAL SURGERY','GENERAL(ACUTE MEDICINE)','HEMATOLOGY/ONCOLOGY',
					'INTERMEDIATE MEDICINE','MEDICAL ICU','MEDICAL STEP DOWN','METABOLIC',
					'NEUROLOGY','NEUROSURGERY','OB/GYN','OPHTHALMOLOGY','ORAL SURGERY',
					'ORTHOPEDIC','PERIPHERAL VASCULAR','PLASTIC SURGERY','PODIATRY',
					'PROCTOLOGY','PULMONARY, NON-TB','PULMONARY, TUBERCULOSIS','STROKE UNIT',
					'SURGICAL ICU','SURGICAL STEPDOWN','TELEMETRY','THORACIC SURGERY','TRANSPLANTATION',
					'UROLOGY','VASCULAR','z GEM ACUTE MEDICINE','zCARDIAC-STEP DOWN UNIT','zCARDIOLOGY',
					'zDERMATOLOGY','zENDOCRINOLOGY','zEPILEPSY CENTER','zGASTROENTEROLOGY','zGENERAL(ACUTE MEDICINE',
					'zHEMATOLOGY/ONCOLOGY','zMETABOLIC','zNEUROLOGY','zNEUROSURGERY','zOPHTHALMOLOGY',
					'zORTHOPEDIC','zPERIPHERAL VASCULAR', 'zPODIATRY', 'zPROCTOLOGY', 'zPULMONARY, NON-TB',
					'zPULMONARY, TUBERCULOSI', 'zSTROKE UNIT','zSURGICAL ICU','zTELEMETRY','zUROLOGY',
					'ZZPULMONARY DISEASE','MEDICAL OBSERVATION','SURGICAL OBSERVATION','ED OBSERVATION','NEUROLOGY OBSERVATION',
					'CARDIAC STEP DOWN UNIT','DOD BEDS IN VA FACILITY','HIGH INTENSITY GEN INPT','PEDIATRICS') 
						 then 1 else 0 end as acute
into #acutespecialty
from cdwwork.dim.treatingspecialty ;



declare @currentdate date=getdate();
declare @startdate2 date=getdate()-58;
declare @enddate2 date=getdate()-5;

SELECT distinct 
		st.InpatientSID,
		st.PatientSID, 
		st.sta3n,
		st.SpecialtyTransferDateTime,
		st.specialtytransfersid, /*to join to dx table*/
		st.LOSInService, 
		a.specialty,
		a.acute, 
		flag='S'
into #vapd_inpatspec
FROM Src.Inpat_SpecialtyTransfer st
inner join #acutespecialty a /*get acute indicator and specialty name*/
on st.TreatingSpecialtySID=a.TreatingSpecialtySID
where st.admitdatetime >= @STARTDATE2 and st.admitdatetime<= @ENDDATE2 and st.InpatientSID>0 ;


/*get diagnosis codes*/
select a.*, b.POAIndicator, b.OrdinalNumber, c.icd9code, d.icd10code
into #vapd_inpatspec_dx
from #vapd_inpatspec a
left join SRC.Inpat_SpecialtyTransferDiagnosis b on a.specialtytransfersid=b.specialtytransfersid
left join cdwwork.dim.icd10 d on b.icd10sid=d.icd10sid
left join cdwwork.dim.icd9 c  on b.icd9sid=c.icd9sid;


DROP TABLE #vapd_inpatspec; 

alter table #vapd_inpatspec_dx 
drop column specialtytransfersid;

/*INPAT.INPATIENT RECORDS (to get DischargeDateTime and sta6a)*/
declare @startdate date=getdate()-58;
declare @enddate date=getdate()-5;

SELECT distinct 
		i.InpatientSID,
		i.PatientSID,
		i.sta3n,
		i.AdmitDateTime, 
		i.DischargeDateTime,
		w.sta6a,
		w.WardLocationSID,
		w.WardLocationName,
		w.DivisionSID,
		w.DivisionName,
		w.BedSection,
		w.MedicalService,
		w.PrimaryLocation,
		w.NursingService,
		w.GLOrder,
		w.LocationSID, /*added 9/23/19 to test getting bedsection back*/
		flag='I'
into #VAPD_inpat
FROM SRC.Inpat_Inpatient AS i
left join cdwwork.dim.wardlocation  w  /*get sta6a*/
on i.admitwardlocationsid=w.wardlocationsid
where i.admitdatetime >= @STARTDATE and i.admitdatetime<= @ENDDATE and i.inpatientsid>0 ;


/*Join diagnosis tables*/
select a.*, b.OrdinalNumber as inpat_ordinalnumber, c.icd10code as inpat_icd10code, d.icd9code  as inpat_icd9code
into  #VAPD_inpat_dx
from #VAPD_inpat a
left join SRC.Inpat_Inpatientdiagnosis b on a.inpatientsid=b.inpatientsid
left join cdwwork.dim.icd10 c on b.icd10sid=c.icd10sid
left join cdwwork.dim.icd9 d  on b.icd9sid=d.icd9sid;


/*Join Inpatient and Inpatient Specialty Transfer tables (with dx codes), 
get PatientICN and ScrSSN for DOD from SPatient table*/
SELECT distinct s.PatientICN, s.scrssn,  a.*,  b.admitdatetime, b.dischargedatetime, b.sta6a
into #merged_vapd_inpat
FROM #vapd_inpatspec_dx a
left join #vapd_inpat_dx b
on a.inpatientsid=b.inpatientsid and a.patientsid=b.patientsid 
inner JOIN src.SPatient_SPatient  s
ON (a.PatientSID=s.PatientSID);


/*remove null dx codes and get from inpat.inpatientdiagnosis table*/
select *
into  #merged_vapd_inpat2
from #merged_vapd_inpat
where icd9code is not null or icd10code is not null;

select * into #nulldx 
from #merged_vapd_inpat 
where icd9code is null and icd10code is null;

SELECT distinct a.patienticn, a.scrssn, a.InpatientSID, a.PatientSID, a.Sta3n, 
a.SpecialtyTransferDateTime, a.LOSInService, a.specialty,
a.acute, a.flag, a.admitdatetime, a.dischargedatetime, a.sta6a, 
b.inpat_ordinalnumber as ordinalnumber, b.inpat_icd9code as icd9code, 
b.inpat_icd10code as icd10code
into #nulldx2
FROM #nulldx a
left join #vapd_inpat_dx b
on a.inpatientsid=b.inpatientsid and a.patientsid=b.patientsid;


drop table #merged_vapd_inpat;

/*union the two tables. 
This table has all specialty stays with the dx codes from either the spec trans dx table or inpat dx table*/
select patienticn, scrssn, inpatientsid, patientsid, sta3n, specialtytransferdatetime, 
losinservice, specialty, acute, flag, admitdatetime, dischargedatetime, sta6a, 
ordinalnumber, icd9code, icd10code
into #merged_vapd_inpatb
from  #merged_vapd_inpat2
union 
select patienticn, scrssn, inpatientsid, patientsid, sta3n, specialtytransferdatetime, 
losinservice, specialty, acute, flag, admitdatetime, dischargedatetime, sta6a, 
ordinalnumber, icd9code, icd10code
from #nulldx2 ;


/*date of death for all patients in cohort*/
DROP TABLE IF EXISTS #dod;
select distinct a.PatientICN, cast (a.DeathDateTime as date) as DOD
into #dod
from Src.SPatient_SPatient a
where a.DeathDateTime is not null ;


/*add dod*/
select a.*, b.dod 
into #merged_vapd_inpat_SW20210517
from #merged_vapd_inpatb a
left join #dod b
on a.patienticn=b.patienticn;


drop table #vapd_inpat;

select  *
into #vapd_inpatb
from /*dflt.*/#merged_vapd_inpat_SW20210517
where dischargedatetime is not null;


drop table #merged_vapd_inpat_SW20210517;

/*change the final dflt table name to include the dates pulled in within the name. 
make sure to change the table names below as well. Note: “OS” stands for OverSepsis*/
select *
into #OS_cohort_testpull
from #vapd_inpatb;



select * from #OS_cohort_testpull

);
DISCONNECT FROM TUNNEL ;
QUIT ;


/*Upload sepsis.OS_cohort_&todaysdate into CDW*/
data PRE45DFT.OS_cohort_&todaysdate; 
set sepsis.OS_cohort_&todaysdate; 
run;

proc sort data=sepsis.OS_cohort_&todaysdate;
by descending specialtytransferdatetime;
run;
