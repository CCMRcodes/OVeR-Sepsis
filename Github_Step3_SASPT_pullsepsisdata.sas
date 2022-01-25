/***** SAS Pass Thru Code to pull in additional data to define Sepsis Patients ****/
/*Change dates and cohort with each new run:*/
/*SAS Macro of today's date*/
%let todaysdate=%sysfunc(today(), yymmddn8.); %put &todaysdate;
%let DATASRCs=ORD_STUDYNAME;


proc datasets lib=DIM_RB02;
proc datasets lib=PRE45DFT; /*will be different with each study*/
run;


/*	BMCA ABX meds */
libname meds '/data/dart/2021/Data/meds';

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE meds.BCMA_ABX&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 

declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT a.DrugNameWithoutDose, a.LocalDrugNameWithDose,  a.NationalDrugNameWithDose,a.NationalDrug, a.Sta3n, a.LocalDrugSID, a.VAClassification,
a.UnitDoseMedicationRoute
into #localdrugsid
FROM  [CDWWork].[Dim].[LocalDrug] AS A 
WHERE a.LocalDrugNameWithDose like ('%Acyclovir%') or a.LocalDrugNameWithDose like ('%Amikacin%') or a.LocalDrugNameWithDose like ('%Amoxicillin%') or a.LocalDrugNameWithDose like ('%Clavulanate%') or 
a.LocalDrugNameWithDose like ('%Amphotericin B%') or a.LocalDrugNameWithDose like ('%Ampicillin%') or a.LocalDrugNameWithDose like ('%Sulbactam%') or 
a.LocalDrugNameWithDose like ('%Anidulafungin%') or a.LocalDrugNameWithDose like ('%Azithromycin%') or a.LocalDrugNameWithDose like ('%Aztreonam%') or a.LocalDrugNameWithDose like ('%Caspofungin%') or 
a.LocalDrugNameWithDose like ('%Cefaclor%') or a.LocalDrugNameWithDose like ('%Cefadroxil%') or a.LocalDrugNameWithDose like ('%Cefamandole%') or a.LocalDrugNameWithDose like ('%Cefazolin%') or 
a.LocalDrugNameWithDose like ('%Cefdinir%') or a.LocalDrugNameWithDose like ('%Cefditoren%') or a.LocalDrugNameWithDose like ('%Cefepime%') or a.LocalDrugNameWithDose like ('%Cefixime%') or 
a.LocalDrugNameWithDose like ('%Cefmetazole%') or a.LocalDrugNameWithDose like ('%Cefonicid%') or a.LocalDrugNameWithDose like ('%Cefoperazone%') or a.LocalDrugNameWithDose like ('%Cefotaxime%') or 
a.LocalDrugNameWithDose like ('%Cefotetan%') or a.LocalDrugNameWithDose like ('%Cefoxitin%') or a.LocalDrugNameWithDose like ('%Cefpodoxime%') or 
a.LocalDrugNameWithDose like ('%Cefprozil%') or a.LocalDrugNameWithDose like ('%Ceftaroline%') or a.LocalDrugNameWithDose like ('%Ceftazidime%') or 
a.LocalDrugNameWithDose like ('%Avibactam%') or a.LocalDrugNameWithDose like ('%Ceftibuten%') or a.LocalDrugNameWithDose like ('%Ceftizoxime%') or a.LocalDrugNameWithDose like ('%Tazobactam%') or 
a.LocalDrugNameWithDose like ('%Ceftriaxone%') or a.LocalDrugNameWithDose like ('%Cefuroxime%') or a.LocalDrugNameWithDose like ('%Cephalexin%') or a.LocalDrugNameWithDose like ('%Cephalothin%') or a.LocalDrugNameWithDose like ('%Cephapirin%') or 
a.LocalDrugNameWithDose like ('%Cephradine%') or a.LocalDrugNameWithDose like ('%Chloramphenicol%') or a.LocalDrugNameWithDose like ('%Cidofovir%') or a.LocalDrugNameWithDose like ('%Cinoxacin%') or 
a.LocalDrugNameWithDose like ('%Ciprofloxacin%') or a.LocalDrugNameWithDose like ('%Clindamycin%') or a.LocalDrugNameWithDose like ('%Cloxacillin%') or a.LocalDrugNameWithDose like ('%Colistin%') or 
a.LocalDrugNameWithDose like ('%Colistimethate%') or a.LocalDrugNameWithDose like ('%Dalbavancin%') or a.LocalDrugNameWithDose like ('%Daptomycin%') or a.LocalDrugNameWithDose like ('%Dicloxacillin%') or 
a.LocalDrugNameWithDose like ('%Doripenem%') or a.LocalDrugNameWithDose like ('%Doxycycline%') or a.LocalDrugNameWithDose like ('%Ertapenem%') or 
a.LocalDrugNameWithDose like ('%Fidaxomicin%') or a.LocalDrugNameWithDose like ('%Fluconazole%') or a.LocalDrugNameWithDose like ('%Foscarnet%') or a.LocalDrugNameWithDose like ('%Fosfomycin%') or a.LocalDrugNameWithDose like ('%Ganciclovir%') or 
a.LocalDrugNameWithDose like ('%Gatifloxacin%') or a.LocalDrugNameWithDose like ('%Gentamicin%') or a.LocalDrugNameWithDose like ('%Imipenem%') or a.LocalDrugNameWithDose like ('%Itraconazole%') or 
a.LocalDrugNameWithDose like ('%Kanamycin%') or a.LocalDrugNameWithDose like ('%Levofloxacin%') or a.LocalDrugNameWithDose like ('%Lincomycin%') or a.LocalDrugNameWithDose like ('%Linezolid%') or 
a.LocalDrugNameWithDose like ('%Meropenem%') or a.LocalDrugNameWithDose like ('%Methicillin%') or a.LocalDrugNameWithDose like ('%Metronidazole%') or a.LocalDrugNameWithDose like ('%Mezlocillin%') or 
a.LocalDrugNameWithDose like ('%Micafungin%') or a.LocalDrugNameWithDose like ('%Minocycline%') or a.LocalDrugNameWithDose like ('%Moxifloxacin%') or a.LocalDrugNameWithDose like ('%Nafcillin%') or 
a.LocalDrugNameWithDose like ('%Nitrofurantoin%') or a.LocalDrugNameWithDose like ('%Norfloxacin%') or a.LocalDrugNameWithDose like ('%Ofloxacin%') or a.LocalDrugNameWithDose like ('%Oritavancin%') or 
a.LocalDrugNameWithDose like ('%Oxacillin%') or a.LocalDrugNameWithDose like ('%Penicillin%') or a.LocalDrugNameWithDose like ('%Peramivir%') or a.LocalDrugNameWithDose like ('%Piperacillin%') or 
a.LocalDrugNameWithDose like ('%Tazobactam%') or a.LocalDrugNameWithDose like ('%Pivampicillin%') or a.LocalDrugNameWithDose like ('%Polymyxin B%') or a.LocalDrugNameWithDose like ('%Posaconazole%') or 
a.LocalDrugNameWithDose like ('%Quinupristin%') or a.LocalDrugNameWithDose like ('%Dalfopristin%') or a.LocalDrugNameWithDose like ('%Streptomycin%') or a.LocalDrugNameWithDose like ('%Sulfadiazine%') or 
a.LocalDrugNameWithDose like ('%trimethoprim%') or a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or a.LocalDrugNameWithDose like ('%Sulfisoxazole%') or 
a.LocalDrugNameWithDose like ('%Tedizolid%') or a.LocalDrugNameWithDose like ('%Telavancin%') or a.LocalDrugNameWithDose like ('%Telithromycin%') or a.LocalDrugNameWithDose like ('%Tetracycline%') or 
a.LocalDrugNameWithDose like ('%Ticarcillin%') or a.LocalDrugNameWithDose like ('%Clavulanate%') or a.LocalDrugNameWithDose like ('%Tigecycline%') or a.LocalDrugNameWithDose like ('%Tobramycin%') or 
a.LocalDrugNameWithDose like ('%Trimethoprim%') or a.LocalDrugNameWithDose like ('%Sulfamethoxazole%') or a.LocalDrugNameWithDose like ('%Vancomycin%') or a.LocalDrugNameWithDose like ('%Voriconazole%')
 or a.LocalDrugNameWithDose like ('%Oseltamivir%') or a.LocalDrugNameWithDose like ('%Isavuconazonium%') or a.LocalDrugNameWithDose like ('%Clarithromycin%') or a.LocalDrugNameWithDose like ('%Rifampin%')
or
a.drugnamewithoutdose like ('%Acyclovir%') or a.drugnamewithoutdose like ('%Amikacin%') or a.drugnamewithoutdose like ('%Amoxicillin%') or a.drugnamewithoutdose like ('%Clavulanate%') or 
a.drugnamewithoutdose like ('%Amphotericin B%') or a.drugnamewithoutdose like ('%Ampicillin%') or a.drugnamewithoutdose like ('%Sulbactam%') or 
a.drugnamewithoutdose like ('%Anidulafungin%') or a.drugnamewithoutdose like ('%Azithromycin%') or a.drugnamewithoutdose like ('%Aztreonam%') or a.drugnamewithoutdose like ('%Caspofungin%') or 
a.drugnamewithoutdose like ('%Cefaclor%') or a.drugnamewithoutdose like ('%Cefadroxil%') or a.drugnamewithoutdose like ('%Cefamandole%') or a.drugnamewithoutdose like ('%Cefazolin%') or 
a.drugnamewithoutdose like ('%Cefdinir%') or a.drugnamewithoutdose like ('%Cefditoren%') or a.drugnamewithoutdose like ('%Cefepime%') or a.drugnamewithoutdose like ('%Cefixime%') or 
a.drugnamewithoutdose like ('%Cefmetazole%') or a.drugnamewithoutdose like ('%Cefonicid%') or a.drugnamewithoutdose like ('%Cefoperazone%') or a.drugnamewithoutdose like ('%Cefotaxime%') or 
a.drugnamewithoutdose like ('%Cefotetan%') or a.drugnamewithoutdose like ('%Cefoxitin%') or a.drugnamewithoutdose like ('%Cefpodoxime%') or 
a.drugnamewithoutdose like ('%Cefprozil%') or a.drugnamewithoutdose like ('%Ceftaroline%') or a.drugnamewithoutdose like ('%Ceftazidime%') or 
a.drugnamewithoutdose like ('%Avibactam%') or a.drugnamewithoutdose like ('%Ceftibuten%') or a.drugnamewithoutdose like ('%Ceftizoxime%') or a.drugnamewithoutdose like ('%Tazobactam%') or 
a.drugnamewithoutdose like ('%Ceftriaxone%') or a.drugnamewithoutdose like ('%Cefuroxime%') or a.drugnamewithoutdose like ('%Cephalexin%') or a.drugnamewithoutdose like ('%Cephalothin%') or a.drugnamewithoutdose like ('%Cephapirin%') or 
a.drugnamewithoutdose like ('%Cephradine%') or a.drugnamewithoutdose like ('%Chloramphenicol%') or a.drugnamewithoutdose like ('%Cidofovir%') or a.drugnamewithoutdose like ('%Cinoxacin%') or 
a.drugnamewithoutdose like ('%Ciprofloxacin%') or a.drugnamewithoutdose like ('%Clindamycin%') or a.drugnamewithoutdose like ('%Cloxacillin%') or a.drugnamewithoutdose like ('%Colistin%') or 
a.drugnamewithoutdose like ('%Colistimethate%') or a.drugnamewithoutdose like ('%Dalbavancin%') or a.drugnamewithoutdose like ('%Daptomycin%') or a.drugnamewithoutdose like ('%Dicloxacillin%') or 
a.drugnamewithoutdose like ('%Doripenem%') or a.drugnamewithoutdose like ('%Doxycycline%') or a.drugnamewithoutdose like ('%Ertapenem%') or 
a.drugnamewithoutdose like ('%Fidaxomicin%') or a.drugnamewithoutdose like ('%Fluconazole%') or a.drugnamewithoutdose like ('%Foscarnet%') or a.drugnamewithoutdose like ('%Fosfomycin%') or a.drugnamewithoutdose like ('%Ganciclovir%') or 
a.drugnamewithoutdose like ('%Gatifloxacin%') or a.drugnamewithoutdose like ('%Gentamicin%') or a.drugnamewithoutdose like ('%Imipenem%') or a.drugnamewithoutdose like ('%Itraconazole%') or 
a.drugnamewithoutdose like ('%Kanamycin%') or a.drugnamewithoutdose like ('%Levofloxacin%') or a.drugnamewithoutdose like ('%Lincomycin%') or a.drugnamewithoutdose like ('%Linezolid%') or 
a.drugnamewithoutdose like ('%Meropenem%') or a.drugnamewithoutdose like ('%Methicillin%') or a.drugnamewithoutdose like ('%Metronidazole%') or a.drugnamewithoutdose like ('%Mezlocillin%') or 
a.drugnamewithoutdose like ('%Micafungin%') or a.drugnamewithoutdose like ('%Minocycline%') or a.drugnamewithoutdose like ('%Moxifloxacin%') or a.drugnamewithoutdose like ('%Nafcillin%') or 
a.drugnamewithoutdose like ('%Nitrofurantoin%') or a.drugnamewithoutdose like ('%Norfloxacin%') or a.drugnamewithoutdose like ('%Ofloxacin%') or a.drugnamewithoutdose like ('%Oritavancin%') or 
a.drugnamewithoutdose like ('%Oxacillin%') or a.drugnamewithoutdose like ('%Penicillin%') or a.drugnamewithoutdose like ('%Peramivir%') or a.drugnamewithoutdose like ('%Piperacillin%') or 
a.drugnamewithoutdose like ('%Tazobactam%') or a.drugnamewithoutdose like ('%Pivampicillin%') or a.drugnamewithoutdose like ('%Polymyxin B%') or a.drugnamewithoutdose like ('%Posaconazole%') or 
a.drugnamewithoutdose like ('%Quinupristin%') or a.drugnamewithoutdose like ('%Dalfopristin%') or a.drugnamewithoutdose like ('%Streptomycin%') or a.drugnamewithoutdose like ('%Sulfadiazine%') or 
a.drugnamewithoutdose like ('%trimethoprim%') or a.drugnamewithoutdose like ('%Sulfamethoxazole%') or a.drugnamewithoutdose like ('%Sulfisoxazole%') or 
a.drugnamewithoutdose like ('%Tedizolid%') or a.drugnamewithoutdose like ('%Telavancin%') or a.drugnamewithoutdose like ('%Telithromycin%') or a.drugnamewithoutdose like ('%Tetracycline%') or 
a.drugnamewithoutdose like ('%Ticarcillin%') or a.drugnamewithoutdose like ('%Clavulanate%') or a.drugnamewithoutdose like ('%Tigecycline%') or a.drugnamewithoutdose like ('%Tobramycin%') or 
a.drugnamewithoutdose like ('%Trimethoprim%') or a.drugnamewithoutdose like ('%Sulfamethoxazole%') or a.drugnamewithoutdose like ('%Vancomycin%') or a.drugnamewithoutdose like ('%Voriconazole%') 
or a.drugnamewithoutdose like ('%Oseltamivir%') or a.drugnamewithoutdose like ('%Isavuconazonium%') or a.drugnamewithoutdose like ('%Clarithromycin%') or a.drugnamewithoutdose like ('%Rifampin%')
or
a.NationalDrug like ('%Acyclovir%') or a.NationalDrug like ('%Amikacin%') or a.NationalDrug like ('%Amoxicillin%') or a.NationalDrug like ('%Clavulanate%') or 
a.NationalDrug like ('%Amphotericin B%') or a.NationalDrug like ('%Ampicillin%') or a.NationalDrug like ('%Sulbactam%') or 
a.NationalDrug like ('%Anidulafungin%') or a.NationalDrug like ('%Azithromycin%') or a.NationalDrug like ('%Aztreonam%') or a.NationalDrug like ('%Caspofungin%') or 
a.NationalDrug like ('%Cefaclor%') or a.NationalDrug like ('%Cefadroxil%') or a.NationalDrug like ('%Cefamandole%') or a.NationalDrug like ('%Cefazolin%') or 
a.NationalDrug like ('%Cefdinir%') or a.NationalDrug like ('%Cefditoren%') or a.NationalDrug like ('%Cefepime%') or a.NationalDrug like ('%Cefixime%') or 
a.NationalDrug like ('%Cefmetazole%') or a.NationalDrug like ('%Cefonicid%') or a.NationalDrug like ('%Cefoperazone%') or a.NationalDrug like ('%Cefotaxime%') or 
a.NationalDrug like ('%Cefotetan%') or a.NationalDrug like ('%Cefoxitin%') or a.NationalDrug like ('%Cefpodoxime%') or 
a.NationalDrug like ('%Cefprozil%') or a.NationalDrug like ('%Ceftaroline%') or a.NationalDrug like ('%Ceftazidime%') or 
a.NationalDrug like ('%Avibactam%') or a.NationalDrug like ('%Ceftibuten%') or a.NationalDrug like ('%Ceftizoxime%') or a.NationalDrug like ('%Tazobactam%') or 
a.NationalDrug like ('%Ceftriaxone%') or a.NationalDrug like ('%Cefuroxime%') or a.NationalDrug like ('%Cephalexin%') or a.NationalDrug like ('%Cephalothin%') or a.NationalDrug like ('%Cephapirin%') or 
a.NationalDrug like ('%Cephradine%') or a.NationalDrug like ('%Chloramphenicol%') or a.NationalDrug like ('%Cidofovir%') or a.NationalDrug like ('%Cinoxacin%') or 
a.NationalDrug like ('%Ciprofloxacin%') or a.NationalDrug like ('%Clindamycin%') or a.NationalDrug like ('%Cloxacillin%') or a.NationalDrug like ('%Colistin%') or 
a.NationalDrug like ('%Colistimethate%') or a.NationalDrug like ('%Dalbavancin%') or a.NationalDrug like ('%Daptomycin%') or a.NationalDrug like ('%Dicloxacillin%') or 
a.NationalDrug like ('%Doripenem%') or a.NationalDrug like ('%Doxycycline%') or a.NationalDrug like ('%Ertapenem%') or 
a.NationalDrug like ('%Fidaxomicin%') or a.NationalDrug like ('%Fluconazole%') or a.NationalDrug like ('%Foscarnet%') or a.NationalDrug like ('%Fosfomycin%') or a.NationalDrug like ('%Ganciclovir%') or 
a.NationalDrug like ('%Gatifloxacin%') or a.NationalDrug like ('%Gentamicin%') or a.NationalDrug like ('%Imipenem%') or a.NationalDrug like ('%Itraconazole%') or 
a.NationalDrug like ('%Kanamycin%') or a.NationalDrug like ('%Levofloxacin%') or a.NationalDrug like ('%Lincomycin%') or a.NationalDrug like ('%Linezolid%') or 
a.NationalDrug like ('%Meropenem%') or a.NationalDrug like ('%Methicillin%') or a.NationalDrug like ('%Metronidazole%') or a.NationalDrug like ('%Mezlocillin%') or 
a.NationalDrug like ('%Micafungin%') or a.NationalDrug like ('%Minocycline%') or a.NationalDrug like ('%Moxifloxacin%') or a.NationalDrug like ('%Nafcillin%') or 
a.NationalDrug like ('%Nitrofurantoin%') or a.NationalDrug like ('%Norfloxacin%') or a.NationalDrug like ('%Ofloxacin%') or a.NationalDrug like ('%Oritavancin%') or 
a.NationalDrug like ('%Oxacillin%') or a.NationalDrug like ('%Penicillin%') or a.NationalDrug like ('%Peramivir%') or a.NationalDrug like ('%Piperacillin%') or 
a.NationalDrug like ('%Tazobactam%') or a.NationalDrug like ('%Pivampicillin%') or a.NationalDrug like ('%Polymyxin B%') or a.NationalDrug like ('%Posaconazole%') or 
a.NationalDrug like ('%Quinupristin%') or a.NationalDrug like ('%Dalfopristin%') or a.NationalDrug like ('%Streptomycin%') or a.NationalDrug like ('%Sulfadiazine%') or 
a.NationalDrug like ('%trimethoprim%') or a.NationalDrug like ('%Sulfamethoxazole%') or a.NationalDrug like ('%Sulfisoxazole%') or 
a.NationalDrug like ('%Tedizolid%') or a.NationalDrug like ('%Telavancin%') or a.NationalDrug like ('%Telithromycin%') or a.NationalDrug like ('%Tetracycline%') or 
a.NationalDrug like ('%Ticarcillin%') or a.NationalDrug like ('%Clavulanate%') or a.NationalDrug like ('%Tigecycline%') or a.NationalDrug like ('%Tobramycin%') or 
a.NationalDrug like ('%Trimethoprim%') or a.NationalDrug like ('%Sulfamethoxazole%') or a.NationalDrug like ('%Vancomycin%') or a.NationalDrug like ('%Voriconazole%')
or a.NationalDrug like ('%Oseltamivir%') or a.NationalDrug like ('%Isavuconazonium%') or a.NationalDrug like ('%Clarithromycin%') or a.NationalDrug like ('%Rifampin%')
or
a.NationalDrugNameWithDose like ('%Acyclovir%') or a.NationalDrugNameWithDose like ('%Amikacin%') or a.NationalDrugNameWithDose like ('%Amoxicillin%') or a.NationalDrugNameWithDose like ('%Clavulanate%') or 
a.NationalDrugNameWithDose like ('%Amphotericin B%') or a.NationalDrugNameWithDose like ('%Ampicillin%') or a.NationalDrugNameWithDose like ('%Sulbactam%') or 
a.NationalDrugNameWithDose like ('%Anidulafungin%') or a.NationalDrugNameWithDose like ('%Azithromycin%') or a.NationalDrugNameWithDose like ('%Aztreonam%') or a.NationalDrugNameWithDose like ('%Caspofungin%') or 
a.NationalDrugNameWithDose like ('%Cefaclor%') or a.NationalDrugNameWithDose like ('%Cefadroxil%') or a.NationalDrugNameWithDose like ('%Cefamandole%') or a.NationalDrugNameWithDose like ('%Cefazolin%') or 
a.NationalDrugNameWithDose like ('%Cefdinir%') or a.NationalDrugNameWithDose like ('%Cefditoren%') or a.NationalDrugNameWithDose like ('%Cefepime%') or a.NationalDrugNameWithDose like ('%Cefixime%') or 
a.NationalDrugNameWithDose like ('%Cefmetazole%') or a.NationalDrugNameWithDose like ('%Cefonicid%') or a.NationalDrugNameWithDose like ('%Cefoperazone%') or a.NationalDrugNameWithDose like ('%Cefotaxime%') or 
a.NationalDrugNameWithDose like ('%Cefotetan%') or a.NationalDrugNameWithDose like ('%Cefoxitin%') or a.NationalDrugNameWithDose like ('%Cefpodoxime%') or 
a.NationalDrugNameWithDose like ('%Cefprozil%') or a.NationalDrugNameWithDose like ('%Ceftaroline%') or a.NationalDrugNameWithDose like ('%Ceftazidime%') or 
a.NationalDrugNameWithDose like ('%Avibactam%') or a.NationalDrugNameWithDose like ('%Ceftibuten%') or a.NationalDrugNameWithDose like ('%Ceftizoxime%') or a.NationalDrugNameWithDose like ('%Tazobactam%') or 
a.NationalDrugNameWithDose like ('%Ceftriaxone%') or a.NationalDrugNameWithDose like ('%Cefuroxime%') or a.NationalDrugNameWithDose like ('%Cephalexin%') or a.NationalDrugNameWithDose like ('%Cephalothin%') or a.NationalDrugNameWithDose like ('%Cephapirin%') or 
a.NationalDrugNameWithDose like ('%Cephradine%') or a.NationalDrugNameWithDose like ('%Chloramphenicol%') or a.NationalDrugNameWithDose like ('%Cidofovir%') or a.NationalDrugNameWithDose like ('%Cinoxacin%') or 
a.NationalDrugNameWithDose like ('%Ciprofloxacin%') or a.NationalDrugNameWithDose like ('%Clindamycin%') or a.NationalDrugNameWithDose like ('%Cloxacillin%') or a.NationalDrugNameWithDose like ('%Colistin%') or 
a.NationalDrugNameWithDose like ('%Colistimethate%') or a.NationalDrugNameWithDose like ('%Dalbavancin%') or a.NationalDrugNameWithDose like ('%Daptomycin%') or a.NationalDrugNameWithDose like ('%Dicloxacillin%') or 
a.NationalDrugNameWithDose like ('%Doripenem%') or a.NationalDrugNameWithDose like ('%Doxycycline%') or a.NationalDrugNameWithDose like ('%Ertapenem%') or 
a.NationalDrugNameWithDose like ('%Fidaxomicin%') or a.NationalDrugNameWithDose like ('%Fluconazole%') or a.NationalDrugNameWithDose like ('%Foscarnet%') or a.NationalDrugNameWithDose like ('%Fosfomycin%') or a.NationalDrugNameWithDose like ('%Ganciclovir%') or 
a.NationalDrugNameWithDose like ('%Gatifloxacin%') or a.NationalDrugNameWithDose like ('%Gentamicin%') or a.NationalDrugNameWithDose like ('%Imipenem%') or a.NationalDrugNameWithDose like ('%Itraconazole%') or 
a.NationalDrugNameWithDose like ('%Kanamycin%') or a.NationalDrugNameWithDose like ('%Levofloxacin%') or a.NationalDrugNameWithDose like ('%Lincomycin%') or a.NationalDrugNameWithDose like ('%Linezolid%') or 
a.NationalDrugNameWithDose like ('%Meropenem%') or a.NationalDrugNameWithDose like ('%Methicillin%') or a.NationalDrugNameWithDose like ('%Metronidazole%') or a.NationalDrugNameWithDose like ('%Mezlocillin%') or 
a.NationalDrugNameWithDose like ('%Micafungin%') or a.NationalDrugNameWithDose like ('%Minocycline%') or a.NationalDrugNameWithDose like ('%Moxifloxacin%') or a.NationalDrugNameWithDose like ('%Nafcillin%') or 
a.NationalDrugNameWithDose like ('%Nitrofurantoin%') or a.NationalDrugNameWithDose like ('%Norfloxacin%') or a.NationalDrugNameWithDose like ('%Ofloxacin%') or a.NationalDrugNameWithDose like ('%Oritavancin%') or 
a.NationalDrugNameWithDose like ('%Oxacillin%') or a.NationalDrugNameWithDose like ('%Penicillin%') or a.NationalDrugNameWithDose like ('%Peramivir%') or a.NationalDrugNameWithDose like ('%Piperacillin%') or 
a.NationalDrugNameWithDose like ('%Tazobactam%') or a.NationalDrugNameWithDose like ('%Pivampicillin%') or a.NationalDrugNameWithDose like ('%Polymyxin B%') or a.NationalDrugNameWithDose like ('%Posaconazole%') or 
a.NationalDrugNameWithDose like ('%Quinupristin%') or a.NationalDrugNameWithDose like ('%Dalfopristin%') or a.NationalDrugNameWithDose like ('%Streptomycin%') or a.NationalDrugNameWithDose like ('%Sulfadiazine%') or 
a.NationalDrugNameWithDose like ('%trimethoprim%') or a.NationalDrugNameWithDose like ('%Sulfamethoxazole%') or a.NationalDrugNameWithDose like ('%Sulfisoxazole%') or 
a.NationalDrugNameWithDose like ('%Tedizolid%') or a.NationalDrugNameWithDose like ('%Telavancin%') or a.NationalDrugNameWithDose like ('%Telithromycin%') or a.NationalDrugNameWithDose like ('%Tetracycline%') or 
a.NationalDrugNameWithDose like ('%Ticarcillin%') or a.NationalDrugNameWithDose like ('%Clavulanate%') or a.NationalDrugNameWithDose like ('%Tigecycline%') or a.NationalDrugNameWithDose like ('%Tobramycin%') or 
a.NationalDrugNameWithDose like ('%Trimethoprim%') or a.NationalDrugNameWithDose like ('%Sulfamethoxazole%') or a.NationalDrugNameWithDose like ('%Vancomycin%') or a.NationalDrugNameWithDose like ('%Voriconazole%')  
or a.NationalDrugNameWithDose like ('%Oseltamivir%') or a.NationalDrugNameWithDose like ('%Isavuconazonium%') or a.NationalDrugNameWithDose like ('%Clarithromycin%') or a.NationalDrugNameWithDose like ('%Rifampin%')


SELECT a.IVSolutionIngredientSID, a.Sta3n, a.LocalDrugSID, a.Volume, a.IVSolutionFirstIngredientPrintName
into #IVSolutionIngredient
FROM  [CDWWORK].[Dim].[IVSolutionIngredient] AS A 
WHERE a.IVSolutionFirstIngredientPrintName like ('%Acyclovir%') or a.IVSolutionFirstIngredientPrintName like ('%Amikacin%') or a.IVSolutionFirstIngredientPrintName like ('%Amoxicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Clavulanate%') or 
a.IVSolutionFirstIngredientPrintName like ('%Amphotericin B%') or a.IVSolutionFirstIngredientPrintName like ('%Ampicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Sulbactam%') or 
a.IVSolutionFirstIngredientPrintName like ('%Anidulafungin%') or a.IVSolutionFirstIngredientPrintName like ('%Azithromycin%') or a.IVSolutionFirstIngredientPrintName like ('%Aztreonam%') or a.IVSolutionFirstIngredientPrintName like ('%Caspofungin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefaclor%') or a.IVSolutionFirstIngredientPrintName like ('%Cefadroxil%') or a.IVSolutionFirstIngredientPrintName like ('%Cefamandole%') or a.IVSolutionFirstIngredientPrintName like ('%Cefazolin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefdinir%') or a.IVSolutionFirstIngredientPrintName like ('%Cefditoren%') or a.IVSolutionFirstIngredientPrintName like ('%Cefepime%') or a.IVSolutionFirstIngredientPrintName like ('%Cefixime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefmetazole%') or a.IVSolutionFirstIngredientPrintName like ('%Cefonicid%') or a.IVSolutionFirstIngredientPrintName like ('%Cefoperazone%') or a.IVSolutionFirstIngredientPrintName like ('%Cefotaxime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefotetan%') or a.IVSolutionFirstIngredientPrintName like ('%Cefoxitin%') or a.IVSolutionFirstIngredientPrintName like ('%Cefpodoxime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cefprozil%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftaroline%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftazidime%') or 
a.IVSolutionFirstIngredientPrintName like ('%Avibactam%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftibuten%') or a.IVSolutionFirstIngredientPrintName like ('%Ceftizoxime%') or a.IVSolutionFirstIngredientPrintName like ('%Tazobactam%') or 
a.IVSolutionFirstIngredientPrintName like ('%Ceftriaxone%') or a.IVSolutionFirstIngredientPrintName like ('%Cefuroxime%') or a.IVSolutionFirstIngredientPrintName like ('%Cephalexin%') or a.IVSolutionFirstIngredientPrintName like ('%Cephalothin%') or a.IVSolutionFirstIngredientPrintName like ('%Cephapirin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Cephradine%') or a.IVSolutionFirstIngredientPrintName like ('%Chloramphenicol%') or a.IVSolutionFirstIngredientPrintName like ('%Cidofovir%') or a.IVSolutionFirstIngredientPrintName like ('%Cinoxacin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Ciprofloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Clindamycin%') or a.IVSolutionFirstIngredientPrintName like ('%Cloxacillin%') or a.IVSolutionFirstIngredientPrintName like ('%Colistin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Colistimethate%') or a.IVSolutionFirstIngredientPrintName like ('%Dalbavancin%') or a.IVSolutionFirstIngredientPrintName like ('%Daptomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Dicloxacillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Doripenem%') or a.IVSolutionFirstIngredientPrintName like ('%Doxycycline%') or a.IVSolutionFirstIngredientPrintName like ('%Ertapenem%') or 
a.IVSolutionFirstIngredientPrintName like ('%Fidaxomicin%') or a.IVSolutionFirstIngredientPrintName like ('%Fluconazole%') or a.IVSolutionFirstIngredientPrintName like ('%Foscarnet%') or a.IVSolutionFirstIngredientPrintName like ('%Fosfomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Ganciclovir%') or 
a.IVSolutionFirstIngredientPrintName like ('%Gatifloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Gentamicin%') or a.IVSolutionFirstIngredientPrintName like ('%Imipenem%') or a.IVSolutionFirstIngredientPrintName like ('%Itraconazole%') or 
a.IVSolutionFirstIngredientPrintName like ('%Kanamycin%') or a.IVSolutionFirstIngredientPrintName like ('%Levofloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Lincomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Linezolid%') or 
a.IVSolutionFirstIngredientPrintName like ('%Meropenem%') or a.IVSolutionFirstIngredientPrintName like ('%Methicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Metronidazole%') or a.IVSolutionFirstIngredientPrintName like ('%Mezlocillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Micafungin%') or a.IVSolutionFirstIngredientPrintName like ('%Minocycline%') or a.IVSolutionFirstIngredientPrintName like ('%Moxifloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Nafcillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Nitrofurantoin%') or a.IVSolutionFirstIngredientPrintName like ('%Norfloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Ofloxacin%') or a.IVSolutionFirstIngredientPrintName like ('%Oritavancin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Oxacillin%') or a.IVSolutionFirstIngredientPrintName like ('%Penicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Peramivir%') or a.IVSolutionFirstIngredientPrintName like ('%Piperacillin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Tazobactam%') or a.IVSolutionFirstIngredientPrintName like ('%Pivampicillin%') or a.IVSolutionFirstIngredientPrintName like ('%Polymyxin B%') or a.IVSolutionFirstIngredientPrintName like ('%Posaconazole%') or 
a.IVSolutionFirstIngredientPrintName like ('%Quinupristin%') or a.IVSolutionFirstIngredientPrintName like ('%Dalfopristin%') or a.IVSolutionFirstIngredientPrintName like ('%Streptomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfadiazine%') or 
a.IVSolutionFirstIngredientPrintName like ('%trimethoprim%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfamethoxazole%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfisoxazole%') or 
a.IVSolutionFirstIngredientPrintName like ('%Tedizolid%') or a.IVSolutionFirstIngredientPrintName like ('%Telavancin%') or a.IVSolutionFirstIngredientPrintName like ('%Telithromycin%') or a.IVSolutionFirstIngredientPrintName like ('%Tetracycline%') or 
a.IVSolutionFirstIngredientPrintName like ('%Ticarcillin%') or a.IVSolutionFirstIngredientPrintName like ('%Clavulanate%') or a.IVSolutionFirstIngredientPrintName like ('%Tigecycline%') or a.IVSolutionFirstIngredientPrintName like ('%Tobramycin%') or 
a.IVSolutionFirstIngredientPrintName like ('%Trimethoprim%') or a.IVSolutionFirstIngredientPrintName like ('%Sulfamethoxazole%') or a.IVSolutionFirstIngredientPrintName like ('%Vancomycin%') or a.IVSolutionFirstIngredientPrintName like ('%Voriconazole%') 
or a.IVSolutionFirstIngredientPrintName like ('%Oseltamivir%') or a.IVSolutionFirstIngredientPrintName like ('%Isavuconazonium%') or a.IVSolutionFirstIngredientPrintName like ('%Clarithromycin%') or a.IVSolutionFirstIngredientPrintName like ('%Rifampin%');


SELECT a.IVAdditiveIngredientSID, a.Sta3n,  a.LocalDrugSID, a.DrugUnit, a.IVAdditiveIngredientPrintName
into #IVAdditiveIngredient
FROM  [CDWWORK].[Dim].[IVAdditiveIngredient] AS A 
WHERE a.IVAdditiveIngredientPrintName like ('%Acyclovir%') or a.IVAdditiveIngredientPrintName like ('%Amikacin%') or a.IVAdditiveIngredientPrintName like ('%Amoxicillin%') or a.IVAdditiveIngredientPrintName like ('%Clavulanate%') or 
a.IVAdditiveIngredientPrintName like ('%Amphotericin B%') or a.IVAdditiveIngredientPrintName like ('%Ampicillin%') or a.IVAdditiveIngredientPrintName like ('%Sulbactam%') or 
a.IVAdditiveIngredientPrintName like ('%Anidulafungin%') or a.IVAdditiveIngredientPrintName like ('%Azithromycin%') or a.IVAdditiveIngredientPrintName like ('%Aztreonam%') or a.IVAdditiveIngredientPrintName like ('%Caspofungin%') or 
a.IVAdditiveIngredientPrintName like ('%Cefaclor%') or a.IVAdditiveIngredientPrintName like ('%Cefadroxil%') or a.IVAdditiveIngredientPrintName like ('%Cefamandole%') or a.IVAdditiveIngredientPrintName like ('%Cefazolin%') or 
a.IVAdditiveIngredientPrintName like ('%Cefdinir%') or a.IVAdditiveIngredientPrintName like ('%Cefditoren%') or a.IVAdditiveIngredientPrintName like ('%Cefepime%') or a.IVAdditiveIngredientPrintName like ('%Cefixime%') or 
a.IVAdditiveIngredientPrintName like ('%Cefmetazole%') or a.IVAdditiveIngredientPrintName like ('%Cefonicid%') or a.IVAdditiveIngredientPrintName like ('%Cefoperazone%') or a.IVAdditiveIngredientPrintName like ('%Cefotaxime%') or 
a.IVAdditiveIngredientPrintName like ('%Cefotetan%') or a.IVAdditiveIngredientPrintName like ('%Cefoxitin%') or a.IVAdditiveIngredientPrintName like ('%Cefpodoxime%') or 
a.IVAdditiveIngredientPrintName like ('%Cefprozil%') or a.IVAdditiveIngredientPrintName like ('%Ceftaroline%') or a.IVAdditiveIngredientPrintName like ('%Ceftazidime%') or 
a.IVAdditiveIngredientPrintName like ('%Avibactam%') or a.IVAdditiveIngredientPrintName like ('%Ceftibuten%') or a.IVAdditiveIngredientPrintName like ('%Ceftizoxime%') or a.IVAdditiveIngredientPrintName like ('%Tazobactam%') or 
a.IVAdditiveIngredientPrintName like ('%Ceftriaxone%') or a.IVAdditiveIngredientPrintName like ('%Cefuroxime%') or a.IVAdditiveIngredientPrintName like ('%Cephalexin%') or a.IVAdditiveIngredientPrintName like ('%Cephalothin%') or a.IVAdditiveIngredientPrintName like ('%Cephapirin%') or 
a.IVAdditiveIngredientPrintName like ('%Cephradine%') or a.IVAdditiveIngredientPrintName like ('%Chloramphenicol%') or a.IVAdditiveIngredientPrintName like ('%Cidofovir%') or a.IVAdditiveIngredientPrintName like ('%Cinoxacin%') or 
a.IVAdditiveIngredientPrintName like ('%Ciprofloxacin%') or a.IVAdditiveIngredientPrintName like ('%Clindamycin%') or a.IVAdditiveIngredientPrintName like ('%Cloxacillin%') or a.IVAdditiveIngredientPrintName like ('%Colistin%') or 
a.IVAdditiveIngredientPrintName like ('%Colistimethate%') or a.IVAdditiveIngredientPrintName like ('%Dalbavancin%') or a.IVAdditiveIngredientPrintName like ('%Daptomycin%') or a.IVAdditiveIngredientPrintName like ('%Dicloxacillin%') or 
a.IVAdditiveIngredientPrintName like ('%Doripenem%') or a.IVAdditiveIngredientPrintName like ('%Doxycycline%') or a.IVAdditiveIngredientPrintName like ('%Ertapenem%') or 
a.IVAdditiveIngredientPrintName like ('%Fidaxomicin%') or a.IVAdditiveIngredientPrintName like ('%Fluconazole%') or a.IVAdditiveIngredientPrintName like ('%Foscarnet%') or a.IVAdditiveIngredientPrintName like ('%Fosfomycin%') or a.IVAdditiveIngredientPrintName like ('%Ganciclovir%') or 
a.IVAdditiveIngredientPrintName like ('%Gatifloxacin%') or a.IVAdditiveIngredientPrintName like ('%Gentamicin%') or a.IVAdditiveIngredientPrintName like ('%Imipenem%') or a.IVAdditiveIngredientPrintName like ('%Itraconazole%') or 
a.IVAdditiveIngredientPrintName like ('%Kanamycin%') or a.IVAdditiveIngredientPrintName like ('%Levofloxacin%') or a.IVAdditiveIngredientPrintName like ('%Lincomycin%') or a.IVAdditiveIngredientPrintName like ('%Linezolid%') or 
a.IVAdditiveIngredientPrintName like ('%Meropenem%') or a.IVAdditiveIngredientPrintName like ('%Methicillin%') or a.IVAdditiveIngredientPrintName like ('%Metronidazole%') or a.IVAdditiveIngredientPrintName like ('%Mezlocillin%') or 
a.IVAdditiveIngredientPrintName like ('%Micafungin%') or a.IVAdditiveIngredientPrintName like ('%Minocycline%') or a.IVAdditiveIngredientPrintName like ('%Moxifloxacin%') or a.IVAdditiveIngredientPrintName like ('%Nafcillin%') or 
a.IVAdditiveIngredientPrintName like ('%Nitrofurantoin%') or a.IVAdditiveIngredientPrintName like ('%Norfloxacin%') or a.IVAdditiveIngredientPrintName like ('%Ofloxacin%') or a.IVAdditiveIngredientPrintName like ('%Oritavancin%') or 
a.IVAdditiveIngredientPrintName like ('%Oxacillin%') or a.IVAdditiveIngredientPrintName like ('%Penicillin%') or a.IVAdditiveIngredientPrintName like ('%Peramivir%') or a.IVAdditiveIngredientPrintName like ('%Piperacillin%') or 
a.IVAdditiveIngredientPrintName like ('%Tazobactam%') or a.IVAdditiveIngredientPrintName like ('%Pivampicillin%') or a.IVAdditiveIngredientPrintName like ('%Polymyxin B%') or a.IVAdditiveIngredientPrintName like ('%Posaconazole%') or 
a.IVAdditiveIngredientPrintName like ('%Quinupristin%') or a.IVAdditiveIngredientPrintName like ('%Dalfopristin%') or a.IVAdditiveIngredientPrintName like ('%Streptomycin%') or a.IVAdditiveIngredientPrintName like ('%Sulfadiazine%') or 
a.IVAdditiveIngredientPrintName like ('%trimethoprim%') or a.IVAdditiveIngredientPrintName like ('%Sulfamethoxazole%') or a.IVAdditiveIngredientPrintName like ('%Sulfisoxazole%') or 
a.IVAdditiveIngredientPrintName like ('%Tedizolid%') or a.IVAdditiveIngredientPrintName like ('%Telavancin%') or a.IVAdditiveIngredientPrintName like ('%Telithromycin%') or a.IVAdditiveIngredientPrintName like ('%Tetracycline%') or 
a.IVAdditiveIngredientPrintName like ('%Ticarcillin%') or a.IVAdditiveIngredientPrintName like ('%Clavulanate%') or a.IVAdditiveIngredientPrintName like ('%Tigecycline%') or a.IVAdditiveIngredientPrintName like ('%Tobramycin%') or 
a.IVAdditiveIngredientPrintName like ('%Trimethoprim%') or a.IVAdditiveIngredientPrintName like ('%Sulfamethoxazole%') or a.IVAdditiveIngredientPrintName like ('%Vancomycin%') or a.IVAdditiveIngredientPrintName like ('%Voriconazole%') 
 or a.IVAdditiveIngredientPrintName like ('%Oseltamivir%') or a.IVAdditiveIngredientPrintName like ('%Isavuconazonium%') or a.IVAdditiveIngredientPrintName like ('%Clarithromycin%') or a.IVAdditiveIngredientPrintName like ('%Rifampin%');


 /*Pull ABX Dispensed*/
SELECT distinct c.PatientICN, b.Sta3n, b.PatientSID, b.ActionDateTime, b.BCMAMedicationLogSID, b.UnitOfAdministration,  y.LocalDrugNameWithDose, y.LocalDrugSID,
 1  as Dispensed, 0 as Additive, 0 as Solution
into #DispensedDrugs
FROM  dflt.OS_cohort_&todaysdate as coh
inner join [Src].[BCMA_BCMADispensedDrug] as B on coh.PatientSID=b.PatientSID
inner join #localdrugsid y on b.LocalDrugSID=y.LocalDrugSID
left join [Src].[SPatient_SPatient] as c on b.PatientSID=c.PatientSID
where b.ActionDateTime>= @startdate2 and b.ActionDateTime <@currentdate

/*IV Additive*/
SELECT  distinct a.DrugUnit, a.IVAdditiveIngredientPrintName as LocalDrugNameWithDose, a.LocalDrugSID, b.*
into #BCMA_Additive_v1
FROM  #IVAdditiveIngredient  as A
inner join [Src].[BCMA_BCMAAdditive] as B on a.IVAdditiveIngredientSID=b.IVAdditiveIngredientSID
where b.ActionDateTime>= @startdate2 and b.ActionDateTime <@currentdate

/*get patientsid for BCMAAdditive from BCMAMedicationLog*/
SELECT distinct A.*, B.PatientSID, c.PatientICN
into  #BCMA_Additive_v2
FROM  #BCMA_Additive_v1  A
inner JOIN [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID=B.BCMAMedicationLogSID 
left join [Src].[SPatient_SPatient] as c on b.PatientSID=c.PatientSID 

select distinct b.PatientICN, b.Sta3n, b.PatientSID, b.ActionDateTime, b.BCMAMedicationLogSID, 
b.UnitOfAdministration, b.LocalDrugNameWithDose, b.LocalDrugSID, 0 as Dispensed, 1 as Additive, 0 as Solution
into #AdditiveDrugs
from dflt.OS_cohort_&todaysdate as coh
inner join #BCMA_Additive_v2 b on coh.patientsid=b.PatientSID

/*IV Solutions*/
SELECT distinct  a.IVSolutionFirstIngredientPrintName as LocalDrugNameWithDose,  a.LocalDrugSID, b.*
into #BCMA_Solution_v1
FROM  #IVSolutionIngredient  as A
left join [Src].[BCMA_BCMASolution] as B on a.IVSolutionIngredientSID=b.IVSolutionIngredientSID
where b.ActionDateTime>= @StartDate2 and b.ActionDateTime <@currentdate  

/*get patientsid for BCMASolution from BCMAMedicationLog*/
SELECT distinct A.*, B.PatientSID, c.PatientICN
into  #BCMA_Solution_v2
FROM  #BCMA_Solution_v1  A
Inner JOIN [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[SPatient_SPatient] as c on b.PatientSID=c.PatientSID 

select distinct b.PatientICN, b.Sta3n, b.PatientSID,  b.ActionDateTime, b.BCMAMedicationLogSID, b.UnitOfAdministration,  b.LocalDrugNameWithDose, b.LocalDrugSID,
 0 as Dispensed, 0 as Additive, 1 as Solution
into #SolutionDrugs
from dflt.OS_cohort_&todaysdate as coh
inner join #BCMA_Solution_v2 b on coh.patientsid=b.PatientSID

/*Union all 3 tables together*/
select * 
from #DispensedDrugs
union 
Select * 
from #AdditiveDrugs
union 
Select * 
from #SolutionDrugs

);
DISCONNECT FROM TUNNEL ;
QUIT ;

/*******************************************************************************/
/*	BMCA Pressors */
PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE meds.BCMA_Pressor&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 
declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT a.DrugNameWithoutDose, a.LocalDrugNameWithDose,  a.NationalDrugNameWithDose,a.NationalDrug, a.Sta3n, a.LocalDrugSID, a.VAClassification,
a.UnitDoseMedicationRoute
into #localdrugsid
FROM  [CDWWork].[Dim].[LocalDrug] AS A 
WHERE a.LocalDrugNameWithDose like ('%DOPAMINE%') or a.LocalDrugNameWithDose like ('%NOREPINEPHRINE%') or a.LocalDrugNameWithDose like ('%EPINEPHRINE%')
or a.LocalDrugNameWithDose like ('%PHENYLEPHRINE%') or a.LocalDrugNameWithDose like ('%VASOPRESSIN%') 
or
a.drugnamewithoutdose like ('%DOPAMINE%') or a.drugnamewithoutdose like ('%NOREPINEPHRINE%') or a.drugnamewithoutdose like ('%EPINEPHRINE%') or a.drugnamewithoutdose like ('%PHENYLEPHRINE%') or 
a.drugnamewithoutdose like ('%VASOPRESSIN%')
or
a.NationalDrug like ('%DOPAMINE%') or a.NationalDrug like ('%NOREPINEPHRINE%') or a.NationalDrug like ('%EPINEPHRINE%') or a.NationalDrug like ('%PHENYLEPHRINE%') or 
a.NationalDrug like ('%VASOPRESSIN%') 
or
a.NationalDrugNameWithDose like ('%DOPAMINE%') or a.NationalDrugNameWithDose like ('%NOREPINEPHRINE%') or a.NationalDrugNameWithDose like ('%EPINEPHRINE%') 
or a.NationalDrugNameWithDose like ('%PHENYLEPHRINE%') or  a.NationalDrugNameWithDose like ('%VASOPRESSIN%') ;


SELECT a.IVSolutionIngredientSID, a.Sta3n, a.LocalDrugSID, a.Volume, a.IVSolutionFirstIngredientPrintName
into #IVSolutionIngredient
FROM  [CDWWORK].[Dim].[IVSolutionIngredient] AS A 
WHERE
a.IVSolutionFirstIngredientPrintName like ('%DOPAMINE%') or a.IVSolutionFirstIngredientPrintName like ('%NOREPINEPHRINE%') or a.IVSolutionFirstIngredientPrintName like ('%EPINEPHRINE%') or
a.IVSolutionFirstIngredientPrintName like ('%PHENYLEPHRINE%') or  a.IVSolutionFirstIngredientPrintName like ('%VASOPRESSIN%') ;


SELECT a.IVAdditiveIngredientSID, a.Sta3n, a.LocalDrugSID, a.DrugUnit, a.IVAdditiveIngredientPrintName
into #IVAdditiveIngredient
FROM  [CDWWORK].[Dim].[IVAdditiveIngredient] AS A 
WHERE a.IVAdditiveIngredientPrintName like ('%DOPAMINE%') or a.IVAdditiveIngredientPrintName like ('%NOREPINEPHRINE%') or a.IVAdditiveIngredientPrintName like ('%EPINEPHRINE%') or
a.IVAdditiveIngredientPrintName like ('%PHENYLEPHRINE%') or a.IVAdditiveIngredientPrintName like ('%VASOPRESSIN%') ;


/*Pull ABX Dispensed*/
SELECT distinct c.PatientICN, b.Sta3n, b.PatientSID, b.ActionDateTime, b.BCMAMedicationLogSID, 
b.UnitOfAdministration, y.LocalDrugNameWithDose, y.LocalDrugSID, 1  as Dispensed, 0 as Additive, 0 as Solution
into #DispensedDrugs
FROM  dflt.OS_cohort_&todaysdate as coh
inner join [Src].[BCMA_BCMADispensedDrug] as B on coh.PatientSID=b.PatientSID
inner join #localdrugsid y on b.LocalDrugSID=y.LocalDrugSID
left join [Src].[SPatient_SPatient] as c on b.PatientSID=c.PatientSID
where b.ActionDateTime>= @startdate2 and b.ActionDateTime <@currentdate	 

/*IV Additive*/
SELECT  distinct a.DrugUnit, a.IVAdditiveIngredientPrintName as LocalDrugNameWithDose, a.LocalDrugSID, b.*
into #BCMA_Additive_v1
FROM  #IVAdditiveIngredient  as A
inner join [Src].[BCMA_BCMAAdditive] as B on a.IVAdditiveIngredientSID=b.IVAdditiveIngredientSID
where b.ActionDateTime>= @startdate2 and b.ActionDateTime <@currentdate	

/*get patientsid for BCMAAdditive from BCMAMedicationLog*/
SELECT distinct A.*, B.PatientSID, c.PatientICN
into  #BCMA_Additive_v2
FROM   #BCMA_Additive_v1  A
inner JOIN  [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join  [Src].[SPatient_SPatient] as c on b.PatientSID=c.PatientSID  

select distinct b.PatientICN, b.Sta3n, b.PatientSID, b.ActionDateTime, b.BCMAMedicationLogSID, 
b.UnitOfAdministration,  b.LocalDrugNameWithDose, b.LocalDrugSID, 0 as Dispensed, 1 as Additive, 0 as Solution
into #AdditiveDrugs
from dflt.OS_cohort_&todaysdate as coh
inner join #BCMA_Additive_v2 b on coh.patientsid=b.PatientSID



/*IV Solutions*/
SELECT distinct  a.IVSolutionFirstIngredientPrintName as LocalDrugNameWithDose,  a.LocalDrugSID, b.*
into #BCMA_Solution_v1
FROM  #IVSolutionIngredient  as A
left join .[Src].[BCMA_BCMASolution] as B on a.IVSolutionIngredientSID=b.IVSolutionIngredientSID
where b.ActionDateTime>= @startdate2 and b.ActionDateTime <@currentdate	 

/*get patientsid for BCMASolution from BCMAMedicationLog*/
SELECT distinct A.*, B.PatientSID, c.PatientICN
into #BCMA_Solution_v2
FROM #BCMA_Solution_v1  A
Inner JOIN [Src].[BCMA_BCMAMedicationLog]  B ON A.BCMAMedicationLogSID =B.BCMAMedicationLogSID 
left join [Src].[SPatient_SPatient] as c on b.PatientSID=c.PatientSID 

select distinct b.PatientICN, b.Sta3n, b.PatientSID, b.ActionDateTime, b.BCMAMedicationLogSID, 
b.UnitOfAdministration, b.LocalDrugNameWithDose, b.LocalDrugSID, 0 as Dispensed, 0 as Additive, 1 as Solution
into #SolutionDrugs
from dflt.OS_cohort_&todaysdate as coh
inner join #BCMA_Solution_v2 b on coh.patientsid=b.PatientSID

/*Union all 3 tables together*/
select * 
from #DispensedDrugs
union 
Select * 
from #AdditiveDrugs
union 
Select * 
from #SolutionDrugs
);
DISCONNECT FROM TUNNEL ;
QUIT ;

/****************************************************************************************/
/*	EDIS arrival time*/
libname edis '/data/dart/2021/Data/EDIS';

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE edis.Edis_arrivaltime&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 
/*Pull EDIS for OverSepsis Cohort */
declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

select distinct coh.patienticn, coh.PatientSID, coh.Sta3n, b.PatientArrivalDateTime, b.PatientDepartureDateTime, b.EDISLogSID
FROM dflt.OS_cohort_&todaysdate as coh
inner join [Src].[EDIS_EDISLog] b on coh.patientsid=b.patientsid
where b.PatientArrivalDateTime >=@startdate2  and b.PatientArrivalDateTime < @currentdate 

);
DISCONNECT FROM TUNNEL ;
QUIT ;



/****************************************************************************************/
/*	Vitals (temp, pulse, respiration)*/
libname vital '/data/dart/2021/Data/vitals';

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE vital.temperature&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 

declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT distinct coh.patienticn, coh.PatientSID, coh.Sta3n, a.VitalSignTakenDateTime,
cast(a.VitalSignTakenDateTime as date) as Vital_date, a.VitalResult, a.VitalResultNumeric, b.vitaltype
FROM dflt.OS_cohort_&todaysdate as coh
inner join [Src].[Vital_VitalSign] as A ON coh.PatientSID =a.PatientSID
Inner JOIN [CDWWORK].[Dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
WHERE a.VitalSignTakenDateTime >= @startdate2 and a.VitalSignTakenDateTime <@currentdate
and b.vitaltype in ('TEMPERATURE' ,'ZZTEMPERATURE')
);
DISCONNECT FROM TUNNEL ;
QUIT ;


PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE vital.pulse&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 

declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT distinct coh.patienticn, coh.PatientSID, coh.Sta3n, a.VitalSignTakenDateTime,
cast(a.VitalSignTakenDateTime as date) as Vital_date, a.VitalResult, a.VitalResultNumeric, b.vitaltype
FROM dflt.OS_cohort_&todaysdate as coh
inner join [Src].[Vital_VitalSign] as A ON coh.PatientSID =a.PatientSID
Inner JOIN [CDWWORK].[Dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
WHERE a.VitalSignTakenDateTime >= @startdate2 and a.VitalSignTakenDateTime <@currentdate
and b.vitaltype in ('PULSE','ZZPULSE')
);
DISCONNECT FROM TUNNEL ;
QUIT ;

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE vital.RESPIRATION&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 

declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT distinct coh.patienticn, coh.PatientSID, coh.Sta3n, a.VitalSignTakenDateTime,
cast(a.VitalSignTakenDateTime as date) as Vital_date, a.VitalResult, a.VitalResultNumeric, b.vitaltype
FROM dflt.OS_cohort_&todaysdate as coh
inner join [Src].[Vital_VitalSign] as A ON coh.PatientSID =a.PatientSID
Inner JOIN [CDWWORK].[Dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
WHERE a.VitalSignTakenDateTime >= @startdate2 and a.VitalSignTakenDateTime <@currentdate 
and b.vitaltype in ('RESPIRATION','ZZRESPIRATION')
);
DISCONNECT FROM TUNNEL ;
QUIT ;



/****************************************************************************************/
/*	Labs (WBC, Lactate, platelet, Creatinine, bilirubin)*/
libname labs '/data/dart/2021/Data/labs';

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE labs.labs&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL ( 

declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

select b.LOINCSID, b.LOINC
	,case when loinc in ('14631-6','1975-2','42719-5','54363-7','59827-6','59828-4','35194-0','77137-8') then 'Bilirubin' 
		  when loinc in ('2160-0','44784-7','35203-9','14682-9','38483-4','21232-4','59826-8','77140-2') then 'Creatinine'
		  when loinc in ('13056-7','26515-7','26516-5','777-3','778-1','49497-1') then 'Platelets'
		  when loinc in ('59032-3','30242-2','51829-0','30241-4','14118-4','32693-4','2518-9','19239-3','19240-1','2519-7','32132-3','32133-1','2524-7') then 'Lactate'
		  when loinc in ('26464-8','49498-9','6690-2','804-5') then 'WBC' 
		  end as LabGroup
into #Loincs
from cdwwork.dim.loinc b
where
b.LOINC in ('59032-3','30242-2','51829-0','30241-4','14118-4','32693-4','2518-9','19239-3','19240-1','2519-7','32132-3','32133-1','2524-7',
'14631-6','1975-2','42719-5','54363-7','59827-6','59828-4','35194-0','77137-8',
'2160-0','44784-7','35203-9','14682-9','38483-4','21232-4','59826-8','77140-2',
'13056-7','26515-7','26516-5','777-3','778-1','49497-1','26464-8','49498-9','6690-2','804-5')

select e.labchemtestsid, e.labchemtestname
	,case when e.labchemtestname in ('TOT. BILIRUBIN', 'TOTAL BILIRUBIN', 'BILIRUBIN, TOTAL', 'BILIRUBIN,TOTAL', 'BILIRUBIN,TOTAL (V2)', 'BILIRUBIN TOTAL', 
					'TOTAL BILIRUBIN*', 'TOTAL BILIRUBIN*IA', 'TOT.BILIRUBIN', 'T.BILIRUBIN', 'BILIRUBIN TOTAL, SERUM', 'Bilirubin', 'TOTAL BILIRUBIN (FV)', 'TOT BILIRUBIN', 
					'TOT.BILIRUBIN,SERUM', 'TOTAL BILIRUBIN (CX)', 'BILIRUBIN, TOTAL-----', 'T.BILI', 'TOT BILIRUBIN(DCed 2.1.15', 'BILIRUBIN (DC"D 6/17)', 'TBIL(D/C 6/7/17)', 
					'ZTOT. BILIRUBIN', 'BILIRUBIN,TOTAL,Blood', 'BILIRUBIN, TOTAL, SERUM', 'BILI,TOTAL', 'TOTAL BILIRUBIN, PLASMA', 'TOT. BILIRUBIN(DCd 2.17.15)', 'BILI TOTAL', 
					'TOT. BILIRUBIN(BMT)', 'TOTAL BILIRUBIN (FS)', 'TOT. BILIRUBIN(LUF)', 'TBIL', 'TOT. BILIRUBIN(KTY)', 'TOT. BILIRUBIN(TMB)', 'P-TOTAL BILIRUBIN(I)', 
					'TOTAL BILIRUBIN (MV)*INACT(1-1-15)', 'D-TOTAL BILIRUBIN', 'Bilirubin,Total-LC', 'MN BILI, TOTAL', 'PB TOTAL BILIRUBIN', 'W-BILIRUBIN TOTAL', 'FS-TOTAL BILIRUBIN* (V2)', 'REF-Bilirubin, Total', 
					'BR-TOTAL BILI', 'LF-TOTAL BILIRUBIN* (V2/Q)', 'ELD TOTAL BILIRUBIN', 'TOT. BILIRUBIN (QUEST)', 'TOTAL BILIRUBIN---O', 'MMC TOTAL BILIRUBIN', 'BILIRUBIN,TOTAL(LABCORP)', 
					'BILIRUBIN,TOTAL-Q', 'TOTAL BILIRUBIN QUEST', 'BILIRUBIN, TOTAL (LC)', 'ZZBILIRUBIN-LCA (D/C 11/16/17)', 'LRL TOTAL BILIRUBIN', 'Bilirubin, Total LC', 
					'Bilirubin, Total (Quest)', 'TOTAL BILIRUBIN-LC', 'BILIRUBIN, TOTAL (AML)', 'TOTAL BILIRUBIN -', 'TOTAL BILIRUBIN (FIB)', 'TOTAL BILIRUBIN (QUEST),blood', 
					'TOTAL BILI (REF LAB)', 'BILIRUBIN,TOTAL (LC)', 'TOTAL BILIRUBIN (Ref.Lab)', 'SALEM TOTAL BILIRUBIN  (PB)', 'BILIRUBIN TOTAL (TAMC)', 'MH BILIRUBIN TOTAL', 
					'BILIRUBIN,TOTAL (Q)', 'BILIRUBIN, TOTAL-RBL', 'HATT-T.BILI', 'LEG T BILI', 'BILIRUBIN, TOTAL (FIBRO)') 
					then 'Bilirubin' 
		  when e.labchemtestname in ('CREATININE', 'CREATININE,SERUM', 'CREATININE, SERUM', '*CREATININE', 'CREATININE (V2)', 'Creatinine', 'CREATININE*', 
					'CREATININE idms', 'CREATININE----------O', 'CREATININE-e', 'CREATININE2', 'CREATININE*IA', 'CREATININE(DOES NOT INCLUDE EGFR)', 'CREATININE, SERUM OR PLASMA', 
					'CREATININE (DOES NOT CONTAIN eGFR)', 'Creatinine Serum Result', 'CREATININE,SERUM/PLASMA', 'CREATININE*NE', 'CREATININE (SERUM/PLASMA)', 'CREATININE (FV)', 'CREATININE,blood', 
					'CREATININE,PLASMA (in mg/dL)', 'CREATININE (DCed 2.1.15', 'CREATININE (CX)', 'CREAT.', 'CREATININE,DC 1/14/16', 'ZCREATININE', '_CREATININE (OF eGFR PANEL)', 
					'POC CREATININE', 'EGFR-CREATININE', 'CREATININE(D/C 6/7/17)', 'CREATININE-SERUM', 'CREATININE SER -', 'CREATININE (BLOOD)', 'CREATININE (SERUM)', 
					'CREATININE(serum/plasma)', 'I-STAT CREA', 'CREATININE, PLASMA', 'iCREATININE', 'CREATININE, Serum', 'POC-CREATININE', 'CREATININE, BLOOD', 'CREATININE (WB)(R)', 
					'CREATININE (POC)', 'CREATININE(BMT)', 'iCreatinine', 'CREATININE(LUF)', 'CREATININE (FS)', 'iSTAT CREATININE', 'ISTAT CREATININE', 'POCT CREATININE', 
					'CREATININE (BG)', 'I-CREATININE', 'POC CREATININE ISTAT', 'creatinine', 'CREATININE_KTY', 'AT- CREATININE', 'WB CREATININE', 'ATS CREATININE', 'Ancillary Creatinine', 'CREATININE (ISTAT)', 
					'ANCILLARY I-STAT CREATININE', 'Creatinine-iSTAT', 'CREATININE(TMB)', 'I-STAT, CREAT (STL-MA)', 'CREATININE (MV)*INACT(1-1-15)', 'P-CREATININE(I)', 'ANCILLARY CREATININE', 
					'CREATININE, ANC', 'CREATININE-POC', 'CREATININE, ANCILLARY', 'D-CREATININE', 'CREATININE-POC*IC', 'CREAT (iSTAT)', 'PB CREATININE', '*POC-CREATININE', 'MN CREATININE, SERUM', 
					'I-STAT CREATININE', 'I-STAT CREAT', 'CREATININE(FOR CT STUDIES ONLY)', 'CREATININE (SEND OUT ONLY)', 'CREATININE ENZYMATIC', 'CREAT (dialysis)(D/C 6/7/17)', 
					'W-CREATININE', 'POC CREAT', 'CREATININE (eGFR)', 'CREATININE {i-STAT}', 'POC - CREAT', 'iCREAT', 'SERUM CREATININE', '_POC CREAT', 'SERUM CREATININE VALUE', 
					'CREAT-ISTAT', 'DELTA EGFR', 'CREATININE (DIALYSIS ONLY)', 'BR-CREATININE', 'CT CREATININE', 'Creatinine-i', 'POC CREATININE (ISTAT)', '_CREATININE (I-STAT)', 
					'CREATININE, (WOPC)', 'CREATININE POC (BU/BH)', 'CREATININE (ATS)', 'SERUM CREATININE', 'CREATININE  (sendout)', 'ELD CREATININE', 'CREATININE(POC)', 'MMC CREATININE', 
					'PLASMA CREATININE (CrCl)', 'eCREAT', 'CREATININE,SERUM(LABCORP)', 'CREATININE SERUM', 'CREAT (dialysis)', 'CT CONTRAST CREATININE (DC"D 2/15)', 'LRL CREATININE', 
					'CREATININE GFR', 'STAT CREATININE', 'SALEM CREATININE  (PB)', 'I-STAT CREATININE (I-STAT)', 'SEND OUT CREATININE', 'PL CREAT (RAW)', 'HATT-CREATININE', 
					'CREATININE POC', 'CREATININE (Ref.Lab)', 'POC CREATININE (POC)', 'SERUM CREAT.(CL.)', 'MH CREAT, SER, mg/dL', 'LEG CREATININE', '_CREAT (SER OF CLEAR PNL)', 'CREATININE(Q)', 
					'SERUM CREAT (FOR CLEARANCE)', 'Creatinine', 'CREATININE, (S/O)', 'CREATININE-iSTAT', 'i-Creatinine', 'AT-CREATININE', 'CREATININE I-STAT') 
					then 'Creatinine'
		  when e.labchemtestname in ('PLT', 'PLATELET COUNT', 'PLATELETS', 'PLT (V2)', 'PLT*', 'PLATELET', 'PLTS', 'PLT CT', 'Pltct', 'PLATELETS:', 'PLT3', 
					'PLATELET COUNT-------', 'PLATELET (AA)', 'PLT (FV)', 'PLT(D/C 5/25/17)', 'PLATELET CT', 'PLATELET COUNT  -', 'PLATELET~disc 10/14', 'PLT (XN2000)', 
					'PLT (COUNT)', 'PLT COUNT', 'PLT-AUTO', 'PLT(BMT)', 'PLT(LUFKIN)', 'PLATELET (TOPC)', 'PLT (FS)', 'PLT(s)', 'PLT(KTY)', 'PLT(TMB)', 'PLT (MV)*INACT(1-1-15)', 
					'P-PLATELET COUNT', 'PLT (ESTM)', 'D-Platelets', 'MN PLT', 'MANUAL PLATELETS', 'PB PLT', 'W-PLATELETS', 'plt, lca', 'PLT (HR)', 'PLTCOUNT-COAG PANEL-O', 
					'PLT-PIERRE', 'BR-PLT', 'PLT COUNT ESTIMATE', 'PLT (CD4/CD8)', 'PLATELET (BLUE TOP)', 'PLATELET ONLY(auto)', 'OR PLATELET', 'PLT-MMC', 'PLATELET IN CITRATE ANTICOAGULANT', 
					'PLATELET CNT (BLUE TOP)', 'LRL PLATELET', 'PLATELET COUNT FOR PLT CLUMPS', 'PLT, BLUE TOP*', 'PLATELETS (LABCORP)', 'CIT PLATELET', 'ELD PLT', 
					'PLATELETS (096925)', 'PLATELETS', 'CITRATED PLT', 'PLTS (LC) -', 'PLATELET (CITRATED)', 'PLATELET COUNT (CITRATE)', 'PLATELET COUNT-BLUE TOP', 'PLT (LABCORP)', 
					'SALEM PLATELETS-PB', 'PLT-BTT', '*PLT COUNT', 'HATT-PLT', '(FFTH) PLT', 'Sp.Pl.(Blue)', 'CITRATE PLATELET COUNT', '(STRONG) PLT', 'CITRATED PLATELET COUNT', 
					'PLT-ACL', '_PLT (UW)', '_PLT-BTT (LOW PLT ONLY)', 'MH PLATELET COUNT', 'LEG PLT', 'PLATELET BLUE', 'PLT (CDH)', 'PLATELETS(LABCORP)', 'TAMC PLT', 
					'PLT (NMMC)')
					then 'Platelets'
		  when e.labchemtestname in ('_POC ABG LA', 'AT- LACTATE', 'ATS LACTIC ACID', 'BG LACTATE', 'CVICU-LACTIC ACID', 'GAS-LACTATE', 'GAS-LACTATE(SICU)', 
					'GEM-Lactate', 'iLACTATE', 'ISTAT LACTATE', 'iSTAT LACTATE', 'I-STAT LACTATE', 'LACT ACID (.5-2.2mmol/L)-DON"T USE', 'LACTATE', 'Lactate', 'LACTATE  (MA)', 
					'LACTATE - ARTERIAL {i-STAT}', 'Lactate (ABG)', 'LACTATE (ART)', 'LACTATE (BLOOD GAS)', 'Lactate (Gas)', 'Lactate (Gas)(Pre-3/29/18)', 'LACTATE (ISTAT)', 'LACTATE (LAB)', 
					'LACTATE (OR)', 'LACTATE (POC)', 'LACTATE (VEN)', 'LACTATE (VENOUS BLOOD)', 'Lactate ABL', 'LACTATE BLOOD(POC STL)', 'LACTATE(...03/2009)*CI', 'LACTATE,ARTERIAL BLOOD', 
					'Lactate..', 'LACTATE---BLOOD/FLUID', 'Lactate-iStat', 'LACTATE-P(BLOOD GAS)*', 'LACTATE-POC', 'LACTATE-SALISBURY ABG', 'LACTATE-WB', 'LACTIC ACD', 'LACTIC ACID', 
					'LACTIC ACID  (STL-PB)', 'LACTIC ACID (ARTERIAL)', 'LACTIC ACID (B) DC"d 9/4/7', 'LACTIC ACID (B-GAS)', 'LACTIC ACID (BU/SY)', 'LACTIC ACID (CN/AL/BH)', 
					'LACTIC ACID (D/C 8/1/13)', 'LACTIC ACID (dc"d 6-10-09)', 'LACTIC ACID (DCed 2.1.15', 'LACTIC ACID (DCT 3/2015)', 'LACTIC ACID (FV)', 'LACTIC ACID (FVAMC only)', 
					'LACTIC ACID (IN HOUSE)', 'LACTIC ACID (LABCORP)PRIOR TO 12/08', 'LACTIC ACID (mmol/L)', 'LACTIC ACID (NEW)', 'LACTIC ACID (OLD)', 'LACTIC ACID (PLASMA)', 
					'LACTIC ACID (plasma)', 'LACTIC ACID (PLASMA)~disc 8/13', 'LACTIC ACID (Q)(For CBOC use only)', 'LACTIC ACID (QUEST)', 'LACTIC ACID (QUEST)(dc"d)', 'LACTIC ACID (Sanford)365', 
					'LACTIC ACID (UNSPECIFIED)', 'LACTIC ACID (VENOUS)', 'LACTIC ACID (WR)(dc"d 9/30/11)', 'LACTIC ACID Dc"d 11/8/10', 'LACTIC ACID II DC"D', 'LACTIC ACID SPL', 
					'LACTIC ACID(..4/17)*IC', 'LACTIC ACID(1/DAY)', 'LACTIC ACID(DCd 2.17.15)', 'LACTIC ACID(POST 6/4/97)', 'LACTIC ACID(PRIOR TO 11/5/15)', 'LACTIC ACID(Roseburg)', 
					'LACTIC ACID*', 'LACTIC ACID* THRU 3/31/18', 'LACTIC ACID*IA', 'LACTIC ACID*NE', 'LACTIC ACID, FLUID', 'LACTIC ACID, PLASMA', 'LACTIC ACID, ROUTINE', 'LACTIC ACID, STAT', 
					'LACTIC ACID,BBC', 'LACTIC ACID,BLOOD', 'LACTIC ACID,CSF', 'LACTIC ACID,PLASMA', 'LACTIC ACID,PLASMA(QUEST)', 'LACTIC ACID.', 'Lactic-Gas', 'LC LACTIC ACID', 'MMC LACTIC ACID', 
					'POC LACTATE', 'POC LACTIC ACID', 'POC-LACTATE(PRE 9/15/16)', 'POC-LACTIC ACID', 'STAT LACTIC ACID', 'zLACTIC ACID (DC 1-12)', 'zLACTIC ACID (NA,KX)', 'ZZ LACTIC ACID (DCT:031111)', 
					'ZZLACTATE I-STAT (MA)', 'ZZLACTIC ACID (SY)(<1/21/07)', 'ZZLACTIC ACID DC 11-29-2011', 'ZZ-LACTIC ACID-QUEST', 'ZZZLactate.', 'ZZZLACTATE-P(BLOOD GAS)')
					then 'Lactate'
			when e.labchemtestname in ('WBC','WBC (REFERENCE LAB)','ZWBC (RETIRED 6/29/05)','.WBC (MINOT AFB)DC 6/8/10', 'AUTO WBC','CBC/WBC EX','COOK WBC','CORRECTED WBC',
					'CORRECTED WBC-------0','C-WBC-CBOC','HOPC-WBC','LEG WBC','MH WBC (WET PREP)','MN WBC','NAVY STAT WBC','NEW WBC','OB-WBC','Q-WBC DC"D','T-CELL WBC','TOTAL WBC',
					'Total WBC','Total WBC Count (AML)','Wbc','WBC------------------','WBC  -','WBC - SCAN Dc"d 1-21-08','WBC (AA)','WBC (AUTOMATED)','WBC (AUTOMATED) WR','WBC (BEFORE 5/9/06)',
					'WBC (DO NOT USE)','WBC (FCM)','WBC (FOR ANC CALC.)','WBC (FV)','WBC (LABCORP)','WBC (MV)','WBC (ORS)','WBC (REFERENCE LAB)','WBC (RESEARCH PANEL) (TO 6/13/05)',
					'WBC (thru 10/6/09)','WBC (V2)','WBC {Reference Lab}','WBC {St. George}','WBC AUTO','WBC AUTO  -','WBC COUNT','WBC COUNT (K/uL)','WBC Dc"d 1-21-08','WBC SCAN Dc"D 4-9-09',
					'WBC(CBOC)','WBC(EST)','WBC(PRE-2/2/12)','WBC*','WBC/uL','WBC2','WBC-auto (V1FC)','WBC--------------CSFO','WBC-FL','WBC"S','Z++WBC-OUTSIDE LAB','ZHS WBC','ZSJWBC(DC"D 5-10)',
					'ZWBC (RETIRED 6/29/05)','zzz WBC(BRAD)','WBC (for CD4/CD8)','z*INACT*WBC (4-1-10)','ZSJUAWBC(DC"D 5-10)','WHITE BLOOD CELLS Thru 2/12/07','TOTAL WHITE BLOOD COUNT','WHITE CELL COUNT','WHITE CELLS, TOTAL') 
					then 'WBC'
			end as LabGroup
into #Labs
from cdwwork.dim.LabChemTest e
where e.LabChemTestName in ('_POC ABG LA', 'AT- LACTATE', 'ATS LACTIC ACID', 'BG LACTATE', 'CVICU-LACTIC ACID', 'GAS-LACTATE', 'GAS-LACTATE(SICU)', 
'GEM-Lactate', 'iLACTATE', 'ISTAT LACTATE', 'iSTAT LACTATE', 'I-STAT LACTATE', 'LACT ACID (.5-2.2mmol/L)-DON"T USE', 'LACTATE', 'Lactate', 'LACTATE  (MA)', 
'LACTATE - ARTERIAL {i-STAT}', 'Lactate (ABG)', 'LACTATE (ART)', 'LACTATE (BLOOD GAS)', 'Lactate (Gas)', 'Lactate (Gas)(Pre-3/29/18)', 'LACTATE (ISTAT)', 'LACTATE (LAB)', 
'LACTATE (OR)', 'LACTATE (POC)', 'LACTATE (VEN)', 'LACTATE (VENOUS BLOOD)', 'Lactate ABL', 'LACTATE BLOOD(POC STL)', 'LACTATE(...03/2009)*CI', 'LACTATE,ARTERIAL BLOOD', 
'Lactate..', 'LACTATE---BLOOD/FLUID', 'Lactate-iStat', 'LACTATE-P(BLOOD GAS)*', 'LACTATE-POC', 'LACTATE-SALISBURY ABG', 'LACTATE-WB', 'LACTIC ACD', 'LACTIC ACID', 
'LACTIC ACID  (STL-PB)', 'LACTIC ACID (ARTERIAL)', 'LACTIC ACID (B) DC"d 9/4/7', 'LACTIC ACID (B-GAS)', 'LACTIC ACID (BU/SY)', 'LACTIC ACID (CN/AL/BH)', 
'LACTIC ACID (D/C 8/1/13)', 'LACTIC ACID (dc"d 6-10-09)', 'LACTIC ACID (DCed 2.1.15', 'LACTIC ACID (DCT 3/2015)', 'LACTIC ACID (FV)', 'LACTIC ACID (FVAMC only)', 
'LACTIC ACID (IN HOUSE)', 'LACTIC ACID (LABCORP)PRIOR TO 12/08', 'LACTIC ACID (mmol/L)', 'LACTIC ACID (NEW)', 'LACTIC ACID (OLD)', 'LACTIC ACID (PLASMA)', 
'LACTIC ACID (plasma)', 'LACTIC ACID (PLASMA)~disc 8/13', 'LACTIC ACID (Q)(For CBOC use only)', 'LACTIC ACID (QUEST)', 'LACTIC ACID (QUEST)(dc"d)', 'LACTIC ACID (Sanford)365', 
'LACTIC ACID (UNSPECIFIED)', 'LACTIC ACID (VENOUS)', 'LACTIC ACID (WR)(dc"d 9/30/11)', 'LACTIC ACID Dc"d 11/8/10', 'LACTIC ACID II DC"D', 'LACTIC ACID SPL', 
'LACTIC ACID(..4/17)*IC', 'LACTIC ACID(1/DAY)', 'LACTIC ACID(DCd 2.17.15)', 'LACTIC ACID(POST 6/4/97)', 'LACTIC ACID(PRIOR TO 11/5/15)', 'LACTIC ACID(Roseburg)', 
'LACTIC ACID*', 'LACTIC ACID* THRU 3/31/18', 'LACTIC ACID*IA', 'LACTIC ACID*NE', 'LACTIC ACID, FLUID', 'LACTIC ACID, PLASMA', 'LACTIC ACID, ROUTINE', 'LACTIC ACID, STAT', 
'LACTIC ACID,BBC', 'LACTIC ACID,BLOOD', 'LACTIC ACID,CSF', 'LACTIC ACID,PLASMA', 'LACTIC ACID,PLASMA(QUEST)', 'LACTIC ACID.', 'Lactic-Gas', 'LC LACTIC ACID', 'MMC LACTIC ACID', 
'POC LACTATE', 'POC LACTIC ACID', 'POC-LACTATE(PRE 9/15/16)', 'POC-LACTIC ACID', 'STAT LACTIC ACID', 'zLACTIC ACID (DC 1-12)', 'zLACTIC ACID (NA,KX)', 'ZZ LACTIC ACID (DCT:031111)', 
'ZZLACTATE I-STAT (MA)', 'ZZLACTIC ACID (SY)(<1/21/07)', 'ZZLACTIC ACID DC 11-29-2011', 'ZZ-LACTIC ACID-QUEST', 'ZZZLactate.', 'ZZZLACTATE-P(BLOOD GAS)',
'TOT. BILIRUBIN', 'TOTAL BILIRUBIN', 'BILIRUBIN, TOTAL', 'BILIRUBIN,TOTAL', 'BILIRUBIN,TOTAL (V2)', 'BILIRUBIN TOTAL', 
'TOTAL BILIRUBIN*', 'TOTAL BILIRUBIN*IA', 'TOT.BILIRUBIN', 'T.BILIRUBIN', 'BILIRUBIN TOTAL, SERUM', 'Bilirubin', 'TOTAL BILIRUBIN (FV)', 'TOT BILIRUBIN', 
'TOT.BILIRUBIN,SERUM', 'TOTAL BILIRUBIN (CX)', 'BILIRUBIN, TOTAL-----', 'T.BILI', 'TOT BILIRUBIN(DCed 2.1.15', 'BILIRUBIN (DC"D 6/17)', 'TBIL(D/C 6/7/17)', 
'ZTOT. BILIRUBIN', 'BILIRUBIN,TOTAL,Blood', 'BILIRUBIN, TOTAL, SERUM', 'BILI,TOTAL', 'TOTAL BILIRUBIN, PLASMA', 'TOT. BILIRUBIN(DCd 2.17.15)', 'BILI TOTAL', 
'TOT. BILIRUBIN(BMT)', 'TOTAL BILIRUBIN (FS)', 'TOT. BILIRUBIN(LUF)', 'TBIL', 'TOT. BILIRUBIN(KTY)', 'TOT. BILIRUBIN(TMB)', 'P-TOTAL BILIRUBIN(I)', 
'TOTAL BILIRUBIN (MV)*INACT(1-1-15)', 'D-TOTAL BILIRUBIN', 'Bilirubin,Total-LC', 'MN BILI, TOTAL', 'PB TOTAL BILIRUBIN', 'W-BILIRUBIN TOTAL', 'FS-TOTAL BILIRUBIN* (V2)', 'REF-Bilirubin, Total', 
'BR-TOTAL BILI', 'LF-TOTAL BILIRUBIN* (V2/Q)', 'ELD TOTAL BILIRUBIN', 'TOT. BILIRUBIN (QUEST)', 'TOTAL BILIRUBIN---O', 'MMC TOTAL BILIRUBIN', 'BILIRUBIN,TOTAL(LABCORP)', 
'BILIRUBIN,TOTAL-Q', 'TOTAL BILIRUBIN QUEST', 'BILIRUBIN, TOTAL (LC)', 'ZZBILIRUBIN-LCA (D/C 11/16/17)', 'LRL TOTAL BILIRUBIN', 'Bilirubin, Total LC', 
'Bilirubin, Total (Quest)', 'TOTAL BILIRUBIN-LC', 'BILIRUBIN, TOTAL (AML)', 'TOTAL BILIRUBIN -', 'TOTAL BILIRUBIN (FIB)', 'TOTAL BILIRUBIN (QUEST),blood', 
'TOTAL BILI (REF LAB)', 'BILIRUBIN,TOTAL (LC)', 'TOTAL BILIRUBIN (Ref.Lab)', 'SALEM TOTAL BILIRUBIN  (PB)', 'BILIRUBIN TOTAL (TAMC)', 'MH BILIRUBIN TOTAL', 
'BILIRUBIN,TOTAL (Q)', 'BILIRUBIN, TOTAL-RBL', 'HATT-T.BILI', 'LEG T BILI', 'BILIRUBIN, TOTAL (FIBRO)',
'CREATININE', 'CREATININE,SERUM', 'CREATININE, SERUM', '*CREATININE', 'CREATININE (V2)', 'Creatinine', 'CREATININE*', 
'CREATININE idms', 'CREATININE----------O', 'CREATININE-e', 'CREATININE2', 'CREATININE*IA', 'CREATININE(DOES NOT INCLUDE EGFR)', 'CREATININE, SERUM OR PLASMA', 
'CREATININE (DOES NOT CONTAIN eGFR)', 'Creatinine Serum Result', 'CREATININE,SERUM/PLASMA', 'CREATININE*NE', 'CREATININE (SERUM/PLASMA)', 'CREATININE (FV)', 'CREATININE,blood', 
'CREATININE,PLASMA (in mg/dL)', 'CREATININE (DCed 2.1.15', 'CREATININE (CX)', 'CREAT.', 'CREATININE,DC 1/14/16', 'ZCREATININE', '_CREATININE (OF eGFR PANEL)', 
'POC CREATININE', 'EGFR-CREATININE', 'CREATININE(D/C 6/7/17)', 'CREATININE-SERUM', 'CREATININE SER -', 'CREATININE (BLOOD)', 'CREATININE (SERUM)', 
'CREATININE(serum/plasma)', 'I-STAT CREA', 'CREATININE, PLASMA', 'iCREATININE', 'CREATININE, Serum', 'POC-CREATININE', 'CREATININE, BLOOD', 'CREATININE (WB)(R)', 
'CREATININE (POC)', 'CREATININE(BMT)', 'iCreatinine', 'CREATININE(LUF)', 'CREATININE (FS)', 'iSTAT CREATININE', 'ISTAT CREATININE', 'POCT CREATININE', 
'CREATININE (BG)', 'I-CREATININE', 'POC CREATININE ISTAT', 'creatinine', 'CREATININE_KTY', 'AT- CREATININE', 'WB CREATININE', 'ATS CREATININE', 'Ancillary Creatinine', 'CREATININE (ISTAT)', 
'ANCILLARY I-STAT CREATININE', 'Creatinine-iSTAT', 'CREATININE(TMB)', 'I-STAT, CREAT (STL-MA)', 'CREATININE (MV)*INACT(1-1-15)', 'P-CREATININE(I)', 'ANCILLARY CREATININE', 
'CREATININE, ANC', 'CREATININE-POC', 'CREATININE, ANCILLARY', 'D-CREATININE', 'CREATININE-POC*IC', 'CREAT (iSTAT)', 'PB CREATININE', '*POC-CREATININE', 'MN CREATININE, SERUM', 
'I-STAT CREATININE', 'I-STAT CREAT', 'CREATININE(FOR CT STUDIES ONLY)', 'CREATININE (SEND OUT ONLY)', 'CREATININE ENZYMATIC', 'CREAT (dialysis)(D/C 6/7/17)', 
'W-CREATININE', 'POC CREAT', 'CREATININE (eGFR)', 'CREATININE {i-STAT}', 'POC - CREAT', 'iCREAT', 'SERUM CREATININE', '_POC CREAT', 'SERUM CREATININE VALUE', 
'CREAT-ISTAT', 'DELTA EGFR', 'CREATININE (DIALYSIS ONLY)', 'BR-CREATININE', 'CT CREATININE', 'Creatinine-i', 'POC CREATININE (ISTAT)', '_CREATININE (I-STAT)', 
'CREATININE, (WOPC)', 'CREATININE POC (BU/BH)', 'CREATININE (ATS)', 'SERUM CREATININE', 'CREATININE  (sendout)', 'ELD CREATININE', 'CREATININE(POC)', 'MMC CREATININE', 
'PLASMA CREATININE (CrCl)', 'eCREAT', 'CREATININE,SERUM(LABCORP)', 'CREATININE SERUM', 'CREAT (dialysis)', 'CT CONTRAST CREATININE (DC"D 2/15)', 'LRL CREATININE', 
'CREATININE GFR', 'STAT CREATININE', 'SALEM CREATININE  (PB)', 'I-STAT CREATININE (I-STAT)', 'SEND OUT CREATININE', 'PL CREAT (RAW)', 'HATT-CREATININE', 
'CREATININE POC', 'CREATININE (Ref.Lab)', 'POC CREATININE (POC)', 'SERUM CREAT.(CL.)', 'MH CREAT, SER, mg/dL', 'LEG CREATININE', '_CREAT (SER OF CLEAR PNL)', 'CREATININE(Q)', 
'SERUM CREAT (FOR CLEARANCE)', 'Creatinine', 'CREATININE, (S/O)', 'CREATININE-iSTAT', 'i-Creatinine', 'AT-CREATININE', 'CREATININE I-STAT',
'PLT', 'PLATELET COUNT', 'PLATELETS', 'PLT (V2)', 'PLT*', 'PLATELET', 'PLTS', 'PLT CT', 'Pltct', 'PLATELETS:', 'PLT3', 
'PLATELET COUNT-------', 'PLATELET (AA)', 'PLT (FV)', 'PLT(D/C 5/25/17)', 'PLATELET CT', 'PLATELET COUNT  -', 'PLATELET~disc 10/14', 'PLT (XN2000)', 
'PLT (COUNT)', 'PLT COUNT', 'PLT-AUTO', 'PLT(BMT)', 'PLT(LUFKIN)', 'PLATELET (TOPC)', 'PLT (FS)', 'PLT(s)', 'PLT(KTY)', 'PLT(TMB)', 'PLT (MV)*INACT(1-1-15)', 
'P-PLATELET COUNT', 'PLT (ESTM)', 'D-Platelets', 'MN PLT', 'MANUAL PLATELETS', 'PB PLT', 'W-PLATELETS', 'plt, lca', 'PLT (HR)', 'PLTCOUNT-COAG PANEL-O', 
'PLT-PIERRE', 'BR-PLT', 'PLT COUNT ESTIMATE', 'PLT (CD4/CD8)', 'PLATELET (BLUE TOP)', 'PLATELET ONLY(auto)', 'OR PLATELET', 'PLT-MMC', 'PLATELET IN CITRATE ANTICOAGULANT', 
'PLATELET CNT (BLUE TOP)', 'LRL PLATELET', 'PLATELET COUNT FOR PLT CLUMPS', 'PLT, BLUE TOP*', 'PLATELETS (LABCORP)', 'CIT PLATELET', 'ELD PLT', 
'PLATELETS (096925)', 'PLATELETS', 'CITRATED PLT', 'PLTS (LC) -', 'PLATELET (CITRATED)', 'PLATELET COUNT (CITRATE)', 'PLATELET COUNT-BLUE TOP', 'PLT (LABCORP)', 
'SALEM PLATELETS-PB', 'PLT-BTT', '*PLT COUNT', 'HATT-PLT', '(FFTH) PLT', 'Sp.Pl.(Blue)', 'CITRATE PLATELET COUNT', '(STRONG) PLT', 'CITRATED PLATELET COUNT', 
'PLT-ACL', '_PLT (UW)', '_PLT-BTT (LOW PLT ONLY)', 'MH PLATELET COUNT', 'LEG PLT', 'PLATELET BLUE', 'PLT (CDH)', 'PLATELETS(LABCORP)', 'TAMC PLT', 
'PLT (NMMC)','WBC','WBC (REFERENCE LAB)','ZWBC (RETIRED 6/29/05)','.WBC (MINOT AFB)DC 6/8/10', 'AUTO WBC','CBC/WBC EX','COOK WBC','CORRECTED WBC',
'CORRECTED WBC-------0','C-WBC-CBOC','HOPC-WBC','LEG WBC','MH WBC (WET PREP)','MN WBC','NAVY STAT WBC','NEW WBC','OB-WBC','Q-WBC DC"D','T-CELL WBC','TOTAL WBC',
'Total WBC','Total WBC Count (AML)','Wbc','WBC------------------','WBC  -','WBC - SCAN Dc"d 1-21-08','WBC (AA)','WBC (AUTOMATED)','WBC (AUTOMATED) WR','WBC (BEFORE 5/9/06)',
'WBC (DO NOT USE)','WBC (FCM)','WBC (FOR ANC CALC.)','WBC (FV)','WBC (LABCORP)','WBC (MV)','WBC (ORS)','WBC (REFERENCE LAB)','WBC (RESEARCH PANEL) (TO 6/13/05)',
'WBC (thru 10/6/09)','WBC (V2)','WBC {Reference Lab}','WBC {St. George}','WBC AUTO','WBC AUTO  -','WBC COUNT','WBC COUNT (K/uL)','WBC Dc"d 1-21-08','WBC SCAN Dc"D 4-9-09',
'WBC(CBOC)','WBC(EST)','WBC(PRE-2/2/12)','WBC*','WBC/uL','WBC2','WBC-auto (V1FC)','WBC--------------CSFO','WBC-FL','WBC"S','Z++WBC-OUTSIDE LAB','ZHS WBC','ZSJWBC(DC"D 5-10)',
'ZWBC (RETIRED 6/29/05)','zzz WBC(BRAD)','WBC (for CD4/CD8)','z*INACT*WBC (4-1-10)','ZSJUAWBC(DC"D 5-10)','WHITE BLOOD CELLS Thru 2/12/07','TOTAL WHITE BLOOD COUNT','WHITE CELL COUNT','WHITE CELLS, TOTAL')



select distinct  coh.PatientICN,  coh.PatientSID, coh.Sta3n, a.Loincsid, 
       a.LabChemTestSID,  a.TopographySID, d.topography, a.LabChemSpecimenDateTime, a.Units,
	    a.LabChemResultValue, a.LabChemResultNumericValue, b.LabGroup
from dflt.OS_cohort_&todaysdate as coh
INNER JOIN src.Chem_PatientLabChem AS A  on coh.PatientSID=a.PatientSID
INNER JOIN #loincs AS b ON  a.Loincsid=b.Loincsid 
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
WHERE a.LabChemSpecimenDateTime >= @startdate2 AND a.LabChemSpecimenDateTime < @currentdate 


UNION /*union the labs and loince tables together, this step also removes duplicates*/

select distinct  coh.PatientICN,  coh.PatientSID, coh.Sta3n,  a.Loincsid,
       a.LabChemTestSID,   a.TopographySID, d.topography, a.LabChemSpecimenDateTime, a.Units,
	    a.LabChemResultValue, a.LabChemResultNumericValue, c.LabGroup
FROM  dflt.OS_cohort_&todaysdate as coh
INNER JOIN src.Chem_PatientLabChem AS A  on coh.PatientSID=a.PatientSID
INNER JOIN #labs AS c ON  a.LabChemTestSID=c.LabChemTestSID
LEFT JOIN [CDWWork].[Dim].[topography] AS d ON A.TopographySID =D.TopographySID
WHERE a.LabChemSpecimenDateTime >= @startdate2 AND a.LabChemSpecimenDateTime < @currentdate

);
DISCONNECT FROM TUNNEL ;
QUIT ;

/****************************************************************************************/
/*	Mechanical Ventilation (procedure codes)*/
libname proc '/data/dart/2021/Data/procedures';

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE proc.proc_mechvent&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL (
declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT distinct coh.patienticn, coh.PatientSID, coh.Sta3n, a.InpatientICDProcedureSID, a.InpatientSID,
  a.ICD10ProcedureSID, a.ICDProcedureDateTime, e.ICD10ProcedureCode
from dflt.OS_cohort_&todaysdate as coh
inner join [Src].[Inpat_InpatientICDProcedure] a on coh.patientsid=a.PatientSID
inner join  [CDWWORK].[Dim].[ICD10Procedure] e on a.ICD10ProcedureSID=e.ICD10ProcedureSID
where a.AdmitDateTime >=@startdate2 and a.AdmitDateTime <@currentdate 
and e.ICD10ProcedureCode in ('5A1935Z','5A1945Z','5A1955Z')
);
DISCONNECT FROM TUNNEL ;
QUIT ;


/****************************************************************************************/
/*	PulseOX*/
libname pulseox '/data/dart/2021/Data/pulse ox';

PROC SQL  ;
CONNECT TO SQLSVR AS TUNNEL (DATASRC=&DATASRCs. &SQL_OPTIMAL. connection=global );

CREATE TABLE pulseox.pulseox&todaysdate AS
SELECT  *
  FROM CONNECTION TO TUNNEL (
declare @currentdate date=getdate()
declare @startdate2 date=getdate()-60

SELECT distinct coh.patienticn, coh.PatientSID, coh.Sta3n, a.vitalSignTakenDateTime, a.VitalResultNumeric,
a.SupplementalO2, a.VitalTypeSID,B.VitalType
FROM dflt.OS_cohort_&todaysdate as coh
inner join [Src].[Vital_VitalSign] as   A on coh.patientsid=a.PatientSID
left JOIN [CDWWORK].[Dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
WHERE ( a.VitalSignTakenDateTime >= @startdate2 and a.VitalSignTakenDateTime <@currentdate) /*times pulled*/
AND (
(a.SupplementalO2 <> 'NULL' and  b.VitalType='PULSE OXIMETRY' and a.VitalResultNumeric > 0 and a.VitalResultNumeric <= 100)

OR (a.SupplementalO2 is NULL and b.VitalType='PULSE OXIMETRY' and a.VitalResultNumeric > 0 and a.VitalResultNumeric <= 100)
) 
);
DISCONNECT FROM TUNNEL ;
QUIT ;


/****************************************************************************************/
