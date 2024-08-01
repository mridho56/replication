/* ********************************************************************************* */
/* *********************** Merge TRI with COMPUSTAT ******************************** */
libname Comp '...\JF Replication\Data\SAS\Comp';  /*Compustat*/
libname CPI '...\JF Replication\Data\SAS\CPI';  /*CPI Index*/
libname SA '...\JF Replication\Data\SAS\SA';  /*SA Index*/
libname WW '...\JF Replication\Data\SAS\WW';  /*WW Index*/
libname Rate '...\JF Replication\Data\SAS\Rate';  /*S&P Credit Rating*/
libname CAP '...\JF Replication\Data\SAS\Capital Age';  /*Capital Age*/
libname OL '...\JF Replication\Data\SAS\Operating Flexibility';  /*Operating Flexibility*/
libname Redp '...\JF Replication\Data\SAS\Redeployability';  /*Asset Redeployability*/
libname PC '...\JF Replication\Data\SAS\Contribution'; /*Political Connections*/
libname Abt '...\JF Replication\Data\SAS\Abatement';  /*Abatement*/
libname Tox '...\JF Replication\Data\SAS\Toxicity'; /*Toxicity Adjustment*/
libname TRI '...\JF Replication\Data\SAS\TRI'; /*TRI Data*/
libname Link '...\JF Replication\Data\SAS\Link Table'; /*TRI Data*/

options nosource nonotes;
/* Set the Date Range */
%let BEGDATE = 01JAN1990;
%let ENDDATE = 31DEC2017;
 
%let vars = AT SALE PPENT BE BM IK Q ROA ROE Lev_B TANT DIV D_EI D_DI D_EF SD_AT LD_AT OL profit sale; 
/* Step1. Extract Compustat Sample */
data comp (keep = gvkey permno datadate fyear year year_comp year_pc &vars);
set comp.funda;
where datadate between "&BEGDATE"d and "&ENDDATE"d
 and DATAFMT = 'STD' and INDFMT = 'INDL' and CONSOL = 'C' and POPSRC = 'D';
/* Use Daniel and Titman (JF 1997) Book of Equity Calculation: */
/*if SEQ>0;*/ /* Keep Companies with Existing Shareholders' Equity */
/* PSTKRV: Preferred stock Redemption Value . If missing, use PSTKL: Liquidating Value */
/* If still missing, then use PSTK: Preferred stock - Carrying Value, Stock (Capital)  */
PREF = coalesce(PSTKRV,PSTKL,PSTK);
/* BE = Stockholders Equity + Deferred Taxes + Envestment Tax Credit - Preferred Stock */
BE = sum(SEQ, TXDB, ITCB, -PREF);
/* Calculate Market Value of Equity at Year End */
/* use prrc_c at the calendar year end for a fair cross sectional comparison */
ME = PRCC_F*CSHO;
/* Set missing retained earnings and missing current assets figures to zero */
if missing(RE) then RE=0; if missing(ACT) then ACT=0;
/* Calculate Market-to-Book Ratio */

if BE>0 then BM = BE / ME;
month = month(datadate);
year_comp = year(datadate);

/*Even year to link to political contributions*/
remainder = year_comp - floor(year_comp/10)*10;
odd_even = mod(remainder, 2);
year_pc = year_comp-odd_even;

/*We form our portfolio at the end of Sept, because TRI information in the last year will be reported
by the end of Sept. If a firm's fiscal year end between April and Dec, then we use its accounting information
to form portfolio next year*/
if 4 <= month <= 12 then year = year(datadate)+1;
 
/*If a firm's fiscal year end between Jan and March, then we use its accounting information to form 
portfolio this year*/
if 3 >= month >= 1 then year = year(datadate);

rename lpermno = permno;
IK = capx/ppent; /*investment rate*/
Q = (me+at-ceq-txdb)/at; /*Tobin's q*/

if xsga = . then xsga = 0;
if xrd = . then xrd = 0;
/*Nominator in the Profitability*/

profit = oiadp;
ROA = profit/at; 
ROE = profit/be;
Lev_B = (dltt+dlc)/at; /*book leverage*/
TANT = ppent/at;/*tangibility*/

/*dividend payment dummy according to Farre-Mensa and Ljungqvist (2015, RFS)*/
if dvt = 0 and dvc = 0 and dvp = 0 then DIV = 0; 
if DIV = . then DIV = 1;

/*Drop total asset/sales less than one million dollars according to the Campello and Giambona (2013, JFQA)*/
if at < 1 then delete; 
if sale < 1 then delete;

/*Debt Financing*/
EI = -(prstkc+dv-sstk);
DI = -(dltr+dlcch+xint-dltis); 
EF = EI_DI;

if EI < 0 then D_EI = 1;
if EI >=0 then D_EI = 0;

if DI < 0 then D_DI = 1;
if DI >=0 then D_DI = 0;

if EF < 0 then D_EF = 1;
if EF >=0 then D_EF = 0;

SD_AT = dlc/at;
LD_AT = dltt/at;
if xsga = . then xsga = 0;
OL = (cogs+xsga)/at;
run;

proc sort data = comp nodupkey; by gvkey datadate; run;

proc expand data = comp method = none out = comp;
by gvkey;
/*lagged 1 year*/
convert roa = roa_l1 / transformout=(lag 1); 
/*future 1 year*/
convert roa = roa_f1 / transformout=(lead 1); 
/*future 2 year*/
convert roa = roa_f2 / transformout=(lead 2);  
/*future 3 year*/
convert roa = roa_f3 / transformout=(lead 3); 
/*future 4 year*/
convert roa = roa_f4 / transformout=(lead 4);  
/*future 5 year*/
convert roa = roa_f5 / transformout=(lead 5);  
/*future 6 year*/
convert roa = roa_f6 / transformout=(lead 6); 
/*future 7 year*/
convert roa = roa_f7 / transformout=(lead 7); 
/*future 8 year*/
convert roa = roa_f8 / transformout=(lead 8); 
/*future 9 year*/
convert roa = roa_f9 / transformout=(lead 9); 
/*future 10 year*/
convert roa = roa_f10 / transformout=(lead 10); 
run;

%let vars = gvkey permno datadate fyear year year_comp year_pc
AT SALE PPENT BE BM IK Q ROA1-ROA8 ROE Lev_B TANT DIV D_EI D_DI D_EF SD_AT LD_AT OL
roa_f1 roa_f3 roa_f5 roa_f7 roa_f10 ROA_cum_3 ROA_cum_5 ROA_cum_10 ROA_cum3to5 ROA_cum6to10 droa profit sale;
data comp (keep = &vars); set comp;
/*3-year Cumulative ROA*/
ROA_cum_3 = sum(of roa1_f1-roa1_f3)/3;
/*5-year Cumulative ROA*/
ROA_cum_5 = sum(of roa1_f1-roa1_f5)/5;
/*10-year Cumulative ROA*/
ROA_cum_10 = sum(of roa1_f1-roa1_f10)/10;
/*3 to 5-year Cumulative ROA*/
ROA_cum3to5 = sum(of roa1_f3-roa1_f5)/3;
/*6 to 10-year Cumulative ROA*/
ROA_cum6to10 = sum(of roa1_f6-roa1_f10)/5;
/*Changes in ROA*/
droa = roa - roa_l1;
run;

/*CPI*/
proc sort data = CPI.cpi_ann out = cpi_ann; by year; run; 
proc sort data = comp; by gvkey year_comp; run;

proc sql; 
create table comp as	
select a.*, b.cpi_09
from comp as a left join cpi_ann as b
on  a.year_comp = b.year;
quit;

data comp (drop = cpi_09 profit); set comp; 
S_r = sale/cpi_09;
Profit_r = profit/cpi_09;
run;

/*SA Index according to Hadlock and Pierce (2010, RFS)*/
proc sort data = comp; by gvkey year_comp; run;
proc sort data = sa.sa_index out = sa_index nodupkey; by gvkey year; run;

proc sql; 
create table comp as	
select a.*, b.sa
from  comp as a left join sa_index as b
on a.gvkey = b.gvkey and a.year_comp = b.year;
quit;

/*WW Index according to Whited and Wu (2006, RFS)*/
proc sort data = comp; by gvkey year_comp; run;
proc sort data = ww.ww_index out = ww_index nodupkey; by gvkey year; run;

proc sql; 
create table comp as	
select a.*, b.ww
from comp as a left join ww_index as b
on a.gvkey = b.gvkey and a.year_comp = b.year;
quit;

/*Credit Rating according to Avarmov, Chordia, Jostova, and Philipov (R&R, JF)*/
data Rating (keep = gvkey datadate Rating); set Rate.Adsprate;
if splticrm = 'AA+' or splticrm = 'AA' or splticrm = 'AA-' then Rating = 6;
if splticrm = 'A+' or splticrm = 'A' or splticrm = 'A-' then Rating = 5;
if splticrm = 'BBB+' or splticrm = 'BBB' or splticrm = 'BBB-' then Rating = 4;
if splticrm = 'BB+' or splticrm = 'BB' or splticrm = 'BB-' then Rating = 3;
if splticrm = 'B+' or splticrm = 'B' or splticrm = 'B-' then Rating = 2;
if splticrm = '' then Rating = 0;
if Rating = . then Rating = 1;
run; 

proc sort data = comp; by gvkey datadate; run;
proc sort data = Rating; by gvkey datadate; run;

proc sql; 
create table comp as	
select a.*, b.Rating
from  comp as a left join  Rating as b
on a.gvkey = b.gvkey and a.datadate = b.datadate;
quit;

data comp; set comp;
if Rating = . then Rating = 0;
run;

/*Inflexibility based on Gu, Hackbarth, and Johnson (2017, RFS)*/
proc sort data = ol.Inflexibility out = Inflexibility; by gvkey fyear; run; 
proc sort data = comp; by gvkey fyear; run;

proc sql; 
create table comp as	
select a.*, b.Inflexibility
from  comp as a left join Inflexibility as b
on a.gvkey = b.gvkey and a.fyear = b.fyear;
quit;

/*Quasi-Fixed Cost based on Gu, Hackbarth, and Johnson (2017, RFS)*/
proc sort data = ol.qfc out = qfc; by gvkey fyear; run; 
proc sort data = comp; by gvkey fyear; run;

proc sql; 
create table comp as	
select a.*, b.qfc, b.a_i_alt
from  comp as a left join qfc  as b
on a.gvkey = b.gvkey and a.fyear = b.fyear;
quit;

/*Capital Age*/
data Cag_age; set cap.Cag_age;
year = year(datadate);
if qtr(datadate) = 1;
run;

proc sort data = comp; by permno year_comp; run;
proc sort data = Cag_age nodupkey; by permno year; run;

proc sql; 
create table comp as	
select a.*, b.age
from  comp as a left join Cag_age as b
on a.permno = b.permno and a.year_comp = b.year;
quit;

/*Redeployability*/
proc sort data = redp.redp out = redp nodupkey; by gvkey year; run; 
proc sort data = comp; by gvkey year_comp; run;

proc sql; 
create table comp as	
select a.*, b.Redeploy, b.Redeploy_r, b.Redeploy_ew
from comp as a left join redp  as b
on a.gvkey = b.gvkey and a.year_comp = b.year;
quit;

/*Intangible Capital with adjustment according to Peter and Taylor (2017, JFE)*/
proc import out = intangible
datafile = '...\JF Replication\Data\Stata\Intangible\intangible.dta'
dbms = dta replace;
run;

data intangible; set intangible; 
year = year(datadate);
run;

proc sort data = comp; by permno year_comp; run;
proc sort data = intangible nodupkey; by permno year; run;

proc sql; 
create table comp as	
select a.*, b.og_at, b.rd_at
from  comp as a left join intangible as b
on a.permno = b.permno and a.year_comp = b.year;
quit;

/*G and E index*/
proc import out = Governance
datafile = '...\JF Replication\Data\Stata\Governance Index\Governance.dta'
dbms = dta replace;
run;

data Governance (drop =gvkey); set Governance;
gvkey_char = STRIP(PUT(gvkey, z6.));
run;

data Governance; set Governance;
rename gvkey_char = gvkey;
run;

proc sort data = comp; by gvkey fyear; run;
proc sort data = Governance nodupkey; by gvkey fyear; run;

proc sql; 
create table comp as	
select a.*, b.eindex, b.gindex
from  comp as a left join Governance as b
on a.gvkey = b.gvkey and a.fyear = b.fyear;
quit;

/*extract the G index at 2006 and use it for post 2006*/
data Governance_06; set Governance;
if fyear = 2006;
rename gindex = gindex_06;
run;

proc sort data = comp; by gvkey; run;
proc sort data = Governance_06 nodupkey; by gvkey; run;

proc sql; 
create table comp as	
select a.*, b.gindex_06
from  comp as a left join Governance_06 as b
on a.gvkey = b.gvkey;
quit;

/*extract the E index at 2011 and use it for post 2011*/
data Governance_11; set Governance;
if fyear = 2011;
rename eindex = eindex_11;
run;

proc sort data = comp; by gvkey; run;
proc sort data = Governance_11 nodupkey; by gvkey; run;

proc sql; 
create table comp as	
select a.*, b.eindex_11
from  comp as a left join Governance_11 as b
on a.gvkey = b.gvkey;
quit;

/*Given the data limitation, we assign the same value after 2011 for e index and 2006 for g index*/
data comp (drop = year_comp fyear eindex gindex eindex_11 gindex_06);set comp;
if fyear <= 2011 then e_index = eindex;
if fyear <= 2006 then g_index = gindex;
if fyear > 2011 then e_index = eindex_11;
if fyear > 2006 then g_index = gindex_06;
run;

/*Political Constributions*/
proc sort data = pc.Contributions out = Contributions nodupkey; by permno year; run; 
proc sort data = comp; by permno year_pc; run;

proc sql; 
create table comp as	
select a.*, b.Donations, b.Demo, b.Repub
from  comp as a left join Contributions  as b
on a.permno = b.permno and a.year_pc = b.year;
quit;

data comp; set comp; 
if Donations = . then Donations = 0;
Don_AT = Donations/at;
run;

/*Abatement Cost: ENER and ENRR*/
proc sort data = abt.Abatement out = Abatement nodupkey; by permno year; run;
proc sort data = comp; by permno year_pc; run;

proc sql; 
create table comp as	
select a.*, b.ener, b.enrr
from  comp as a left join Abatement as b
on a.permno = b.permno and a.year_pc = b.year;
quit;

/*ENV*/
proc sort data = abt.Abatement_ENV out = Abatement_ENV nodupkey; by permno year; run; 
proc sort data = comp; by permno year_pc; run;
 
proc sql; 
create table comp as	
select a.*, b.env
from  comp as a left join Abatement_ENV as b
on a.permno = b.permno and a.year_pc = b.year;
quit;

/*****************************************TRI Database*****************************************************************/
/*Export facility-level emissions and firm-level link table with crsp/compustat identifier*/
proc sort data = tri.tri_raw out = Tri; by PARENT_COMPANY_NAME; run;
proc sort data = link.match out = match; by PARENT_COMPANY_NAME; run;

proc sql; 
create table tri as	
select a.*, b.permno, b.gvkey
from  tri as a left join match as b
on a.PARENT_COMPANY_NAME = b.PARENT_COMPANY_NAME;
quit;

/*No emission observations before 1991, so I truncate the sample*/
data tri; set tri;
if permno = . then delete;
if year < 1991 then delete;
run;

/*Toxicity Adjustment: 3 year moving window*/
proc sort data = tox.Toxicity_adj out = Toxicity_adj; by chemical year; run;
proc sort data = Tri; by chemical year; run;

proc sql; 
create table Tri as	
select a.*, b.Score
from Tri as a left join Toxicity_adj as b
on a.chemical = b.chemical and a.year = b.year;
quit;

/*Given the estimation is until 2014, I assign the score after 2016 by using 2016*/
data Toxicity_adj_2016; set Toxicity_adj;
if year = 2016;
rename Score = Score_2016;
run;

proc sort data= Toxicity_adj_2016; by chemical; run;
proc sort data= Tri; by chemical; run;

proc sql; 
create table Tri as	
select a.*, b.Score_2016
from Tri as a left join Toxicity_adj_2016 as b
on a.chemical = b.chemical;
quit;

data Tri (drop = score Score_2016); set Tri;
if year <= 2016 then waste_12_adj = waste_12*score;
if year > 2016 then waste_12_adj = waste_12*Score_2016;
run;

/*Sum over all emissions at firm-level*/
proc sort data= Tri; by permno year; run;

proc means data=Tri noprint; 
var waste_12 waste_12_adj;
by permno year;
output out=Tri_firm sum = ;  
run;

data Tri_firm (drop = _type_ _freq_ year); set Tri_firm;
year_tri = year+1; /*The emisssion data in year t is released by the end of Sept in year t+1*/
run;

proc datasets library = work nolist;
delete Match Tri Rating SA_index Ww_index Inflexibility qfc Cag_Age Redp intangible Cpi_ann 
Governance Governance_06 Governance_11 Contributions Toxicity_adj Toxicity_adj_2014
Abatement Abatement_ENV;
quit;

proc sort data = Tri_firm nodupkey; by permno year_tri; run; 

/********************************************************************************************************************/
/*Combine firm-level emission data and other accounting variables*/

proc sort data= Tri_firm; by permno year_tri; run;
proc sort data= Comp; by permno year; run;

proc sql; 
create table Comp_Tri as	
select a.*, b.*
from  Tri_firm as a left join Comp as b
on a.permno = b.permno and a.year_tri = b.year;
quit;

/*Normalize by AT, PPENT, BE, or Sales*/
data Comp_Tri (drop = at ppent sale be year waste_12_adj); set Comp_Tri;
if gvkey = '' then delete;
Total_E = waste_12;
if Total_E = . then Total_E = 0;
E_AT = waste_12/at;
E_AT_adj = waste_12_adj/at;
E_K = waste_12/ppent;
E_BE = waste_12/be;
E_S = waste_12/sale;
run;

proc sort data = Comp_Tri; by permno year_tri; run;

proc expand data = Comp_Tri method = none out = Comp_Tri;
by permno;
convert E_AT = E_AT_f1 / transformout=(lead 1); 
run;

data Comp_Tri (drop = time); set Comp_Tri; run;

proc datasets library=work nolist;
delete Comp Tri_firm;
quit;
