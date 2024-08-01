/*Change the location in your local disk*/
options nosource nonotes;
libname crsp '...\JF Replication\Data\SAS\CRSP'; /*CRSP*/
libname ff '...\JF Replication\Data\SAS\FF'; /*Risk-free Rate*/
libname cpi '...\JF Replication\Data\SAS\CPI'; /*CPI Index*/
libname main '...\JF Replication\Data\SAS\Main'; /*we save the CRSP-Compustat-TRI merged result in this folder*/

/************************ Part 1: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd siccd; 
%let msfvars =  prc ret retx shrout cfacpr cfacshr; 

/*Change the location in your local disk*/
%include '...\JF Replication\Code\Data Construction\crspmerge.sas'; /*This is the sas code to merge CRSP and Compustat*/
  
%crspmerge_po(s=m,start=01jan1990,end=31dec2018,   /*<=============== Range of CRSP*******************/
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3)); 
  
/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2 
 as select a.*, b.dlret, 
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting", 
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b 
 on a.permno=b.permno and 
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E') 
 order by a.date, a.permco, MEq; 
quit; 
  
/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2; 
  by date permco Meq; 
  retain ME; 
  if first.permco and last.permco then do; 
    ME=meq; 
  output; /* most common case where a firm has a unique permno*/
  end; 
  else do ; 
    if  first.permco then ME=meq; 
    else ME=sum(meq,ME); 
    if last.permco then output; 
  end; 
run; 

data crspm2a; set crspm2a;
price = abs(prc); /*stock price per share*/
run;
  
/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date; run; 
  
/* The next step does 2 things:                                        */
/* - Create weights for later calculation of VW returns.               */
/*   Each firm's monthly return RET t willl be weighted by             */
/*   ME(t-1) = ME(t-2) * (1 + RETX (t-1))                              */
/*     where RETX is the without-dividend return.                      */
/* - Create a File with December t-1 Market Equity (ME)                */
data crspm3 (keep=permno date retadj weight_port ME exchcd shrcd cumretx siccd price) 
decme (keep = permno date ME rename=(me=DEC_ME) )  ; 
     set crspm2a; 
 by permno date; 
 retain weight_port cumretx me_base; 
 Lpermno=lag(permno); 
 LME=lag(me); 
     if first.permno then do; 
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end; 
     else do; 
     if month(date)=7 then do; 
        weight_port= LME; 
        me_base=LME; /* lag ME also at the end of Aug */
        cumretx=sum(1,retx); 
     end; 
     else do; 
        if LME>0 then weight_port=cumretx*me_base; 
        else weight_port=.; 
        cumretx=cumretx*sum(1,retx); 
     end; end; 
output crspm3; 
if month(date)=12 and ME>0 then output decme; 
run; 

/*Impose restriction for stock return data to be considered in empirical tests*/
data ccm4 (drop = cumretx); set crspm3; 
 where weight_port>0 and exchcd in (1,2,3)  
      and shrcd in (10,11); 
	  year = year(date);
	  month = month(date);
	  if month >= 10 then fyear = year; 
      if month <= 9 then fyear = year-1; 
run;

/*Deflate market equity by CPI index*/
proc sort data = cpi.cpi_ann out = cpi_ann; by date; run;

data crspm3; set crspm3;
year = year(date);
run;

proc sort data = crspm3; by year; run;

proc sql; 
create table crspm3 as	
select a.*, b.CPI_09
from  crspm3 as a left join cpi_ann as b
on a.year = b.year ;
quit;

data ME (keep = permno fyear ME_Sept); set crspm3;
where weight_port>0 and exchcd in (1,2,3) and shrcd in (10,11); 
year = year(date);
month = month(date);
if month = 9; 
fyear = year -1;
ME_Size = me/1000; /*in million dollars*/
ME_Sept = ME_Size/CPI_09;/*in real term*/
run;

proc sort data = ccm4; by permno fyear; run;
proc sort data = ME; by permno fyear; run;

proc sql; 
create table ccm4 as	
select a.*, b.ME_Sept
from  ccm4 as a left join ME as b
on  a.permno = b.permno and a.fyear = b.fyear ;
quit;

/*Extract ME in Dec*/
data ME_Dec (keep = permno fyear ME_Dec); set crspm3;
where weight_port>0 and exchcd in (1,2,3) and shrcd in (10,11); 
year = year(date);
month = month(date);
if month = 12; 
fyear = year +1;
ME_Dec = me/1000;
run;

proc sort data = ccm4; by permno fyear; run;
proc sort data = ME_Dec; by permno fyear; run;

proc sql; 
create table ccm4 as	
select a.*, b.ME_Dec
from  ccm4 as a left join ME_Dec as b
on  a.permno = b.permno and a.fyear = b.fyear ;
quit;

proc sort data = ccm4 nodupkey; by permno date; run; 

/*Risk Free Rate*/
proc sort data = ff.rf out = rf; by year month; run;
proc sort data = ccm4; by permno year month; run;

proc sql; 
create table ccm4 as	
select a.*, b.rf
from  ccm4 as a left join  rf  as b
on  a.year = b.year and a.month = b.month ;
quit;

data ccm4 (drop = rf) ; set ccm4;
exretadj = (retadj -rf)*100;/*in percentages*/
run;

proc sort data=ccm4 nodupkey; by permno date; run; 

/*Compustat to construct accounting variables*/
/*Change the location in your local disk*/
%include '...\JF Replication\Code\Data Construction\Comp.sas'; 

/*Merge with Annual Compustat*/
proc sort data = ccm4; by permno fyear; run; 
proc sort data = comp_tri; by permno year_tri; run; 
 
proc sql; 
create table ccm41a as	
select a.*, b.*
from ccm4 as a left join comp_tri as b
on a.permno = b.permno and a.fyear = b.year_tri;
quit;

/*Economic Uncertainty (Sydney Ludvigson)*/
/*Change the location in your local disk*/
proc import out = unc_exp
datafile = '...\JF Replication\Data\Stata\Exposure\unc_exp.dta'
dbms = dta replace;
run;

data unc_exp; set unc_exp;
if month = 9;
if unc_beta ~= .;
run;

proc sort data = unc_exp; by permno year; run; 
proc sort data = ccm41a; by permno fyear; run; 

proc sql; 
create table ccm41a as	
select a.*, b.unc_beta
from ccm41a as a left join unc_exp as b
on a.permno = b.permno and a.fyear = b.year;
quit;

/*Economic Policy Uncertainty (Nick Bloom)*/
/*Change the location in your local disk*/
proc import out = epu_exp
datafile = '...\JF Replication\Data\Stata\Exposure\epu_exp.dta'
dbms = dta replace;
run;

data epu_exp; set epu_exp;
if month = 9;
if epu_beta ~= .;
run;

proc sort data = epu_exp; by permno year; run; 
proc sort data = ccm41a; by permno fyear; run; 

proc sql; 
create table ccm41a as	
select a.*, b.epu_beta
from ccm41a as a left join epu_exp as b
on a.permno = b.permno and a.fyear = b.year;
quit;

data ccm41a (drop = me retadj shrcd year_tri waste_12 ME_Dec); set ccm41a;
E_M_Sept = waste_12/ME_Sept;
E_M_Dec = waste_12/ME_Dec;
if year < 1992 then delete;
if year = 1992 and month <= 9 then delete;
if year > 2018 then delete;
if year = 2018 and month >= 10 then delete;
if 6000 <= siccd <= 6999 then delete; /*drop finance industry*/
if 9000 <= siccd <= 9999 then delete; /*drop public administrative industry*/
if gvkey = '' then delete; /*drop firms without chemical emissions*/
run;

proc datasets library = work nolist;
delete crspm2 crspm2a crspm3 crsp_m decme rf ccm4 size_bb comp_tri me me_june me_dec cpi_ann unc_exp epu_exp;
quit;

/*Fama-French 17, 30, 48, and 49 industry classification*/
/*Change the location in your local disk*/
%include '...\JF Replication\Code\Data Construction\Industry Classification.sas'; 

/*save in the main folder*/
proc sort data = ccm41a out = main.ccm41a; by permno year month; run; 

endsas;
