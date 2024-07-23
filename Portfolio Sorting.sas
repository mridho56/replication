/*Change the location in your local disk*/
libname main '...\Data\SAS\Main'; /*The folder to save ccm41a*/
libname ff '...\Data\FF'; /*Fama-French 5  factors*/
libname mom '...\Data\Momentum'; /*Momentum  factor*/
libname hxz '...\Data\HXZ'; /*HXZ q factors*/
/***************************Construct 5 Portfolios based on Characteristics ****************************/

proc sort data = main.ccm41a out = ccm41b; by permno year month; run;
  
/*We only consider firm-year observations with non-missing emissions*/
/*Scale by total assets: E_AT (Panel A), E_K (Panel B), E_S (Panel C), E_M_Dec (Panel D)*/
data ccm41c; set ccm41b;
port_var = E_AT;
if port_var > 0; 
run;

data ccm41_rebal_positive; set ccm41c; 
if month = 10; 
run;

/*Sort within Fama-French 49 industries*/
proc sort data = ccm41_rebal_positive;  
by fyear ff49;
run;

proc means data = ccm41_rebal_positive noprint; 
var port_var;
by fyear ff49;
output out = industry mean = ;  
run;

data industry (keep = fyear ff49 num); set industry;
if ff49 = . then delete;
num = _freq_;
run;

proc univariate data = ccm41_rebal_positive noprint; 
var port_var; 
by fyear ff49; 
output out = ccm41_sort pctlpts = 20 to 80 by 20 pctlpre = perc;
run; 

/*Drop missing industry classification, 
industries with deplicate breaking points, 
industries include less than 20 firms in a year*/
data ccm41_sort; merge ccm41_sort industry; by fyear ff49;
if ff49 = . then delete;
if num <= 20 then delete;
if perc20 = perc40 then delete;
if perc20 = perc60 then delete;
if perc20 = perc80 then delete;
if perc40 = perc60 then delete;
if perc40 = perc80 then delete;
if perc60 = perc80 then delete;
run;

data ccm41_positive; set ccm41c; 
if month = 10; 
run; 

proc sort data = ccm41_positive; by fyear ff49 port_var; run;
proc sort data = ccm41_sort; by fyear ff49; run;

data ccm41_positive (keep = permno fyear port_5); 
merge ccm41_sort ccm41_positive;
by fyear ff49;
if port_var = .   then port_5 = 0;
if perc20 >= port_var > 0                      then port_5 = 1;
if perc40 >= port_var > perc20 and perc20 ne . then port_5 = 2;  
if perc60 >= port_var > perc40 and perc40 ne . then port_5 = 3; 
if perc80 >= port_var > perc60 and perc60 ne . then port_5 = 4;  
if port_var > perc80 and perc80 ne .           then port_5 = 5; 
run;

proc sort data = ccm41c; by permno fyear; run;
proc sort data = ccm41_positive; by permno fyear; run;

data ccm42; merge ccm41c ccm41_positive ;
by permno fyear;
if port_5 = . then delete;
run; 

proc sort data = ccm42; by port_5 year month; run;

/*Cross-sectional value-weighted excess returns across portfolios*/ 
proc means data = ccm42 noprint; 
var exretadj;
weight weight_port;
by port_5 year month;
output out = csave mean = ;  
run;

/*Cross-sectional median of firm characteristics across porfolios*/
%let vars = Total_E E_AT BM IK ROA tant ww ol ME_Sept Lev_B;
proc means data = ccm42 noprint; 
var &vars;
by port_5 year month;
output out = csave_char median = ;  
run;

proc sort data = csave; by port_5 year month; run;
proc sort data = csave_char; by port_5 year month; run;

data csave (drop = _type_ _FREQ_); merge csave csave_char;
by port_5 year month;
num = _freq_;/*Number of firm within each portfolio in each month*/
run;

/*Sample period from 1992 Oct to 2018 Sept*/
data csave; set csave;
if year < 1992 then delete;
if year = 1992 and month <= 9 then delete;
if year > 2018 then delete;
if year = 2018 and month >= 10 then delete;
run;

proc sort data = csave; by port_5 year month; run;

/*Time series average of 5 port returns*/
proc means data = csave noprint;  
var exretadj &vars num; 
by port_5;
output out = tsave mean = ;  
run;

/*Time series standard deviations of portfolio returns to compute Sharpe ratios*/
proc means data = csave noprint;  
var exretadj; 
by port_5;
output out = ts_std std = std_exretadj;  
run;

data tsave;
merge tsave ts_std;
by port_5;
run; 

proc sort data = tsave; by port_5; run;

data tsave (drop = _freq_ _type_ std_exretadj ME_Sept); set tsave;
if port_5 = 0 or port_5 = . then delete;
Exret = exretadj*12;/*annualize portfolio returns by multiplying 12*/
Std = std_exretadj*sqrt(12);/*std is multiplying by sq rooted 12*/
SR = (exretadj/std_exretadj)*sqrt(12);/*annualize sharpe ratio*/
Size = log(ME_Sept);/*log of market equity in September*/
run;	

/*Tranpose the data format*/
proc transpose data = tsave out = tsave; run;

data tsave; set tsave;
if _name_ = "port_5" then delete;
rename _name_ = Var COL1 = Port_1 COL2 = Port_2 COL3 = Port_3 COL4 = Port_4 COL5 = Port_5;
run;

/*Firm Characteristics in Table 3*/
proc export data=tsave
outfile='...\JF Replication\Output\tsave.csv'
dbms=csv
replace;
run; 

/*Construct five portfolios*/
data Csave_1; set Csave (keep = port_5 exretadj year month);
if port_5 = 1;
rename exretadj = exretadj_1;
run;
data Csave_2; set Csave (keep = port_5 exretadj year month);
if port_5 = 2;
rename exretadj = exretadj_2;
run;
data Csave_3; set Csave (keep = port_5 exretadj year month);
if port_5 = 3;
rename exretadj = exretadj_3;
run;
data Csave_4; set Csave (keep = port_5 exretadj year month);
if port_5 = 4;
rename exretadj = exretadj_4;
run;
data Csave_5; set Csave (keep = port_5 exretadj year month);
if port_5 = 5;
rename exretadj = exretadj_5;
run; 

proc sort data = Csave_1; by year month; run; 
proc sort data = Csave_2; by year month; run; 
proc sort data = Csave_3; by year month; run; 
proc sort data = Csave_4; by year month; run; 
proc sort data = Csave_5; by year month; run; 

/*Construct the long-short (H-L) portfolio*/ 
data Csave_all;
merge Csave_1-Csave_5;
by year month;
exretadj_6 = exretadj_5-exretadj_1;
run;

/*The time series standard deviation of the long-short portfolio*/ 
proc means data = Csave_all noprint;  
var exretadj_6; 
output out = ts_std std = ;  
run;

proc datasets library = work nolist;
delete Csave_1-Csave_5 Ccm41_positive Ccm41_rebal_positive industry Ccm41_sort csave;
quit;

proc sort data = ff.ff5 out = ff5; by year month; run;
proc sort data = mom.mom out = mom; by year month; run;
proc sort data = hxz.hxz out = hxz; by year month; run;
proc sort data = Csave_all; by year month; run;

/*Merge with factors*/
data Csave_ff;
merge Csave_all (in = a) ff5 mom hxz;
by year month;
if a;
run;

/*Export to Stata to obtain portfolio average returns in Table 2 and run factor regressions in Table 4*/
proc export data = Csave_ff
file = "...\JF Replication\Data\Stata\Results\Csave.dta"
dbms = stata replace;
run;

proc datasets library = work nolist; 
delete ff5 hxz mom Csave_all Csave_char Csave_hml Exret
Num Tsave Tsave_all Ccm41c; 
quit;
