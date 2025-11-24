-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_Inventory_Taking_Report
(
in itemCodeList_in nvarchar(15000),
in whseCodeList_in nvarchar(15000),
in binLocationList_in nvarchar(15000),
in userCodeList_in nvarchar(15000),
in employeeList_in nvarchar(15000),
in whseUsedType_in nvarchar(32),
in countDateFrom_in timestamp,
in countDateTo_in timestamp,
in postDateFrom_in timestamp,
in postDateTo_in timestamp,
in itemFrom_in nvarchar(50),
in itemTo_in nvarchar(50),
in supplierFrom_in nvarchar(16),
in supplierTo_in nvarchar(16),
in whseIncludeFrom_in nvarchar(16),
in whseIncludeTo_in nvarchar(16),
in whseExcludeFrom_in nvarchar(16),
in whseExcludeTo_in nvarchar(16),
in countingDisplayType_in nvarchar(16),
in notCountSince_in timestamp,
in postDiffPercGT_in nvarchar(16),
in whseIncludeChecked_in integer,
in whseExcludeChecked_in integer,
in binLocationChecked_in integer,
in displayCounting_in integer,
in userChecked_in integer,
in employeeChecked_in integer,
in notCountSinceChecked_in integer,
in displayPosting_in integer,
in postDiffPercGTChecked_in integer
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
AS
execSQL nvarchar(30000);
countingSQL nvarchar(15000);
postingSQL nvarchar(15000);
nullSQL nvarchar(15000);
itwSQL nvarchar(15000);
ibqSQL nvarchar(15000);
subSQL1 nvarchar(1000);
subSQL2 nvarchar(1000);
selectStr nvarchar(1000);
fromStr nvarchar(1000);
whereStr nvarchar(15000);
whereCommon nvarchar(15000);
whereItemAndWhs nvarchar(15000);
itemCodeList nvarchar(15000);
whseCodeList nvarchar(15000);
binLocationList nvarchar(15000);
userCodeList nvarchar(15000);
employeeList nvarchar(15000);
whseUsedType nvarchar(32);
countDateFrom timestamp;
countDateTo timestamp;
postDateFrom timestamp;
postDateTo timestamp;
itemFrom nvarchar(50);
itemTo nvarchar(50);
supplierFrom nvarchar(16);
supplierTo nvarchar(16);
whseIncludeFrom nvarchar(16);
whseIncludeTo nvarchar(16);
whseExcludeFrom nvarchar(16);
whseExcludeTo nvarchar(16);
countingDisplayType nvarchar(16);
notCountSince timestamp;
postDiffPercGT nvarchar(16);
whseIncludeChecked integer;
whseExcludeChecked integer;
binLocationChecked integer;
displayCounting integer;
userChecked integer;
employeeChecked integer;
notCountSinceChecked integer;
displayPosting integer;
postDiffPercGTChecked integer;
transType nvarchar(2);
typeUSR integer := 12;
typeHEM integer := 171;
BEGIN 
    itemCodeList			:= :itemCodeList_in;
	whseCodeList			:= :whseCodeList_in;
	binLocationList			:= :binLocationList_in;
	userCodeList			:= :userCodeList_in;
	employeeList			:= :employeeList_in;
	whseUsedType			:= :whseUsedType_in;
	countDateFrom			:= :countDateFrom_in;
	countDateTo				:= :countDateTo_in;
	postDateFrom			:= :postDateFrom_in;
	postDateTo				:= :postDateTo_in;
	itemFrom				:= :itemFrom_in;
	itemTo					:= :itemTo_in;
	supplierFrom			:= :supplierFrom_in;
	supplierTo				:= :supplierTo_in;
	whseIncludeFrom			:= :whseIncludeFrom_in;
	whseIncludeTo			:= :whseIncludeTo_in;
	whseExcludeFrom			:= :whseExcludeFrom_in;
	whseExcludeTo			:= :whseExcludeTo_in;
	countingDisplayType		:= :countingDisplayType_in;
	notCountSince			:= :notCountSince_in;
	postDiffPercGT			:= :postDiffPercGT_in;
	whseIncludeChecked		:= :whseIncludeChecked_in;
	whseExcludeChecked		:= :whseExcludeChecked_in;
	binLocationChecked		:= :binLocationChecked_in;
	displayCounting			:= :displayCounting_in;
	userChecked				:= :userChecked_in;
	employeeChecked 		:= :employeeChecked_in;
	notCountSinceChecked	:= :notCountSinceChecked_in;
	displayPosting			:= :displayPosting_in;
	postDiffPercGTChecked	:= postDiffPercGTChecked_in;
    whereItemAndWhs 	:= ' where T0."ItemCode" in ' || :itemCodeList;
    
    IF (LENGTH(:itemFrom) > 0) THEN 
        whereItemAndWhs := :whereItemAndWhs || ' and T0."ItemCode" >= ''' || :itemFrom || ''' ';
    END IF;
    IF (LENGTH(:itemTo) > 0) THEN 
        whereItemAndWhs := :whereItemAndWhs || ' and T0."ItemCode" <= ''' || :itemTo || ''' ';
    END IF;
    IF (:whseUsedType = 'L') THEN
    	----------- Used By Location
        whereItemAndWhs := :whereItemAndWhs || ' and T0."WhsCode" in ' || :whseCodeList;
    ELSE
   		----------- Used By Warehouse 
        IF (:whseIncludeChecked = 1) THEN 
            IF (LENGTH(:whseIncludeFrom) > 0) THEN 
                whereItemAndWhs := :whereItemAndWhs || ' and T0."WhsCode" >= ''' || :whseIncludeFrom || ''' ';
            END IF;
            IF (LENGTH(:whseIncludeTo) > 0) THEN 
                whereItemAndWhs := :whereItemAndWhs || ' and T0."WhsCode" <= ''' || :whseIncludeTo || ''' ';
            END IF;
        END IF;
        IF (:whseExcludeChecked = 1) THEN 
            IF ((LENGTH(:whseExcludeFrom) > 0) AND (LENGTH(:whseExcludeTo) > 0)) THEN 
                whereItemAndWhs := :whereItemAndWhs || ' and (';
                whereItemAndWhs := :whereItemAndWhs || ' T0."WhsCode" < ''' || :whseExcludeFrom || ''' ';
                whereItemAndWhs := :whereItemAndWhs || ' or ';
                whereItemAndWhs := :whereItemAndWhs || ' T0."WhsCode" > ''' || :whseExcludeTo || ''' ';
                whereItemAndWhs := :whereItemAndWhs || ')';
            ELSE 
                IF (LENGTH(:whseExcludeFrom) > 0) THEN 
                    whereItemAndWhs := :whereItemAndWhs || n' and T0."WhsCode" < ''' || :whseExcludeFrom || ''' ';
                END IF;
                IF (LENGTH(:whseExcludeTo) > 0) THEN 
                    whereItemAndWhs := :whereItemAndWhs || n' and T0."WhsCode" > ''' || :whseExcludeTo || ''' ';
                END IF;
            END IF;
        END IF;
    END IF;
  
    whereCommon := :whereItemAndWhs;
    IF (:binLocationChecked = 1) THEN 
        whereCommon := :whereCommon || n' and (T0."BinEntry" in ' || :binLocationList || ')';
    END IF;
    IF (LENGTH(:countDateFrom) > 0) THEN 
        whereCommon := :whereCommon || ' and T0."CountDate" >= ''' || :countDateFrom || ''' ';
    END IF;
    IF (LENGTH(:countDateTo) > 0) THEN 
        whereCommon := :whereCommon || ' and T0."CountDate" <= ''' || :countDateTo || ''' ';
    END IF;
    IF (:displayCounting = 1) THEN 
        transType := 'C';
        selectStr := 'Select ''' || :transType || ''' as "TransType", ';
        selectStr := :selectStr ||
        			' T0."DocEntry",
        			  T1."DocNum",
        			  T0."VisOrder" as "LineNum",
        			  T0."ItemCode",
        			  T0."ItemDesc",
        			  T0."WhsCode",
        			  T0."BinEntry",
        			  T2."BinCode",
        			  T0."InWhsQty",
        			  T0."CountQty",
        			  T0."Difference",
        			  T0."DiffPercen" as "DiffPercnt",
        			  NULL as "Price",
        			  NULL as "Total",
        			  NULL as "Currency",
        			  T0."CountDate",
        			  NULL as "PostDate",
        			  T1."Status",
        			  T0."LineStatus"';
        fromStr := ' FROM INC1 T0 join OINC T1 on T0."DocEntry" = T1."DocEntry" left outer join OBIN T2 on T0."BinEntry = T2."AbsEntry"';
        whereStr := :whereCommon;
        IF (LENGTH(:supplierFrom) > 0) THEN 
            whereStr := :whereStr || ' and T0."PrefVendor" >= ''' || :supplierFrom || ''' ';
        END IF;
        IF (LENGTH(:supplierTo) > 0) THEN 
            whereStr := :whereStr || ' and T0."PrefVendor" <= ''' || :supplierTo || ''' ';
        END IF;
        IF (:userChecked = 1) THEN 
            whereStr := :whereStr || ' and ((';
            whereStr := :whereStr || ' T1."Taker1Type" = ' || :typeUSR;
            whereStr := :whereStr || ' and T1."Taker1Id" in ' || :userCodeList;
            whereStr := :whereStr || ') or (';
            whereStr := :whereStr || ' T1."Taker2Type" = ' || :typeUSR;
            whereStr := :whereStr || ' and T1."Taker2Id" in ' || :userCodeList;
            whereStr := :whereStr || '))';
        END IF;
        IF (:employeeChecked = 1) THEN 
            whereStr := :whereStr || ' and ((';
            whereStr := :whereStr || ' T1."Taker1Type" = ' || :typeHEM;
            whereStr := :whereStr || ' and T1."Taker1Id" in ' || :employeeList;
            whereStr := :whereStr || ') or (';
            whereStr := :whereStr || ' T1."Taker2Type" = ' || :typeHEM;
            whereStr := :whereStr || ' and T1."Taker2Id" in ' || :employeeList;
            whereStr := :whereStr || '))';
        END IF;
        IF (:countingDisplayType = 'O' OR :countingDisplayType = 'C') THEN 
            whereStr := :whereStr || ' and T0."LineStatus" = ''' || :countingDisplayType || ''' ';
        END IF;
        IF (:notCountSinceChecked = 1 AND LENGTH(:notCountSince) > 0) THEN 
            whereStr := :whereStr || ' and (';
            whereStr := :whereStr || ' T0."Counted" = ''' || 'N' || ''' ';
            whereStr := :whereStr || ' and ';
            whereStr := :whereStr || ' T0."CountDate" >= ''' || :notCountSince || ''' ';
            whereStr := :whereStr || ')';
        END IF;
        countingSQL := :selectStr || :fromStr || :whereStr;
    END IF;
    IF (:displayPosting = 1) THEN 
        transType := 'P';
        selectStr := 'Select ''' || :transType || ''' as "TransType", ';
        selectStr := :selectStr || 
        			' T0."DocEntry", 
        			T1."DocNum", 
        			T0."DocLineNum" as "LineNum", 
        			T0."ItemCode", 
        			T0."ItemName" as "ItemDesc", 
        			T0."WhsCode", 
        			T0."BinEntry", 
        			T2."BinCode", 
        			T0."OnHandBef" as "InWhsQty", 
        			T0."CountQty", 
        			T0."Quantity" as "Difference", 
        			T0."DiffPercnt", 
        			T0."Price", 
        			T0."DocTotal" as "Total", 
        			T0."Currency", 
        			T0."CountDate", 
        			T1."DocDate" as "PostDate", 
        			T1."Status", 
        			NULL as "LineStatus"';
        fromStr := ' FROM IQR1 T0 join OIQR T1 on T0."DocEntry" = T1."DocEntry" left outer join OBIN T2 on T0."BinEntry" = T2."AbsEntry"';
        whereStr := :whereCommon;
        IF (LENGTH(:supplierFrom) > 0) THEN 
            whereStr := :whereStr || ' and T0."CardCode" >= ''' || :supplierFrom || ''' ';
        END IF;
        IF (LENGTH(:supplierTo) > 0) THEN 
            whereStr := :whereStr || ' and T0."CardCode" <= ''' || :supplierTo || ''' ';
        END IF;
        IF (LENGTH(:postDateFrom) > 0) THEN 
            whereStr := :whereStr || ' and T1."DocDate" >= ''' || :postDateFrom || ''' ';
        END IF;
        IF (LENGTH(:postDateTo) > 0) THEN 
            whereStr := :whereStr || ' and T1."DocDate" <= ''' || :postDateTo || ''' ';
        END IF;
        IF (:postDiffPercGTChecked = 1 AND :postDiffPercGT <> '') THEN 
            whereStr := :whereStr || ' and T0."DiffPercnt" >= ' || :postDiffPercGT;
        END IF;
        postingSQL := :selectStr || :fromStr || :whereStr;
    END IF;
    nullSQL := 'Select Cast(NULL as nvarchar(1)) as "TransType", NULL as "DocEntry", NULL as "DocNum", NULL as "LineNum", Cast(NULL as char(20)) as "ItemCode", '||
    			'Cast(NULL as char(200)) as "ItemDesc", Cast(NULL as char(8)) as "WhsCode", NULL as "BinEntry", Cast(NULL as char(228)) as "BinCode", NULL as "InWhsQty", '||
    			'NULL as "CountQty", NULL as "Difference", NULL as "DiffPercnt", NULL as "Price", NULL as "Total", Cast(NULL as char(20)) as "Currency", '||
    			'Cast(NULL as DateTime) as "CountDate", Cast(NULL as DateTime) as "PostDate", Cast(NULL as nvarchar(1)) as "Status", Cast(NULL as nvarchar(1)) as "LineStatus" from DUMMY';
    execSQL := :nullSQL;
    IF (:displayCounting = 1 AND :displayPosting = 1) THEN 
        execSQL := :execSQL || ' union all ' || :countingSQL || ' union all ' || :postingSQL;
    ELSE 
        IF (:displayCounting = 1) THEN 
            execSQL := :execSQL || ' union all ' || :countingSQL;
        ELSE 
            IF (:displayPosting = 1) THEN 
                execSQL := :execSQL || ' union all ' || :postingSQL;
            END IF;
        END IF;
    END IF;
    IF (:notCountSinceChecked = 1 AND LENGTH(:notCountSince) > 0) THEN
    	------------------------------Load not counted items from oitw that the record is not in INC1 table--------------------------------------------------------------------------------- 
        selectStr := 'Select NULL as "TransType", NULL as "DocEntry", NULL as "DocNum", NULL as "LineNum", T0."ItemCode", T1."ItemName" as "ItemDesc", T0."WhsCode", '|| 
        				'NULL as "BinEntry", Cast(NULL as char(228)) as "BinCode", T0."OnHand" as "InWhsQty", NULL as "CountQty", NULL as "Difference", NULL as "DiffPercnt", '||
        				'NULL as "Price", NULL as "Total", Cast(NULL as char(20)) as "Currency", Cast(NULL as DateTime) as "CountDate", Cast(NULL as DateTime) as "PostDate", '||
        				'Cast(NULL as nvarchar(1)) as "Status", Cast(NULL as nvarchar(1)) as "LineStatus"';
        fromStr := ' from OITW T0 join OITM T1 on T0."ItemCode" = T1."ItemCode"';
        subSQL1 := ' and not exists (' || 'select T2."ItemCode", T2."WhsCode" from INC1 T2 where T0."ItemCode" = T2."ItemCode" and T0."WhsCode" = T2."WhsCode" and T2."BinEntry" is NULL '||
        			'GROUP BY T2."ItemCode", T2."WhsCode" HAVING Max(T2."CountDate")<=''' || :notCountSince || ''')';
        subSQL2 := ' and not exists (' || ' select T4."ItemCode", T4."WhsCode" from IQR1 T4 where T0."ItemCode" = T4."ItemCode" and T0."WhsCode" = T4."WhsCode" and T4."BinEntry" is NULL '||
        			'GROUP BY T4."ItemCode", T4."WhsCode" HAVING Max(T4."CountDate")<= ''' || :notCountSince || ''')';
        itwSQL := :selectStr || :fromStr || :whereItemAndWhs || :subSQL1 || :subSQL2;
        ------------------------------Load not counted items from OIBQ that the warehouse is bin active and the record is not in INC1 table------------------------------------------     
        selectStr := 'Select NULL as "TransType", NULL as "DocEntry", NULL as "DocNum", NULL as "LineNum", T0."ItemCode", T1."ItemName" as "ItemDesc", T0."WhsCode", T2."AbsEntry" as "BinEntry", T2."BinCode", '|| 
 					'T0."OnHandQty" as "InWhsQty", NULL as "CountQty", NULL as "Difference", NULL as "DiffPercnt", NULL as "Price", NULL as "Total", Cast(NULL as char(20)) as "Currency", Cast(NULL as DateTime) as "CountDate", '||
 					'Cast(NULL as DateTime) as "PostDate", Cast(NULL as nvarchar(1)) as "Status", Cast(NULL as nvarchar(1)) as "LineStatus"';
        fromStr := ' from OIBQ T0 join OITM T1 on T0."ItemCode" = T1."ItemCode" join OBIN T2 on T0."BinAbs" = T2."AbsEntry"';
        subSQL1 := ' and not exists (' || 'select T3."ItemCode", T3."WhsCode" from INC1 T3 where T0."ItemCode" = T3."ItemCode" and T0."WhsCode" = T3."WhsCode" and T0."BinAbs" = T3."BinEntry" '||
        			' GROUP BY T3."ItemCode", T3."WhsCode" HAVING Max(T3."CountDate")<= ''' || :notCountSince || ''')';
        subSQL2 := ' and not exists (' || ' select T5."ItemCode", T5."WhsCode" from IQR1 T5 where T0."ItemCode" = T5."ItemCode" and T0."WhsCode" = T5."WhsCode" and T0."BinAbs" = T5."BinEntry" '|| 
        			' GROUP BY T5."ItemCode", T5."WhsCode" HAVING Max(T5."CountDate")<= ''' || :notCountSince || ''')';
        ibqSQL := :selectStr || :fromStr || :whereItemAndWhs || :subSQL1 || :subSQL2;
        IF (:binLocationChecked = 1) THEN 
            ibqSQL := :ibqSQL || ' and (T2."AbsEntry" in ' || :binLocationList || ')';
        END IF;
        execSQL := :nullSQL || ' union all ' || :itwSQL || ' union all ' || :ibqSQL;
    END IF;
    execute immediate(:execSQL);
	--select :execSQL from dummy;
END;











