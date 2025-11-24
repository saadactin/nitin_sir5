-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
Create Procedure ATP_D1_GETTQA_BY_CHECKID(In CheckID Integer)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
Begin
	Select T0."CheckID",
       	T0."CfmDate",
       	T0."CfmQty",
       	T0."TQAType",
       	T0."ReqQty",
		T0."ObjType",
		T0."DocEntry",
		T0."DocLineNum",
		T0."SchdLine",
		T0."ItemCode",
		T0."WhsCode",
		T1."WhsName"
-- it is not necessary to add the transaction time, we can use time travel to get this information
       From OTQA T0 Join OWHS T1 on T0."WhsCode" = T1."WhsCode"
       Where T0."CheckID" = :CheckID;
End;











