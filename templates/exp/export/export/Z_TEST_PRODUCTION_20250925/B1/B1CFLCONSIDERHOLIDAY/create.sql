
	CREATE PROCEDURE B1CFLConsiderHoliday(v_dtIn DATE, v_nvHldCode NVARCHAR(20), out v_dtOut DATE)
	AS
		v_siWndFrm SMALLINT; -- Although OHLD.WndFrm is NVARCHAR(1) !!!
		v_siWndTo SMALLINT; -- Although OHLD.WndTo is NVARCHAR(1) !!!
		v_cIsCurYear NVARCHAR(1);
		v_cIgnrWnd NVARCHAR(1);
		v_siWeekDay SMALLINT;
		v_dtLast DATE;
		l_dtIn DATE;
		v_cnt integer := 0;
BEGIN
	IF IFNULL(:V_NVHLDCODE,'') = '' THEN
		v_dtOut := :V_DTIN;
	ELSE 
		
		select count(*) into v_cnt FROM OHLD WHERE "HldCode" = :V_NVHLDCODE;
		if :V_CNT > 0 then
			SELECT to_smallint("WndFrm"),to_smallint("WndTo"),"isCurYear","ignrWnd" INTO v_siWndFrm, v_siWndTo, 
			v_cIsCurYear,v_cIgnrWnd FROM OHLD WHERE "HldCode" = :V_NVHLDCODE;
		end if;
		v_dtLast := :V_DTIN;
		l_dtIn := :V_DTIN;
		
		WHILE :V_DTLAST is not null do
		
			IF :V_CISCURYEAR = 'Y' then
				select count(*) into v_cnt FROM HLD1 WHERE "HldCode" = :V_NVHLDCODE AND :L_DTIN >= "StrDate" AND :L_DTIN <= "EndDate";
				if :V_CNT > 0 then
					SELECT MAX("EndDate") into v_dtLast FROM HLD1 WHERE "HldCode" = :V_NVHLDCODE AND :L_DTIN >= "StrDate" AND :L_DTIN <= "EndDate";
				else 
					v_dtLast := Null;
				end if;
			ELSE
				select count(*) into v_cnt FROM HLD1
					WHERE "HldCode" = :V_NVHLDCODE AND
					:L_DTIN >= add_months(add_days(to_date(to_varchar(YEAR(:L_DTIN))),EXTRACT (DAY FROM "StrDate") - 1),MONTH("StrDate") - 1) AND
					:L_DTIN <= add_months(add_days(to_date(to_varchar(YEAR(:L_DTIN))),EXTRACT (DAY FROM "EndDate") - 1),MONTH("EndDate") - 1);
				if :V_CNT > 0 then
					SELECT MAX(add_months(add_days(to_date(to_varchar(YEAR(:L_DTIN))),EXTRACT (DAY FROM "EndDate") - 1),MONTH("EndDate") - 1)) INTO v_dtLast FROM HLD1
					WHERE "HldCode" = :V_NVHLDCODE AND
					:L_DTIN >= add_months(add_days(to_date(to_varchar(YEAR(:L_DTIN))),EXTRACT (DAY FROM "StrDate") - 1),MONTH("StrDate") - 1) AND
					:L_DTIN <= add_months(add_days(to_date(to_varchar(YEAR(:L_DTIN))),EXTRACT (DAY FROM "EndDate") - 1),MONTH("EndDate") - 1);
				else 
					v_dtLast := null;
				end if;

			END IF;
			
			IF :V_DTLAST is not null then 
				l_dtIn := ADD_DAYS(:L_DTIN,DAYS_BETWEEN(:L_DTIN, :V_DTLAST) + 1);
			ELSE
				IF :V_CIGNRWND = 'N' then						
					select MAP(WEEKDAY(:L_DTIN),0,2,1,3,2,4,3,5,4,6,5,7,6,1) into v_siWeekDay from dummy;
					IF :V_SIWNDFRM <= :V_SIWNDTO THEN
						IF :V_SIWEEKDAY >= :V_SIWNDFRM AND :V_SIWEEKDAY <= :V_SIWNDTO THEN
							l_dtIn := ADD_DAYS(:L_DTIN,:V_SIWNDTO - :V_SIWEEKDAY + 1);
							v_dtLast := :L_DTIN;
						END IF;
					ELSE
						IF :V_SIWEEKDAY >= :V_SIWNDFRM then
							l_dtIn := ADD_DAYS(:L_DTIN,8 + :V_SIWNDTO - :V_SIWEEKDAY);
							v_dtLast := l_dtIn;
						ELSE
							IF :V_SIWEEKDAY <= :V_SIWNDTO THEN
								l_dtIn := ADD_DAYS(:L_DTIN, 1 + :V_SIWNDTO - :V_SIWEEKDAY);
								v_dtLast := :L_DTIN;
							END IF;
						END if;
					END IF;
				END IF;
			END if;
		end while;
		v_dtOut := to_date(:L_DTIN);
	END IF;
END











