CREATE TABLE employee_sales (
    sale_id                NUMBER PRIMARY KEY,
    employee_id            NUMBER NOT NULL,
    employee_name          VARCHAR2(100),
    monthly_target_points  NUMBER,
    client_type            VARCHAR2(20),
    credit_type            VARCHAR2(30),
    loan_category          VARCHAR2(30),
    is_card_transfer       VARCHAR2(5),
    credit_amount          VARCHAR2(50),
    sale_date              VARCHAR2(30),
    status                 VARCHAR2(20),
    loan_reference         VARCHAR2(50)
);

select * from employee_sales

drop table employee_sales

/*employee_sales cədvəli bankın Filial (Branch) və Zəng Mərkəzi (Call Center) əməkdaşlarının ay ərzində həyata keçirdiyi 
kredit satışlarını və onların performans göstəricilərini izləmək üçün nəzərdə tutulub.*/

/*Cədvəl Sütunlarının İzahı:
sale_id: Hər bir kredit satışı əməliyyatının unikal nömrəsi (ID).
employee_id: Satışı edən bank əməkdaşının ID-si.
employee_name: Əməkdaşın ad və soyadı.
monthly_target_points: Əməkdaşın cari ay üçün yerinə yetirməli olduğu Aylıq Hədəf Balı.
client_type: Kredit ayrılan müştərinin tipidir:
-SALARY: Bankdan əməkhaqqı alan (maaş kartı olan) müştərilər.
-STREET: Kənar (bankda maaş kartı olmayan) müştərilər.
credit_type: Verilən kreditin məhsul növüdür (Consumer, Business, Mortgage, Auto).
loan_category: Kreditin verilmə forması:
-NEW: Müştərinin ilk dəfə götürdüyü yeni kredit.
-INTERNAL_REFINANCE: Müştərinin bankımızdakı mövcud kreditini bağlayaraq yeniləməsi.
-EXTERNAL_REFINANCE: Müştərinin başqa bankdakı kreditini bizim banka köçürməsi.
is_card_transfer: Kredit məbləğləri yekunda müştərinin kartına köçürülüb (YES) və ya kassadan nağd verilib (NO).
credit_amount: Satılan kreditin məbləği və valyutası (Məsələn: 2500 AZN, 15000 USD, 5000 EUR).
sale_date: Kredit satışının rəsmiləşdirildiyi tarix.
status: Satış əməliyyatının cari statusudur:
-APPROVED: Təsdiqlənmiş və verilmiş kreditlər.
-PENDING: Təsdiq gözləyən kreditlər.
loan_reference: Kredit müqaviləsinin referans kodu (kodun son 3 simvolu kreditin aid olduğu filial kodunu bildirir)*/



INSERT INTO employee_sales VALUES (1, 1, 'Elvin Mammadov', 6, 'SALARY', 'Consumer', 'NEW', 'YES', '15000 AZN', '10-08-2026', 'APPROVED', 'LN-84101-001');
INSERT INTO employee_sales VALUES (2, 1, 'Elvin Mammadov', 6, 'STREET', 'Auto', 'EXTERNAL_REFINANCE', 'NO', '12000 USD', '20-08-2026', 'APPROVED', 'LN-84102-001');
INSERT INTO employee_sales VALUES (3, 1, 'Elvin Mammadov', 9, 'SALARY', 'Business', 'NEW', 'YES', '25000 AZN', '10-09-2026', 'APPROVED', 'LN-94103-001');
INSERT INTO employee_sales VALUES (4, 2, 'Aysel Aliyeva', 6, 'STREET', 'Consumer', 'NEW', 'YES', '8000 AZN', '12-08-2026', 'APPROVED', 'LN-84201-001');
INSERT INTO employee_sales VALUES (5, 2, 'Aysel Aliyeva', 9, 'SALARY', 'Consumer', 'INTERNAL_REFINANCE', 'YES', '10000 EUR', '18-09-2026', 'PENDING', 'LN-94202-001');
INSERT INTO employee_sales VALUES (6, 2, 'Aysel Aliyeva', 9, 'STREET', 'Business', 'EXTERNAL_REFINANCE', 'NO', '14000 USD', '25-09-2026', 'APPROVED', 'LN-94203-001');
INSERT INTO employee_sales VALUES (7, 3, 'Rashad Guliyev', 6, 'SALARY', 'Mortgage', 'NEW', 'NO', '40000 AZN', '15-08-2026', 'APPROVED', 'LN-84301-001');
INSERT INTO employee_sales VALUES (8, 3, 'Rashad Guliyev', 6, 'STREET', 'Consumer', 'NEW', 'YES', '10000 AZN', '28-08-2026', 'APPROVED', 'LN-84302-001');
INSERT INTO employee_sales VALUES (9, 3, 'Rashad Guliyev', 9, 'SALARY', 'Auto', 'NEW', 'YES', '18000 AZN', '05-09-2026', 'APPROVED', 'LN-94303-001');
INSERT INTO employee_sales VALUES (10, 4, 'Leyla Huseynova', 6, 'STREET', 'Consumer', 'EXTERNAL_REFINANCE', 'YES', '5000 AZN', '05-08-2026', 'APPROVED', 'LN-84401-001');
INSERT INTO employee_sales VALUES (11, 4, 'Leyla Huseynova', 9, 'SALARY', 'Consumer', 'NEW', 'NO', '15000 AZN', '12-09-2026', 'APPROVED', 'LN-94402-001');
INSERT INTO employee_sales VALUES (12, 4, 'Leyla Huseynova', 9, 'STREET', 'Business', 'NEW', 'YES', '20000 USD', '22-09-2026', 'APPROVED', 'LN-94403-001');
INSERT INTO employee_sales VALUES (13, 5, 'Farid Karimov', 4, 'SALARY', 'Business', 'NEW', 'YES', '10000 AZN', '03-08-2026', 'APPROVED', 'LN-85101-002');
INSERT INTO employee_sales VALUES (14, 5, 'Farid Karimov', 4, 'STREET', 'Consumer', 'EXTERNAL_REFINANCE', 'NO', '8000 USD', '19-08-2026', 'APPROVED', 'LN-85102-002');
INSERT INTO employee_sales VALUES (15, 5, 'Farid Karimov', 5, 'SALARY', 'Consumer', 'INTERNAL_REFINANCE', 'YES', '9000 AZN', '11-09-2026', 'APPROVED', 'LN-95103-002');
INSERT INTO employee_sales VALUES (16, 6, 'Nigar Mammadova', 4, 'STREET', 'Consumer', 'INTERNAL_REFINANCE', 'YES', '20000 AZN', '14-08-2026', 'APPROVED', 'LN-85201-002');
INSERT INTO employee_sales VALUES (17, 6, 'Nigar Mammadova', 5, 'SALARY', 'Auto', 'NEW', 'YES', '14000 AZN', '04-09-2026', 'APPROVED', 'LN-95202-002');
INSERT INTO employee_sales VALUES (18, 6, 'Nigar Mammadova', 5, 'STREET', 'Consumer', 'NEW', 'NO', '7000 AZN', '19-09-2026', 'APPROVED', 'LN-95203-002');
INSERT INTO employee_sales VALUES (19, 7, 'Orkhan Aliyev', 4, 'SALARY', 'Mortgage', 'NEW', 'NO', '30000 AZN', '18-08-2026', 'APPROVED', 'LN-85301-002');
INSERT INTO employee_sales VALUES (20, 7, 'Orkhan Aliyev', 5, 'STREET', 'Business', 'NEW', 'YES', '18000 USD', '15-09-2026', 'PENDING', 'LN-95302-002');
INSERT INTO employee_sales VALUES (21, 7, 'Orkhan Aliyev', 5, 'SALARY', 'Consumer', 'EXTERNAL_REFINANCE', 'YES', '12000 AZN', '24-09-2026', 'APPROVED', 'LN-95303-002');
INSERT INTO employee_sales VALUES (22, 8, 'Gunel Hasanova', 4, 'STREET', 'Consumer', 'NEW', 'YES', '4000 AZN', '28-08-2026', 'APPROVED', 'LN-85401-002');
INSERT INTO employee_sales VALUES (23, 8, 'Gunel Hasanova', 5, 'SALARY', 'Consumer', 'EXTERNAL_REFINANCE', 'YES', '11000 AZN', '07-09-2026', 'APPROVED', 'LN-95402-002');
INSERT INTO employee_sales VALUES (24, 8, 'Gunel Hasanova', 5, 'STREET', 'Auto', 'NEW', 'NO', '18500 EUR', '27-09-2026', 'APPROVED', 'LN-95403-002');
INSERT INTO employee_sales VALUES (25, 9, 'Kamran Valiyev', 10, 'SALARY', 'Business', 'EXTERNAL_REFINANCE', 'YES', '35000 AZN', '04-08-2026', 'APPROVED', 'LN-86101-003');
INSERT INTO employee_sales VALUES (26, 9, 'Kamran Valiyev', 10, 'STREET', 'Consumer', 'NEW', 'NO', '8000 AZN', '16-08-2026', 'APPROVED', 'LN-86102-003');
INSERT INTO employee_sales VALUES (27, 9, 'Kamran Valiyev', 13, 'SALARY', 'Consumer', 'NEW', 'YES', '9000 AZN', '09-09-2026', 'APPROVED', 'LN-96103-003');
INSERT INTO employee_sales VALUES (28, 10, 'Turan Ismayilov', 10, 'STREET', 'Consumer', 'NEW', 'YES', '7000 AZN', '11-08-2026', 'APPROVED', 'LN-86201-003');
INSERT INTO employee_sales VALUES (29, 10, 'Turan Ismayilov', 13, 'SALARY', 'Auto', 'INTERNAL_REFINANCE', 'YES', '16000 EUR', '16-09-2026', 'APPROVED', 'LN-96202-003');
INSERT INTO employee_sales VALUES (30, 10, 'Turan Ismayilov', 13, 'STREET', 'Business', 'NEW', 'NO', '21000 USD', '28-09-2026', 'APPROVED', 'LN-96203-003');
INSERT INTO employee_sales VALUES (31, 11, 'Zaur Rzayev', 10, 'SALARY', 'Mortgage', 'NEW', 'NO', '50000 AZN', '21-08-2026', 'APPROVED', 'LN-86301-003');
INSERT INTO employee_sales VALUES (32, 11, 'Zaur Rzayev', 13, 'STREET', 'Business', 'EXTERNAL_REFINANCE', 'YES', '22000 USD', '23-09-2026', 'APPROVED', 'LN-96302-003');
INSERT INTO employee_sales VALUES (33, 11, 'Zaur Rzayev', 13, 'SALARY', 'Consumer', 'NEW', 'YES', '11000 AZN', '30-09-2026', 'APPROVED', 'LN-96303-003');
INSERT INTO employee_sales VALUES (34, 12, 'Mehriban Aliyeva', 10, 'STREET', 'Consumer', 'NEW', 'YES', '6000 AZN', '26-08-2026', 'APPROVED', 'LN-86401-003');
INSERT INTO employee_sales VALUES (35, 12, 'Mehriban Aliyeva', 13, 'SALARY', 'Consumer', 'NEW', 'YES', '13000 AZN', '14-09-2026', 'APPROVED', 'LN-96402-003');
INSERT INTO employee_sales VALUES (36, 12, 'Mehriban Aliyeva', 13, 'STREET', 'Auto', 'EXTERNAL_REFINANCE', 'NO', '15000 USD', '29-09-2026', 'APPROVED', 'LN-96403-003');

COMMIT;



select * from employee_sales

/*
TASK 1 
Filial adlarını saxlamaq üçün branch_name adlı yeni sütun əlavə edin. 
Filial adlarını aşağıdakı kimi təyin edin: 001 - Nasimi Branch, 002 - Nizami Branch, 003 - Narimanov Branch
Kredit məbləğlərinin AZN ekvivalentini saxlamaq üçün amount_azn adlı yeni sütun əlavə edin. 
Məbləğləri valyuta növünə uyğun olaraq aşağıdakı məzənnələrlə AZN-ə çevirin: AZN – 1.00, USD – 1.70, EUR – 1.85. */


-- 1. branch_name sütununun əlavə edilməsi və filial adlarının təyini

ALTER TABLE employee_sales ADD (branch_name VARCHAR2(50));

UPDATE employee_sales
SET branch_name = CASE SUBSTR(loan_reference, -3)
    WHEN '001' THEN 'Nasimi Branch'
    WHEN '002' THEN 'Nizami Branch'
    WHEN '003' THEN 'Narimanov Branch'
END;


-- 2. amount_azn sütununun əlavə edilməsi və valyuta konversiyası
ALTER TABLE employee_sales ADD (amount_azn NUMBER(10, 2));

UPDATE employee_sales
SET amount_azn = TO_NUMBER(SUBSTR(credit_amount, 1, INSTR(credit_amount, ' ') - 1)) * 
    CASE 
        WHEN credit_amount LIKE '%USD%' THEN 1.70
        WHEN credit_amount LIKE '%EUR%' THEN 1.85
        ELSE 1.00
    END;

COMMIT;

select * from employee_sales

/*
TASK 2
Təsdiqlənmiş kredit satışları əsasında hər bir əməkdaşın aylar üzrə ümumi icra balını 
və aylıq hədəfinin neçə faizini yerinə yetirdiyini göstərən icra faizini (%) hesablayın.

Qeyd:
Hər bir kredit üzrə baza balı kreditin AZN ilə məbləğinin 10 000-ə bölünməsi ilə hesablanır. Baza bala aşağıdakı əmsallar tətbiq edilir:

Müştəri növü əmsalı: Bankımızdan əməkhaqqı alan müştərilər üçün 1.0, digər banklardan əməkhaqqı alan və ya bankımızdan əməkhaqqı almayan müştərilər üçün 1.2.
Kredit növü əmsalı: Biznes və ipoteka kreditləri üçün 1.3, avtomobil kreditləri üçün 1.1, istehlak kreditləri üçün 1.0.
Kredit kateqoriyası əmsalı: Digər bankdan kreditin refinanslaşdırılması üçün 1.4, yeni kreditlər üçün 1.0, bankımız daxilində mövcud kreditin refinanslaşdırılması üçün 0.8.
Kredit məbləğinin təqdim edilmə forması əmsalı: Kredit məbləği müştərinin kartına köçürüldükdə 1.1, məbləğ kassadan nağd verildikdə 1.0.

Yekun bal baza balının bütün müvafiq əmsallara vurulması ilə hesablanır. Hesablamalarda yalnız təsdiqlənmiş kreditlər nəzərə alınır.*/
SELECT 
    TO_CHAR(TO_DATE(sale_date, 'DD.MM.YYYY'), 'MON') AS sale_month,
    employee_id,
    employee_name,
    branch_name,
    monthly_target_points,
    
    -- 1. İcra balının (ümumi qazanılan balın) hesablanması
    ROUND(SUM(
        (amount_azn / 10000)
        * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
        * CASE 
            WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3
            WHEN credit_type = 'Auto' THEN 1.1
            ELSE 1.0 
          END
        * CASE 
            WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4
            WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8
            ELSE 1.0 
          END
        * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
    ), 2) AS total_earned_points,
    
    -- 2. İcra faizinin (%) hesablanması
    ROUND((
        SUM(
            (amount_azn / 10000)
            * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
            * CASE 
                WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3
                WHEN credit_type = 'Auto' THEN 1.1
                ELSE 1.0 
              END
            * CASE 
                WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4
                WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8
                ELSE 1.0 
              END
            * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
        ) / monthly_target_points
    ) * 100, 2) AS execution_percentage

FROM employee_sales
WHERE status = 'APPROVED' 
GROUP BY 
    TO_CHAR(TO_DATE(sale_date, 'DD.MM.YYYY'), 'MON'),
    employee_id,
    employee_name,
    branch_name,
    monthly_target_points;


/* TASK 3

Əməkdaşların aylıq icra faizinə əsasən bonus məbləğini hesablayın.
Bonus məbləği 800 AZN-dən çox olan əməkdaşları müəyyən edin və nəticələri bonus məbləğinə görə artan sıra ilə sıralayın.

Qeyd:

Əməkdaşlara kredit satışları üzrə verilə biləcək maksimal bonus məbləği 1000 AZN-dir.
İcra faizi 30%-dən aşağı olduqda bonus verilmir və bonus məbləği 0 AZN hesablanır.
İcra faizi 30%–100% aralığında olduqda bonus məbləği icra faizinə uyğun olaraq hesablanır.
İcra faizi 100%-dən yuxarı olduqda əməkdaşa yalnız maksimal bonus məbləği — 1000 AZN verilir.
Əməkdaşın aylıq icra faizi Task 2-də göstərilən qaydalara əsasən hesablanır.*/

SELECT 
    TO_CHAR(TO_DATE(sale_date, 'DD.MM.YYYY'), 'MON') AS sale_month,
    employee_id,
    employee_name,
    branch_name,
    monthly_target_points,
    
    -- 1. İcra balının hesablanması
    ROUND(SUM(
        (amount_azn / 10000)
        * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
        * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END
        * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END
        * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
    ), 2) AS total_earned_points,
    
    -- 2. İcra faizinin (%) hesablanması
    ROUND((
        SUM(
            (amount_azn / 10000)
            * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
            * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END
            * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END
            * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
        ) / monthly_target_points
    ) * 100, 2) AS execution_percentage,

    -- 3. Bonus məbləğinin hesablanması
    ROUND(
        CASE 
            WHEN (
                SUM(
                    (amount_azn / 10000)
                    * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
                    * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END
                    * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END
                    * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
                ) / monthly_target_points
            ) * 100 < 30 THEN 0
            
            WHEN (
                SUM(
                    (amount_azn / 10000)
                    * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
                    * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END
                    * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END
                    * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
                ) / monthly_target_points
            ) * 100 > 100 THEN 1000
            
            ELSE (
                (
                    SUM(
                        (amount_azn / 10000)
                        * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END
                        * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END
                        * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END
                        * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END
                    ) / monthly_target_points
                ) * 100
            ) * 10 
            END
        , 2) AS bonus_amount

FROM employee_sales
WHERE status = 'APPROVED' 
GROUP BY 
    TO_CHAR(TO_DATE(sale_date, 'DD.MM.YYYY'), 'MON'),
    employee_id,
    employee_name,
    branch_name,
    monthly_target_points

-- Qruplaşdırmadan sonra yekun bonus məbləğinə görə filtr (HAVING istifadəsi)
HAVING ROUND(
    CASE 
        WHEN (SUM((amount_azn / 10000) * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END) / monthly_target_points) * 100 < 30 THEN 0
        WHEN (SUM((amount_azn / 10000) * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END) / monthly_target_points) * 100 > 100 THEN 1000
        ELSE ((SUM((amount_azn / 10000) * CASE WHEN client_type = 'STREET' THEN 1.2 ELSE 1.0 END * CASE WHEN credit_type IN ('Business', 'Mortgage') THEN 1.3 WHEN credit_type = 'Auto' THEN 1.1 ELSE 1.0 END * CASE WHEN loan_category = 'EXTERNAL_REFINANCE' THEN 1.4 WHEN loan_category = 'INTERNAL_REFINANCE' THEN 0.8 ELSE 1.0 END * CASE WHEN is_card_transfer = 'YES' THEN 1.1 ELSE 1.0 END) / monthly_target_points) * 100) * 10 end
    , 2) >= 800

ORDER BY 
    bonus_amount ASC;



 
