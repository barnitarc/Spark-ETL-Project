from lib.logging_conf import *
from pyspark.sql import *
from pyspark.sql.functions import *

def transform_doctor(doctor,doctor_hospital,doctor_specialty,specialty_names):
    logger.info("Inside Doctor Transformation function")
    doctor=doctor.join(doctor_hospital.filter("is_active"),doctor_hospital.doctor_id==doctor.doctor_id,"left")\
                    .withColumn("No of hospitals",count("hospital_id").over(Window.partitionBy(doctor_hospital.doctor_id)))\
                    .drop(doctor_hospital.doctor_id).drop(doctor_hospital.is_active)\
                    .drop("hospital_id").distinct()
    logger.info("Added number of hospital doctor is working in Column successfully")
    doctor.show(5)
    specialty_names=specialty_names.withColumn("rank",coalesce(specialty_names.Priority,lit(0))*100+(100-specialty_names.specialty_id))
    specialty=doctor_specialty.join(broadcast(specialty_names),doctor_specialty.specialty_id==specialty_names.specialty_id,"inner")\
                                .withColumn("rownum",row_number()\
                                            .over(Window.partitionBy("doctor_id")\
                                                .orderBy(col("rank").desc())))\
                                .filter(col("rownum")<3)\
                                .withColumnRenamed("doctor_id","spc_doc_id")\
                                .drop(doctor_specialty.specialty_id,
                                      specialty_names.specialty_id,
                                      specialty_names.Priority,
                                      specialty_names.rank)
    logger.info("Transforming Specialty and calculation of Specialty rank successful")
    doctor=doctor.join(broadcast(specialty.filter(col("rownum")==1)),specialty.spc_doc_id==doctor.doctor_id,"left")\
                    .withColumnRenamed("specialty_name","specialty 1")\
                    .drop(specialty.spc_doc_id,specialty.rownum)
    logger.info("Specilaty 1 Column added successfully")
    doctor.show(10)
    doctor=doctor.join(broadcast(specialty.filter(col("rownum")==2)),specialty.spc_doc_id==doctor.doctor_id,"left")\
                    .withColumnRenamed("specialty_name","specialty 2")\
                    .drop(specialty.spc_doc_id,specialty.rownum)
    logger.info("Specialty 2 Column added Successfully")
    #doctor=doctor.drop("doctor_id")
    doctor.show(10)
    logger.info("Doctor Transformation done- Returning to main function")
    return doctor

def transform_employee(employee,employee_hospital):
    logger.info("Inside Employee Transformation function")
    emp1=employee.withColumnRenamed("employee_id","emp_id").withColumnRenamed("manager_id","mng_id")
    employee=employee.alias('a').join(emp1.alias('b'),employee.manager_id==emp1.emp_id,"left")\
                                .select(col("a.employee_id"),
                                        col("a.employee_name"),
                                        col("a.contact_number"),
                                        col("a.email"),
                                        col("b.employee_name").alias("Manager_name"),
                                        col("b.contact_number").alias("Manager_contact_number"),
                                        col("b.email").alias("Manager_email"))
    logger.info("Added Manager details in Dataframe successfully")
    employee=employee.join(employee_hospital.filter("is_active"),employee_hospital.employee_id==employee.employee_id,"left")\
                    .withColumn("No of hospitals working in",count("hospital_id").over(Window.partitionBy(employee_hospital.employee_id)))\
                    .drop(employee_hospital.employee_id).drop(employee_hospital.is_active)\
                    .drop("hospital_id").distinct()
    logger.info("Added number of hospital employee is working in column successfully")
    employee.show(5)
    logger.info("Employee transformation done successfully- returning to main function")
    return employee

def transform_hospital(hospital,employee_hospital,doctor_hospital,employee):
    logger.info("Inside Hospital transformation function")
    hospital=hospital.join(doctor_hospital.filter("is_active"),doctor_hospital.hospital_id==hospital.hospital_id,"left")\
                    .withColumn("No of Doctors",count("doctor_id").over(Window.partitionBy(doctor_hospital.hospital_id)))\
                    .drop(doctor_hospital.hospital_id).drop(doctor_hospital.is_active)\
                    .drop("doctor_id").distinct()
    logger.info("Added number of doctors working in the hospital column successfully")
    emp=hospital.join(employee_hospital.filter("is_active"),hospital.hospital_id==employee_hospital.hospital_id,"inner")\
                .join(broadcast(employee),employee_hospital.employee_id==employee.employee_id,"inner")\
                .withColumn("rownum",row_number().over(Window.partitionBy(hospital.hospital_id).orderBy(employee.employee_id)))\
                .select(hospital.hospital_id,
                        employee.employee_name,
                        col("rownum"))\
                .withColumnRenamed("hospital_id","hos_id")
    
    
    hospital=hospital.join(broadcast(emp.filter(col("rownum")==1)),emp.hos_id==hospital.hospital_id,"left")\
                        .withColumnRenamed("employee_name","Employee 1")\
                        .drop(emp.hos_id,"rownum")\
                    .join(broadcast(emp.filter(col("rownum")==2)),emp.hos_id==hospital.hospital_id,"left")\
                        .withColumnRenamed("employee_name","Employee 2")\
                        .drop(emp.hos_id,"rownum")\
                    .join(broadcast(emp.filter(col("rownum")==3)),emp.hos_id==hospital.hospital_id,"left")\
                        .withColumnRenamed("employee_name","Employee 3")\
                        .drop(emp.hos_id,"rownum")\
                    .join(broadcast(emp.filter(col("rownum")==4)),emp.hos_id==hospital.hospital_id,"left")\
                        .withColumnRenamed("employee_name","Employee 4")\
                        .drop(emp.hos_id,"rownum")
    logger.info("Added 4 Employee name who are working in that hospital column successfully")
    hospital.show(5)
    logger.info("Hospital transformation done successfully- returning to main function")
    return hospital
