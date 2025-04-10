# Databricks notebook source
# MAGIC %sql
create database if not exists silver;

# COMMAND ----------

# MAGIC %sql
create database if not exists bronze;

# COMMAND ----------

# MAGIC %sql
 database if not exists gold;

# COMMAND ----------

# MAGIC %sql
 create database if not exists gold_vw;
