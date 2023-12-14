from PysparkRepo/src/Assignment5/util import *

spark = SparkSession.builder.appName("Assignment").getOrCreate()

# Create DataFrames
employee_df = create_employee_df(spark) 
department_df = create_department_df(spark)
country_df = create_country_df(spark)

# 2. Find avg salary of each department
avg_salary_result = avg_salary_per_department(employee_df)
avg_salary_result.show()

# 3. Find employee name and department name whose name starts with 'm'
m_employee_result = employee_name_department_starts_with_m(employee_df)
m_employee_result.show()

# 4. Create another new column as bonus
bonus_df = add_bonus_column(employee_df)
bonus_df.show()

# 5. Reorder the column names of employee_df
reordered_df = reorder_columns(employee_df)
reordered_df.show()

# 6. Inner Join
inner_join_result = join_dataframes(employee_df, department_df, "inner")
inner_join_result.show()

# 6. Left Join
left_join_result = join_dataframes(employee_df, department_df, "left")
left_join_result.show()

# 6. Right Join
right_join_result = join_dataframes(employee_df, department_df, "right")
right_join_result.show()

# 7. Replace state with country name
country_name_df = replace_state_with_country_name(employee_df, country_df)
country_name_df.show()

# 8. Convert column names to lower case
lower_case_df = convert_column_names_to_lower_case(country_name_df)
lower_case_df.show()

# 9. Add load_date column
result_with_load_date = add_load_date_column(lower_case_df)
result_with_load_date.show()

