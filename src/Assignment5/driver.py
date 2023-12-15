from pyspark.sql import SparkSession
from PysparkRepo/src/Assignment5/util import *

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

    # Define schemas for DataFrames dynamically
    employee_schema = define_employee_schema()
    department_schema = define_department_schema()
    country_schema = define_country_schema()

    # Create DataFrames using the defined schemas
    employee_df, department_df, country_df = create_dataframes(spark, employee_schema, department_schema, country_schema)

    # Show the DataFrames
    show_dataframes(employee_df, department_df, country_df)

    # Q2: Find the average salary of each department
    avg_salary_department = calculate_avg_salary(employee_df)
    avg_salary_department.show()

    # Q3: Find the employee name and department name whose name starts with 'm'
    employees_start_with_m = find_employees_start_with_m(employee_df)
    employees_start_with_m.show()

    # Q4: Create another new column 'bonus' by multiplying employee salary * 2
    employee_df = add_bonus_column(employee_df)
    employee_df.show()

    # Q5: Reorder the column names of employee_df
    employee_df = reorder_columns(employee_df)
    employee_df.show()

    # Q6: Perform Inner, Left, and Right joins dynamically
    inner_join, left_join, right_join = perform_joins(employee_df, department_df)
    inner_join.show()
    left_join.show()
    right_join.show()

  # Q7: Deriving a new DataFrame with 'country_name' instead of 'State' in employee_df
    new_employee_df = derive_new_dataframe(employee_df, country_df)
    new_employee_df.show()

    # Q8: Converting all column names into lowercase and add a 'load_date' column with the current date
    final_df = convert_columns_and_add_load_date(new_employee_df)
    final_df.show()

    # Q9: Creating the external tables with Parquet and CSV formats
    create_external_tables(final_df)
