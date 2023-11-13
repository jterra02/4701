import mysql.connector
from mysql.connector import Error


def openConnection():

    connection = mysql.connector.connect(
        user='root',
        password='password',
        host='localhost',
        database='p1'
        )

    cursor = connection.cursor()

    return connection,cursor


def add_new_employee():
    connection, cursor = openConnection()
    try:
        fname = input("Enter first name: ")
        minit = input("Enter middle initial: ")
        lname = input("Enter last name: ")
        ssn = input("Enter SSN (9 digits): ")
        bdate = input("Enter birth date (YYYY-MM-DD): ")
        address = input("Enter address: ")
        sex = input("Enter sex (M/F): ")
        salary = float(input("Enter salary: "))
        super_ssn = input("Enter supervisor SSN (9 digits): ")
        dno = int(input("Enter department number: "))


        query = """INSERT INTO EMPLOYEE (Fname, Minit, Lname, Ssn, Bdate, Address, 
                                    Sex, Salary, Super_ssn, Dno) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        data = (fname, minit, lname, ssn, bdate, address, sex, salary, super_ssn, dno)

        cursor.execute(query, data)
        connection.commit()

        print("Employee added successfully")

    except Error as err:
        print("Error:", err)

    connection.close()
    return


def view_employee():
    connection,cursor = openConnection()
    ssn = input("Enter employee SSN: ")

    view_employee_query = """SELECT E.*, S.Fname AS Supervisor_Fname, S.Lname AS Supervisor_Lname,
                            D.Dname,
                            GROUP_CONCAT(DP.Dependent_name) AS Dependents
                            FROM EMPLOYEE E
                            LEFT JOIN EMPLOYEE S ON E.Super_ssn = S.Ssn
                            LEFT JOIN DEPARTMENT D ON E.Dno = D.Dnumber
                            LEFT JOIN DEPENDENT DP ON E.Ssn = DP.Essn
                            WHERE E.Ssn = %s
                            GROUP BY E.Ssn"""

    cursor.execute(view_employee_query, (ssn,))
    output = cursor.fetchone()

    if output:
        print("Employee Details:")
        for index, column in enumerate(cursor.column_names):
            print(f"{column}: {output[index]}")
    else:
        print("Employee not found")

    connection.close()
    return

def modify_employee():
    connection,cursor=openConnection()
    try:
        ssn = input("Enter employee SSN: ")

        query = "SELECT * FROM EMPLOYEE WHERE Ssn = %s FOR UPDATE"
        cursor.execute(query, (ssn,))
        result = cursor.fetchone()

        if result:
            print("\nEmployee Details:")
            for i, column in enumerate(cursor.column_names):
                print(f"{column}: {result[i]}")

            address = input("Enter new address, press enter to skip: ").strip() or None
            sex = input("Enter new sex, press enter to skip: ").strip() or None
            salary = input("Enter new salary, press enter to skip: ").strip() or None
            super_ssn = input("Enter new supervisor SSN, press enter to skip: ").strip() or None
            dno = input("Enter new department number, press enter to skip: ").strip() or None

            query = """UPDATE EMPLOYEE
                    SET Address = COALESCE(%s, Address),
                    Sex = COALESCE(%s, Sex),
                    Salary = COALESCE(%s, Salary),
                    Super_ssn = COALESCE(%s, Super_ssn),
                    Dno = COALESCE(%s, Dno)
                    WHERE Ssn = %s"""

            data = (address, sex, salary, super_ssn, dno, ssn)
            cursor.execute(query, data)
            connection.commit()

            print("Employee details updated")

        else:
            print("Employee not found")

    except Error as err:
        print("Error:", err)

    connection.close()
    return


def remove_employee():
    connection,cursor=openConnection()
    try:
        ssn = input("Enter employee SSN: ")

        lock_employee_query = "SELECT * FROM EMPLOYEE WHERE Ssn = %s FOR UPDATE"
        cursor.execute(lock_employee_query, (ssn,))
        result = cursor.fetchone()

        if result:
            print("Employee Details:")
            for index, column in enumerate(cursor.column_names):  # Fix here
                print(f"{column}: {result[index]}")  # Fix here

            query = "SELECT * FROM DEPENDENT WHERE Essn = %s"
            cursor.execute(query, (ssn,))
            dependencies = cursor.fetchall()

            if dependencies:
                print("Dependencies exist. Please remove dependencies first.")
            else:
                confirm = input("Are you sure you want to delete this employee? (yes/no): ")
                if confirm.lower() == "yes":
                    delete_employee_query = "DELETE FROM EMPLOYEE WHERE Ssn = %s"
                    cursor.execute(delete_employee_query, (ssn,))
                    connection.commit()
                    print("Employee deleted successfully")
                else:
                    print("Deletion cancelled")

        else:
            print("Employee not found")

    except Error as e:
        print("Error:", e)

    connection.close()
    return


def add_new_dependent():
    connection,cursor = openConnection()
    try:
        ssn = input("Enter employee SSN: ")

        lock_employee_query = "SELECT * FROM EMPLOYEE WHERE Ssn = %s FOR UPDATE"
        cursor.execute(lock_employee_query, (ssn,))
        result = cursor.fetchone()

        if result:
            print("\nEmployee Details:")
            for i, column in enumerate(cursor.column_names):
                print(f"{column}: {result[i]}")

            dep_name = input("\nEnter dependent name: ")
            dep_sex = input("Enter dependent sex: ")
            dep_bdate = input("Enter dependent birth date (YYYY-MM-DD): ")
            dep_relationship = input("Enter dependent relationship: ")

            insert_dependent_query = """INSERT INTO DEPENDENT (Essn, Dependent_name, Sex, Bdate, Relationship)
                                         VALUES (%s, %s, %s, %s, %s)"""
            dependent_data = (ssn, dep_name, dep_sex, dep_bdate, dep_relationship)
            cursor.execute(insert_dependent_query, dependent_data)
            connection.commit()

            print("Dependent added successfully")

        else:
            print("Employee not found.")

    except Error as e:
        print("Error adding new dependent:", e)

    connection.close()
    return


def remove_dependent():
    connection,cursor=openConnection()
    try:
        ssn = input("Enter employee SSN: ")

        lock_employee_query = "SELECT * FROM EMPLOYEE WHERE Ssn = %s FOR UPDATE"
        cursor.execute(lock_employee_query, (ssn,))
        result = cursor.fetchone()

        if result:
            print("Current Dependents:")
            view_dependents_query = "SELECT * FROM DEPENDENT WHERE Essn = %s"
            cursor.execute(view_dependents_query, (ssn,))
            dependents = cursor.fetchall()

            if dependents:
                for dep in dependents:
                    print(f"{dep[1]} ({dep[4]})")

                dep_name = input("Enter name of dependent to be removed: ")
                delete_dependent_query = "DELETE FROM DEPENDENT WHERE Essn = %s AND Dependent_name = %s"
                cursor.execute(delete_dependent_query, (ssn, dep_name))
                connection.commit()

                print("Dependent removed successfully")

            else:
                print("No dependents found.")

        else:
            print("Employee not found.")

    except Error as e:
        print("Error removing dependent:", e)

    connection.close()
    return

def add_department():
    connection,cursor=openConnection()
    try:
        dname = input("Enter department name: ")
        dnumber = int(input("Enter department number: "))
        mgr_ssn = input("Enter manager SSN: ")
        mgr_start_date = input("Enter manager start date (YYYY-MM-DD): ")

        insert_department_query = """INSERT INTO DEPARTMENT (Dname, Dnumber, Mgr_ssn, Mgr_start_date) VALUES (%s, %s, %s, %s)"""
        cursor.execute(insert_department_query, (dname, dnumber, mgr_ssn, mgr_start_date))
        connection.commit()

        print("Department added successfully")

    except Error as err:
        print("Error adding new department:", err)

    connection.close()
    return

def view_department():
    connection,cursor = openConnection()
    try:
        dnumber = int(input("Enter department number: "))

        view_department_query = """SELECT D.*, E.Fname AS Manager_Fname, E.Lname AS Manager_Lname,
                                    GROUP_CONCAT(DL.Dlocation) AS Department_Locations
                                    FROM DEPARTMENT D
                                    LEFT JOIN EMPLOYEE E ON D.Mgr_ssn = E.Ssn
                                    LEFT JOIN DEPT_LOCATIONS DL ON D.Dnumber = DL.Dnumber
                                    WHERE D.Dnumber = %s
                                    GROUP BY D.Dnumber"""

        cursor.execute(view_department_query, (dnumber,))
        result = cursor.fetchone()

        if result:
            print("Department Details:")
            for index, column in enumerate(cursor.column_names):
                print(f"{column}: {result[index]}")
        else:
            print("Department not found.")

    except Error as err:
        print("Error viewing department details:", err)

    connection.close()
    return

def remove_department():
    connection,cursor=openConnection()
    try:
        dnumber = int(input("Enter department number: "))

        lock_department_query = "SELECT * FROM DEPARTMENT WHERE Dnumber = %s FOR UPDATE"
        cursor.execute(lock_department_query, (dnumber,))
        result = cursor.fetchone()

        if result:
            print("Department Details:")
            for index, column in enumerate(cursor.column_names):
                print(f"{column}: {result[index]}")

            check_dependencies_query = "SELECT * FROM PROJECT WHERE Dnum = %s"
            cursor.execute(check_dependencies_query, (dnumber,))
            dependencies = cursor.fetchall()

            if dependencies:
                print("Dependencies exist. Please remove dependencies first.")
            else:
                confirm = input("Are you sure you want to delete this department? (yes/no): ")
                if confirm.lower() == "yes":
                    delete_department_query = "DELETE FROM DEPARTMENT WHERE Dnumber = %s"
                    cursor.execute(delete_department_query, (dnumber,))
                    connection.commit()
                    print("Department deleted successfully")
                else:
                    print("Deletion cancelled.")

        else:
            print("Department not found.")

    except Error as err:
        print("Error removing department:", err)

    connection.close()
    return


def add_department_location():
    connection,cursor=openConnection()
    try:
        dnumber = int(input("Enter department number: "))

        lock_department_query = "SELECT * FROM DEPARTMENT WHERE Dnumber = %s FOR UPDATE"
        cursor.execute(lock_department_query, (dnumber,))
        result = cursor.fetchone()

        if result:
            print("Current Department Locations:")
            view_locations_query = "SELECT * FROM DEPT_LOCATIONS WHERE Dnumber = %s"
            cursor.execute(view_locations_query, (dnumber,))
            locations = cursor.fetchall()

            if locations:
                for location in locations:
                    print(location[1])

            new_location = input("Enter new location: ")

            insert_location_query = "INSERT INTO DEPT_LOCATIONS (Dnumber, Dlocation) VALUES (%s, %s)"
            cursor.execute(insert_location_query, (dnumber, new_location))
            connection.commit()

            print("Location added successfully")

        else:
            print("Department not found.")

    except Error as err:
        print("Error adding department location:", err)

    connection.close()
    return


def remove_department_location():
    connection,cursor=openConnection()
    try:
        dnumber = int(input("Enter department number: "))

        lock_department_query = "SELECT * FROM DEPARTMENT WHERE Dnumber = %s FOR UPDATE"
        cursor.execute(lock_department_query, (dnumber,))
        result = cursor.fetchone()

        if result:
            print("Current Department Locations:")
            view_locations_query = "SELECT * FROM DEPT_LOCATIONS WHERE Dnumber = %s"
            cursor.execute(view_locations_query, (dnumber,))
            locations = cursor.fetchall()

            if locations:
                for location in locations:
                    print(location[1])

                remove_location = input("Enter location to be removed: ")

                delete_location_query = "DELETE FROM DEPT_LOCATIONS WHERE Dnumber = %s AND Dlocation = %s"
                cursor.execute(delete_location_query, (dnumber, remove_location))
                connection.commit()

                print("Location removed successfully")

            else:
                print("No locations found.")

        else:
            print("Department not found.")

    except Error as err:
        print("Error removing department location:", err)

    connection.close()
    return


def menu():
        print("\nMenu:")
        print("1. Add new employee")
        print("2. View employee")
        print("3. Modify employee")
        print("4. Remove employee")
        print("5. Add new dependent")
        print("6. Remove dependent")
        print("7. Add new department")
        print("8. View department")
        print("9. Remove department")
        print("10. Add department location")
        print("11. Remove department location")
        print("0. Exit")
        print("Type 'Menu' to See Options Again")



def main():

    menu()

    while True:
        choice = input("\nEnter the number corresponding to your choice (type 'Menu' or exit by typing '0'): ")

        if choice == "0":
            break
        elif choice == "menu" or choice == "Menu":
            menu()
        elif choice == "1":
            add_new_employee()
        elif choice == "2":
            view_employee()
        elif choice == "3":
            modify_employee()
        elif choice == "4":
            remove_employee()
        elif choice == "5":
            add_new_dependent()
        elif choice == "6":
            remove_dependent()
        elif choice == "7":
            add_department()
        elif choice == "8":
            view_department()
        elif choice == "9":
            remove_department()
        elif choice == "10":
            add_department_location()
        elif choice == "11":
            remove_department_location()
        else:
            print("Invalid choice. Please enter a valid number")


if __name__ == "__main__":
    main()
