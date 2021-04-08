package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;
import corona.data.PostgreSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static corona.business.ReturnValue.*;


public class Solution {

    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE laboratory \n" +
                    "(\n" +
                    "    labID integer,\n" +
                    "    name text NOT NULL ,\n" +
                    "    city text NOT NULL,\n" +
                    "    active boolean NOT NULL,\n" +
                    "    PRIMARY KEY (labID),\n" +
                    "    CHECK (labID > 0)\n" +
                    ")");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE employee\n" +
                    "(\n" +
                    "    employeeID integer,\n" +
                    "    name text ,\n" +
                    "    city text ,\n" +
                    "    PRIMARY KEY (employeeID),\n" +
                    "    CHECK (employeeID > 0)\n" +
                    ")");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {

            pstmt = connection.prepareStatement("CREATE TABLE vaccine\n" +
                    "(\n" +
                    "    vaccineID integer,\n" +
                    "    name text NOT NULL ,\n" +
                    "    cost integer NOT NULL ,\n" +
                    "    units integer NOT NULL ,\n" +
                    "    productivity integer NOT NULL ,\n" +
                    "    income integer,\n"+
                    "    PRIMARY KEY (vaccineID),\n" +
                    "    CHECK (vaccineID > 0),\n" +
                    "    CHECK (productivity >= 0 AND  productivity <=100 ),\n" +
                    "    CHECK (units >=0)\n"+
                    ")");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        connection = DBConnector.getConnection();
        pstmt = null;
        try{
            pstmt = connection.prepareStatement( "CREATE TABLE working\n" +
                    "(\n" +
                    "    employeeID integer NOT NULL ,\n" +
                    "    labID integer NOT NULL,\n" +
                    "    salary integer NOT NULL,\n" +
                    "    labCity text NOT NULL,\n"+
                    "    employeeCity text NOT NULL,\n"+
                    "    PRIMARY KEY (employeeID, labID),\n" +
                    "    FOREIGN KEY (employeeID) REFERENCES employee(employeeID) ON UPDATE CASCADE ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (labID) REFERENCES laboratory(labID) ON UPDATE CASCADE ON DELETE CASCADE \n" +
                    ")");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        connection = DBConnector.getConnection();
        pstmt = null;
        try{
            pstmt = connection.prepareStatement( "CREATE TABLE produces\n" +
                    "(\n" +
                    "    labID integer,\n" +
                    "    vaccineID integer,\n" +
                    "    PRIMARY KEY (labID, vaccineID),\n" +
                    "    FOREIGN KEY (labID) REFERENCES laboratory(labID) ON UPDATE CASCADE ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (vaccineID) REFERENCES vaccine(vaccineID) ON UPDATE CASCADE ON DELETE CASCADE \n" +
                    ")");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try{
            pstmt = connection.prepareStatement(  "CREATE VIEW productiveLabs AS \n" +
             "SELECT labID  FROM  laboratory L WHERE \n" +
             "(SELECT COUNT(*) AS cnt FROM produces WHERE  labID = L.labID) = \n"+
             "(SELECT COUNT(*) FROM vaccine WHERE productivity>20 AND vaccine.vaccineID IN (SELECT vaccineID FROM produces WHERE labID=L.labID))");
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }



    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM laboratory \n" );
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM employee \n " );
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM vaccine \n " );
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        connection = DBConnector.getConnection();
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM working \n " );
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM produces \n " );
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

    }


    public static void dropTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS laboratory");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS employee");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS vaccine");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS working");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS produces");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        connection = DBConnector.getConnection();
        pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS productiveLabs");
            pstmt.execute();
        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


    }


    public static ReturnValue addLab(Lab lab) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO laboratory" +
                    " VALUES ( ?, ?, ?, ?)");
            pstmt.setInt(1, lab.getId());
            pstmt.setString(2, lab.getName());
            pstmt.setString(3, lab.getCity());
            pstmt.setBoolean(4,lab.getIsActive());
            pstmt.execute();
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue() )
                return BAD_PARAMS;
            else if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ALREADY_EXISTS;
            else return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static Lab getLabProfile(Integer labID) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM laboratory WHERE labID = ? ");
            pstmt.setInt(1, labID);
            ResultSet results = pstmt.executeQuery();
            if(results.next()) {
                Lab lab=new Lab();
                lab.setId(labID);
                lab.setName(results.getString(2));
                lab.setCity(results.getString(3));
                lab.setIsActive(results.getBoolean(4));
                results.close();
                return lab;
            }

            results.close();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return Lab.badLab();
    }

    public static ReturnValue deleteLab(Lab lab) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM laboratory " +
                            "WHERE labID = ?");
            pstmt.setInt(1,lab.getId());

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0) return NOT_EXISTS;
        } catch (SQLException e) {
            //e.printStackTrace()();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
        return OK;
    }

    public static ReturnValue addEmployee(Employee employee) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO employee" +
                    " VALUES ( ?, ?, ?)");
            pstmt.setInt(1, employee.getId());
            pstmt.setString(2, employee.getName());
            pstmt.setString(3, employee.getCity());
            pstmt.execute();
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue() )
                return BAD_PARAMS;
            else if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ALREADY_EXISTS;
            else return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

    }

    public static Employee getEmployeeProfile(Integer employeeID) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM employee WHERE employeeID = ? ");
            pstmt.setInt(1, employeeID);
            ResultSet results = pstmt.executeQuery();
            if(results.next()) {
                Employee emp=new Employee();
                emp.setId(employeeID);
                emp.setName(results.getString(2));
                emp.setCity(results.getString(3));
                results.close();
                return emp;
            }

            results.close();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return Employee.badEmployee();

    }

    public static ReturnValue deleteEmployee(Employee employee) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM employee " +
                            "WHERE employeeID = ?");
            pstmt.setInt(1,employee.getId());

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0) return NOT_EXISTS;
        } catch (SQLException e) {
            //e.printStackTrace()();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
        return OK;

    }

    public static ReturnValue addVaccine(Vaccine vaccine) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO vaccine" +
                    " VALUES ( ?, ?, ?, ?, ?,0)");
            pstmt.setInt(1, vaccine.getId());
            pstmt.setString(2, vaccine.getName());
            pstmt.setInt(3, vaccine.getCost());
            pstmt.setInt(4, vaccine.getUnits());
            pstmt.setInt(5, vaccine.getProductivity());
            pstmt.execute();
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue() )
                return BAD_PARAMS;
            else if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ALREADY_EXISTS;
            else return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM vaccine WHERE vaccineID = ? ");
            pstmt.setInt(1, vaccineID);
            ResultSet results = pstmt.executeQuery();
            if(results.next()) {
                Vaccine vac=new Vaccine();
                vac.setId(vaccineID);
                vac.setName(results.getString(2));
                vac.setCost(results.getInt(3));
                vac.setUnits(results.getInt(4));
                vac.setProductivity(results.getInt(5));
                results.close();
                return vac;
            }

            results.close();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return Vaccine.badVaccine();

    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM vaccine " +
                            "WHERE vaccineID = ?");
            pstmt.setInt(1,vaccine.getId());

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0) return NOT_EXISTS;
        } catch (SQLException e) {
            //e.printStackTrace()();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
        return OK;

    }


    public static ReturnValue employeeJoinLab(Integer employeeID, Integer labID, Integer salary) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        PreparedStatement pstmt2 = null;
        PreparedStatement pstmt3 = null;
        try {
            pstmt2=connection.prepareStatement("SELECT city FROM laboratory WHERE labID=?");
            pstmt2.setInt(1,labID);
            ResultSet resLab=pstmt2.executeQuery();
            pstmt3=connection.prepareStatement("SELECT city FROM employee WHERE employeeID=? ");
            pstmt3.setInt(1,employeeID);
            ResultSet resEmp=pstmt3.executeQuery();
            String lCity,eCity;
            if(resEmp.next() && resLab.next()){
                lCity=resLab.getString(1);
                eCity=resEmp.getString(1);
                resEmp.close();
                resLab.close();
            }
            else{
                resEmp.close();
                resLab.close();
                return NOT_EXISTS;
            }
            pstmt = connection.prepareStatement("INSERT INTO working" +
                    " VALUES ( ?, ?, ?, ? ,?)");
            pstmt.setInt(1, employeeID);
            pstmt.setInt(2, labID);
            pstmt.setInt(3, salary);
            pstmt.setString(4,lCity);
            pstmt.setString(5,eCity);
            pstmt.execute();
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue())
                return BAD_PARAMS;
            else if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ALREADY_EXISTS;
            else return ERROR;
        } finally {
            try {
                pstmt2.close();
                pstmt3.close();
                if(pstmt!=null)
                    pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }

        }
    }
    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM working " +
                            "WHERE employeeID = ? AND  labID = ?");
            pstmt.setInt(1, employeeID);
            pstmt.setInt(2, labID);
            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0) return NOT_EXISTS;
        } catch (SQLException e) {
            //e.printStackTrace()();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
        return OK;
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt=connection.prepareStatement("SELECT * FROM vaccine WHERE vaccineID=?");
            pstmt.setInt(1,vaccineID);
            ResultSet r=pstmt.executeQuery();
            if(!r.next())
                return NOT_EXISTS;
            pstmt=connection.prepareStatement("SELECT * FROM laboratory WHERE labID=?");
            pstmt.setInt(1,labID);
            r=pstmt.executeQuery();
            if(!r.next())
                return NOT_EXISTS;
            pstmt = connection.prepareStatement("INSERT INTO produces" +
                    " VALUES ( ?, ?)");
            pstmt.setInt(1, labID);
            pstmt.setInt(2, vaccineID);
            pstmt.execute();
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue())
                return BAD_PARAMS;
            else if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue())
                return ALREADY_EXISTS;
            else return ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }

        }
    }

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM produces " +
                            "WHERE labID = ? AND  vaccineID = ?");
            pstmt.setInt(1, labID);
            pstmt.setInt(2, vaccineID);
            int affectedRows = pstmt.executeUpdate();
            if(affectedRows==0) return NOT_EXISTS;
        } catch (SQLException e) {
            //e.printStackTrace()();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
        return OK;
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "UPDATE vaccine " +
                            "SET units = units -  ? , cost = cost*2 , "+
                            "productivity=LEAST(productivity +15,100),income=income+cost*?" +
                            "where vaccineID = ?");
            pstmt.setInt(1,amount);
            pstmt.setInt(2,amount);
            pstmt.setInt(3,vaccineID);

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows == 0)
                return NOT_EXISTS;
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return BAD_PARAMS;
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "UPDATE vaccine " +
                            "SET units = units +  ? , cost = cost/2 , productivity=GREATEST(productivity -15,0)" +
                            "WHERE vaccineID = ? AND ? >=0");
            pstmt.setInt(1,amount);
            pstmt.setInt(2,vaccineID);
            pstmt.setInt(3,amount);

            int affectedRows = pstmt.executeUpdate();
            if(affectedRows == 0) {
                pstmt = connection.prepareStatement(
                        "UPDATE vaccine " +
                                "SET units = units +  ? , cost = cost/2 , productivity=GREATEST(productivity -15,0)" +
                                "WHERE vaccineID = ?");
                pstmt.setInt(1,amount);
                pstmt.setInt(2,vaccineID);
                affectedRows = pstmt.executeUpdate();
                if(affectedRows ==0)
                    return NOT_EXISTS;
                else return BAD_PARAMS;
            }
            return OK;
        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState())== PostgreSQLErrorCodes.CHECK_VIOLATION.getValue())
                return BAD_PARAMS;
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return ERROR;
            }
        }
    }

    public static Boolean isLabPopular(Integer labID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM productiveLabs WHERE labID=?");
            pstmt.setInt(1, labID);
            ResultSet r=pstmt.executeQuery();
            if(!r.next())
                return false;
            return true;
        }catch (SQLException e){
            return false;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return false;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return false;
            }
        }
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            pstmt=connection.prepareStatement(
                    "SELECT income FROM vaccine " +
                            "WHERE  vaccineID= ? ");
            pstmt.setInt(1,vaccineID);
            ResultSet res=pstmt.executeQuery();
            if(res.next())
                return res.getInt(1);
            else return 0;
        }catch (SQLException e){
            return 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
        }
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            pstmt=connection.prepareStatement(
                    "SELECT SUM (units) FROM vaccine " +
                            "WHERE  productivity > 20");
            ResultSet res=pstmt.executeQuery();
            if(res.next())
                return res.getInt(1);
            else return 0;
        }catch (SQLException e){
            return 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
        }
    }

    public static Integer getTotalWages(Integer labID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            pstmt=connection.prepareStatement(
                    "SELECT SUM(salary),COUNT(employeeID) FROM working " +
                            "WHERE  labID = ? AND labID IN (select labID FROM laboratory  WHERE labID=? AND active=true)");
            pstmt.setInt(1,labID);
            pstmt.setInt(2,labID);
            ResultSet res=pstmt.executeQuery();
            if(res.next()) {
                if(res.getInt(2)==1)
                    return 0;
                else return res.getInt(1);
            }
            else return 0;
        }catch (SQLException e){
            return 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
        }
    }


    public static Integer getBestLab() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT labID , COUNT(*) AS cnt \n" +
                            "FROM working WHERE labCity=employeeCity \n" +
                            "GROUP BY labID \n" +
                            "ORDER BY cnt DESC ,labID \n" +
                            "LIMIT 1");
            ResultSet res = pstmt.executeQuery();
            if (res.next()) {
                return res.getInt(1);
            } else return 0;
        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return 0;
            }
        }
    }

    public static String getMostPopularCity() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt= null;
        try{
            pstmt=connection.prepareStatement(
                    "SELECT employeeCity,COUNT(*) AS cnt \n"+
                            "FROM working \n"+
                            "GROUP BY employeeCity \n"+
                            "ORDER BY cnt DESC , employeeCity DESC \n"+
                            "LIMIT 1");
            ResultSet res=pstmt.executeQuery();
            if(res.next()) {
                return res.getString(1);
            }
            else return "";
        }catch (SQLException e){
            return null;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return null;
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return null;
            }
        }
    }

    public static ArrayList<Integer> getPopularLabs() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT labID FROM productiveLabs PL \n" +
                    "WHERE EXISTS (SELECT labID FROM produces WHERE labID=PL.labID)"+
                    "ORDER BY labID \n" +
                    "LIMIT 3 \n");
            ResultSet results = pstmt.executeQuery();
            ArrayList<Integer> list= new ArrayList<Integer>();
            while(results.next()){
                list.add(results.getInt(1));
            }
            results.close();
            return list;

        } catch (SQLException e) {
            //e.printStackTrace()();
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return new ArrayList<Integer>();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return new ArrayList<Integer>();
            }
        }

    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT vaccineID FROM vaccine V \n" +
                    "ORDER BY V.productivity + V.units - V.cost DESC , vaccineID \n" +
                    "LIMIT 10 \n");
            ResultSet results = pstmt.executeQuery();
            ArrayList<Integer> list= new ArrayList<Integer>();
            while(results.next()){
                list.add(results.getInt(1));
            }
            results.close();
            return list;

        } catch (SQLException e) {
            //e.printStackTrace()();
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return new ArrayList<Integer>();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return new ArrayList<Integer>();
            }
        }

    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT  * FROM working WHERE employeeID=? ");
            pstmt.setInt(1, employeeID);
            ResultSet results = pstmt.executeQuery();
            if (!results.next()) {
                pstmt = connection.prepareStatement("SELECT  * FROM employee WHERE employeeID !=? AND " +
                        "EXISTS (SELECT employeeID FROM employee WHERE employeeID=?)");
                pstmt.setInt(1, employeeID);
                pstmt.setInt(2, employeeID);
                results = pstmt.executeQuery();
            } else {
                pstmt = connection.prepareStatement("SELECT employeeID ,COUNT(*) \n " +
                        "FROM working \n " +
                        "WHERE employeeID != ? AND labID IN (SELECT labID from working WHERE employeeID=?)\n " +
                        "GROUP BY employeeID \n" +
                        "HAVING COUNT(*)*2 >= (SELECT COUNT(*) from working WHERE employeeID=?) \n" +
                        "ORDER BY employeeID \n" +
                        "LIMIT 10");

                pstmt.setInt(1, employeeID);
                pstmt.setInt(2, employeeID);
                pstmt.setInt(3, employeeID);
                results = pstmt.executeQuery();
            }
            ArrayList<Integer> list= new ArrayList<Integer>();
            while(results.next()){
                list.add(results.getInt(1));
            }
            results.close();
            return list;

        } catch (SQLException e) {
            //e.printStackTrace()();
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return new ArrayList<Integer>();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
                return new ArrayList<Integer>();
            }
        }
    }
}

