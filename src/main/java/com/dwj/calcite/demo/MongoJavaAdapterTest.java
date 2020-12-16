package com.dwj.calcite.demo;

import org.apache.calcite.adapter.mongodb.MongoSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.bson.Document;

import java.sql.*;
import java.util.*;

/**
 * @author dwj
 * @date 2020/9/25 10:55
 */
public class MongoJavaAdapterTest {

    public static CalciteConnection createConnection() throws SQLException {

        final Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus root = calciteConnection.getRootSchema();

        Map<String, Object> operand = new HashMap<String, Object>(8) {{
            put("host", "127.0.0.1:27017");
            put("database", "aiops");
            put("authMechanism", "SCRAM-SHA-1");
            put("username", "aiops");
            put("password", "aiops");
            put("authDatabase", "admin");
        }};

        root.add("aiops", new MongoSchemaFactory().create(root, "java_test", operand));

        // add calcite view programmatically
        final String viewSql = "select cast(_MAP['kpi_key'] AS varchar(20)) AS \"kpiKey\", cast(_MAP['name'] AS varchar(20)) AS \"name\", _MAP['server'] AS \"server\" from \"aiops\".\"anomaly.kpi\"";

        ViewTableMacro viewMacro = ViewTable.viewMacro(root, viewSql,
                Collections.singletonList("mongo"), Arrays.asList("mongo", "view"), false);
        root.add("kpi", viewMacro);

        return calciteConnection;
    }

    public static void main(String[] args) throws SQLException {
        CalciteConnection calciteConnection =  createConnection();

        try (Statement statement = calciteConnection.createStatement()) {

            System.out.println(calciteConnection.getSchema());
            System.out.println(calciteConnection.getProperties());

//            ResultSet resultSet = statement.executeQuery("select count(*) from \"mongo\".\"anomaly.kpi\"");
//            ResultSet resultSet = statement.executeQuery("select count(*) from \"kpi\"");
//
//            String string = new MongoAdapterTest.ResultSetFormatter().resultSet(resultSet).string();
//            System.out.println(string);

//            ResultSet resultSet1 = statement.executeQuery("select a._MAP['kpi_key'], a._MAP['name'] from \"mongo\".\"anomaly.kpi\" AS a");
//            ResultSet resultSet1 = statement.executeQuery("select a.kpiKey, a.name from \"kpi\" AS a");
//
//            String string1 = new MongoAdapterTest.ResultSetFormatter().resultSet(resultSet1).string();
//            System.out.println(string1);

            ResultSet resultSet2 = statement.executeQuery("select * from \"kpi\" AS a");

            System.out.println(resultSet2.getMetaData().getColumnTypeName(1));
            System.out.println(resultSet2.getMetaData().getColumnTypeName(2));
            System.out.println(resultSet2.getMetaData().getColumnTypeName(3));
            while (resultSet2.next()) {
                System.out.println(resultSet2.getObject(1));
                System.out.println(resultSet2.getObject(2));
                Document object = (Document) resultSet2.getObject(3);
                System.out.println(object.get("id"));
            }
        }

    }

}
