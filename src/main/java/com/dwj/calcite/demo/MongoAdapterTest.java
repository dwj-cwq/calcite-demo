/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dwj.calcite.demo;

import com.mongodb.Mongo;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import org.apache.calcite.adapter.mongodb.MongoSchema;
import org.apache.calcite.adapter.mongodb.MongoSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.NlsString;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.io.InputStream;
import java.net.URL;
import java.sql.*;
import java.util.*;


/**
 * Testing mongo adapter functionality. By default runs with
 * <a href="https://github.com/fakemongo/fongo">Fongo</a> unless {@code IT} maven profile is enabled
 * (via {@code $ mvn -Pit install}).
 */
public class MongoAdapterTest {

    public Statement statement;
    public CalciteConnection calciteConnection;

    public void connect() throws SQLException {

        Properties info = new Properties() {{
            put("model", this.getClass().getClassLoader().getResource("mongo-model.json").getPath());
            put("parserFactory", "org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY");
        }};

        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", info);

        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        statement = calciteConnection.createStatement();

        this.calciteConnection = calciteConnection;
    }

    public static void main(String[] args) throws SQLException {
        MongoAdapterTest mongoAdapterTest = new MongoAdapterTest();
        mongoAdapterTest.connect();

        System.out.println(mongoAdapterTest.calciteConnection.getSchema());
        System.out.println(mongoAdapterTest.calciteConnection.getProperties());
        System.out.println(mongoAdapterTest.calciteConnection.getCatalog());

        ResultSet resultSet = mongoAdapterTest.statement.executeQuery("select count(*) from \"anomaly.kpi\"");
//        ResultSet resultSet = mongoAdapterTest.statement.executeQuery("select count(*) from \"test\".\"anomaly.kpi\"");

        String string = new ResultSetFormatter().resultSet(resultSet).string();
        System.out.println(string);

        ResultSet resultSet1 = mongoAdapterTest.statement.executeQuery("select a.kpiKey, a.name from \"anomaly.kpi\" AS a");
//        ResultSet resultSet1 = mongoAdapterTest.statement.executeQuery("select a._MAP['kpi_key'], a._MAP['name'], a._MAP['server'] from \"test\".\"anomaly.kpi\" AS a");

        String string1 = new ResultSetFormatter().resultSet(resultSet1).string();
        System.out.println(string1);

//        mongoAdapterTest.statement.executeQuery("INSERT INTO \"anomaly.kpi\" VALUES (\"kpiKey\"[\"fsdan\"], \"name\"[\"test1\"])");

        executeMongoDB(mongoAdapterTest);
    }

    public static void executeMongoDB(MongoAdapterTest mongoAdapterTest) throws SQLException {
        CalciteConnection con = mongoAdapterTest.calciteConnection;
        SchemaPlus postSchema = con.getRootSchema().getSubSchema("test");

        FrameworkConfig postConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(postSchema)
                .build();

        final RelBuilder builder = RelBuilder.create(postConfig).scan("anomaly.kpi");

        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);
//        RexNode nameRexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND,
//                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.ANY), 0),
//                rexBuilder.makeCharLiteral(
//                        new NlsString("name", typeFactory.getDefaultCharset().name(),
//                                SqlCollation.COERCIBLE)));
//        RelDataType mapType = typeFactory.createMapType(
//                typeFactory.createSqlType(SqlTypeName.VARCHAR),
//                typeFactory.createTypeWithNullability(
//                        typeFactory.createSqlType(SqlTypeName.ANY), true));
//        List<RexNode> namedList =
//                ImmutableList.of(rexBuilder.makeInputRef(mapType, 0),
//                        nameRexNode);
//
//        // Add fields in builder stack so it is accessible while filter preparation
//        builder.projectNamed(namedList, Arrays.asList("_MAP", "name"), true);
//        builder.projectNamed(namedList, Arrays.asList("_MAP", "name"), true);

        RexNode filterRexNode = builder
                .equals(builder.field("name"),
                        builder.literal("test"));
        builder.filter(filterRexNode);
        RelNode root = builder.build();

        System.out.println(RelOptUtil.toString(root));

        RelRunner ru = null;
        try {
            ru = (RelRunner) con.unwrap(Class.forName("org.apache.calcite.tools.RelRunner"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (PreparedStatement preparedStatement = ru.prepare(root)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            String string = new ResultSetFormatter().resultSet(resultSet).string();
            System.out.println(string);
        }
    }

    /**
     * Converts a {@link ResultSet} to string.
     */
    static class ResultSetFormatter {
        final StringBuilder buf = new StringBuilder();

        public ResultSetFormatter resultSet(ResultSet resultSet)
                throws SQLException {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                rowToString(resultSet, metaData);
                buf.append("\n");
            }
            return this;
        }

        /**
         * Converts one row to a string.
         */
        ResultSetFormatter rowToString(ResultSet resultSet,
                                       ResultSetMetaData metaData) throws SQLException {
            int n = metaData.getColumnCount();
            if (n > 0) {
                for (int i = 1; ; i++) {
                    buf.append(metaData.getColumnLabel(i))
                            .append("=")
                            .append(adjustValue(resultSet.getString(i)));
                    if (i == n) {
                        break;
                    }
                    buf.append("; ");
                }
            }
            return this;
        }

        protected String adjustValue(String string) {
            return string;
        }

        public Collection<String> toStringList(ResultSet resultSet,
                                               Collection<String> list) throws SQLException {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                rowToString(resultSet, metaData);
                list.add(buf.toString());
                buf.setLength(0);
            }
            return list;
        }

        /**
         * Flushes the buffer and returns its previous contents.
         */
        public String string() {
            String s = buf.toString();
            buf.setLength(0);
            return s;
        }
    }

}
