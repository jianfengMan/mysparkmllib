package com.zjf.java.util.mysql;

import com.zjf.java.util.constant.ConfConstants;

import java.sql.*;
import java.util.*;


/**
 * Description:jdbc工具类
 * Author: zhangjianfeng
 */
public class JDBCUtils {

    private static Connection connection;
    private static PreparedStatement preparedStatement;
    private static ResultSet resultSet;

    static {
        try {
            Class.forName(ConfConstants.JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        try {
            Class.forName(ConfConstants.JDBC_DRIVER);
            connection = DriverManager.getConnection(ConfConstants.JDBC_URL, ConfConstants.JDBC_USER, ConfConstants.JDBC_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static String generateSql(Map<String, Object> insertColumnValueMap, String tableName) {
        String insertSql = " insert into " + tableName + " set ";
        final List<Object> valueList = new ArrayList<Object>();
        if (!insertColumnValueMap.isEmpty()) {
            Iterator<Map.Entry<String, Object>> columnValueIterator = insertColumnValueMap.entrySet().iterator();
            while (columnValueIterator.hasNext()) {
                Map.Entry<String, Object> columnValueEntry = columnValueIterator.next();
                insertSql += columnValueEntry.getKey() + " = ? ,";
                valueList.add(columnValueEntry.getValue());
            }
        }
        final String finalInsertSql = insertSql.substring(0, insertSql.length() - 1);
        return finalInsertSql;
    }

    /**
     * 执行增删改SQL语句
     *
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public static int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        PreparedStatement pstmt = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            pstmt = connection.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rtn = pstmt.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAll();
        }

        return rtn;
    }


    /**
     * 封装jdbc查询方法
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> executeQuery(String sql, Object[] params) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {

            int index = 1;
            connection = getConnection();
            preparedStatement = connection.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(index++, params[i]);
                }
            }
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData setMetaData = resultSet.getMetaData();
            // 获取列的数量
            int col_len = setMetaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 0; i < col_len; i++) {
                    String col_name = setMetaData.getColumnName(i + 1);
                    Object col_value = resultSet.getObject(col_name);
                    if (col_value == null) {
                        col_value = "";
                    }
                    map.put(col_name, col_value);
                }
                list.add(map);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAll();
        }
        return list;
    }


    /**
     * 批量执行SQL语句
     *
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的行数
     */
    public static int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            // 第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            if (paramsList != null && paramsList.size() > 0) {
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            // 最后一步：使用Connection对象，提交批量的SQL语句
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAll();
        }

        return rtn;
    }

    /**
     * close所有的jdbc操作
     */
    private static void closeAll() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
