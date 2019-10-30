package com.service.phoenix.service;

import com.service.phoenix.entity.Contact;
import com.service.phoenix.entity.ContactList;
import com.service.phoenix.entity.MessageResponse;
import com.service.phoenix.util.DB;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component("DataSourceService")
public class DataSourceService {
    private Logger logger = LoggerFactory.getLogger(DataSourceService.class);

    @Value("${contactTable}")
    private String contactTable;

    @Value("${jobTable}")
    private String jobTable;

    @Value("${queryLimit}")
    private int queryLimit;


    MessageResponse queryContactListByUserId(String userId, int limit, int offset, String uploadTime) {
        long startTime = System.nanoTime();
        MessageResponse response = new MessageResponse();
        ContactList contactList = new ContactList();
        List<Contact> contacts = new ArrayList<>();
        List<String> phones = new ArrayList<>();
        String sql;
        String queryId;
        String message;
        Connection connection = DB.getInstance().getConnection();
        try {
            if (connection == null) {
                message = "QueryContactListByUserId: connection is null";
                logger.error(message);
                response.setCode(10001);
                response.setMessage(message);
                return response;
            }
            if (userId == null || userId.trim().length() <= 0) {
                message = "QueryContactListByUserId: no userId";
                logger.error(message);
                response.setCode(10002);
                response.setMessage(message);
                return response;
            } else {
                String rowKeyPrefix = reverseString(userId);
                limit = limit > -1 && limit <= queryLimit ? limit : queryLimit;
                queryId = rowKeyPrefix + "|%";
                if (uploadTime != null && !uploadTime.equals("")) {
                    sql = "SELECT * FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"upload_time\" > ? LIMIT ? OFFSET ?";
                } else {
                    sql = "SELECT * FROM " + contactTable + " WHERE \"user_id\" LIKE ? LIMIT ? OFFSET ?";
                }
            }
            logger.info("QueryContactListByUserId: sql=" + sql);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            if (!uploadTime.equals("")) {
                preparedStatement.setString(1, queryId);
                preparedStatement.setString(2, uploadTime);
                preparedStatement.setInt(3, limit);
                preparedStatement.setInt(4, offset);
            } else {
                preparedStatement.setString(1, queryId);
                preparedStatement.setInt(2, limit);
                preparedStatement.setInt(3, offset);
            }

            ResultSet resultSet = preparedStatement.executeQuery();

            Contact contact;
            while (resultSet.next()) {
                String phone = resultSet.getString("phone");
                if (phones.contains(phone)) {
                    continue;
                }
                phones.add(phone);
                contact = new Contact();
                contact.setPhone(phone);
                contact.setName(resultSet.getString("name"));
                if (resultSet.getString("available") != null && !resultSet.getString("available").equals("")) {
                    contact.setAvailable(resultSet.getString("available"));
                } else {
                    contact.setAvailable("1");
                }

                try {
                    if (resultSet.getString("source") != null && !resultSet.getString("source").equals("")) {
                        contact.setSource(resultSet.getString("source"));
                    } else {
                        contact.setSource("1");
                    }
                } catch (ColumnNotFoundException e) {
                    contact.setSource("1");
                }

                if (resultSet.getString("phone_uid") != null && !resultSet.getString("phone_uid").equals("") && !resultSet.getString("phone_uid").equals("0")) {
                    contact.setPhoneUid(resultSet.getString("phone_uid"));
                }

                contacts.add(contact);
            }

            contactList.setContacts(contacts);

            String totalSql;
            if (!uploadTime.equals("")) {
                totalSql = "SELECT COUNT(DISTINCT(\"phone\")) AS \"total\" FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"upload_time\" > ?";
            } else {
                totalSql = "SELECT COUNT(DISTINCT(\"phone\")) AS \"total\" FROM " + contactTable + " WHERE \"user_id\" LIKE ?";
            }
            preparedStatement = connection.prepareStatement(totalSql);
            if (!uploadTime.equals("")) {
                preparedStatement.setString(1, queryId);
                preparedStatement.setString(2, uploadTime);
            } else {
                preparedStatement.setString(1, queryId);
            }

            resultSet = preparedStatement.executeQuery();
            int total = 0;
            while (resultSet.next()) {
                total = resultSet.getInt("total");
            }

            contactList.setTotal(total);

            resultSet.close();
            preparedStatement.close();
            connection.close();

            logger.info("QueryContactListByUserId: size=" + contacts.size());
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("QueryContactListByUserId: error while query ", e);
        }
        response.setCode(0);
        response.setMessage("");
        response.setData(contactList);
        logger.info("QueryContactListByUserId: userId=" + userId + " query time=" + (System.nanoTime() - startTime) / 1000000);

        return response;
    }

    MessageResponse updateRegisterStatus(String phone, String phoneUid, String registerTime) {
        long startTime = System.nanoTime();
        MessageResponse response = new MessageResponse();
        String sql;
        String message;
        Connection connection = DB.getInstance().getConnection();
        try {
            if (connection == null) {
                message = "UpdateRegisterStatus: connection is null";
                logger.error(message);
                response.setCode(10001);
                response.setMessage(message);
                return response;
            }
            if (phone == null || phone.trim().length() <= 0) {
                message = "UpdateRegisterStatus: no phone";
                logger.error(message);
                response.setCode(10002);
                response.setMessage(message);
                return response;
            } else {
                sql = "SELECT \"user_id\" FROM " + contactTable + " WHERE \"phone\" = ?";
            }
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, phone);

            ResultSet resultSet = preparedStatement.executeQuery();

            List<String> rowKeyList = new ArrayList<>();

            while (resultSet.next()) {
                rowKeyList.add(resultSet.getString("user_id"));
            }

            resultSet.close();
            preparedStatement.close();

            logger.info(String.format("UpdateRegisterStatus: rowKeyListSize=%d, time=%d", rowKeyList.size(), (System.nanoTime() - startTime) / 1000000));

            long upsertStartTime = System.nanoTime();

            connection.setAutoCommit(false);
            int batchSize = 0;
            int commitSize = 500;
            int size = rowKeyList.size();
            String upsert = "UPSERT INTO " + contactTable + "(\"user_id\", \"register_time\", \"phone_uid\") VALUES(?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(upsert)) {
                while (batchSize < size) {
                    String rowKey = rowKeyList.get(batchSize);
                    statement.setString(1, rowKey);
                    statement.setString(2, registerTime);
                    statement.setString(3, phoneUid);
                    statement.executeUpdate();
                    batchSize++;
                    if (batchSize % commitSize == 0) {
                        logger.info("Commit, batchSize: " + batchSize);
                        connection.commit();
                    }
                }
                if (size % commitSize != 0) {
                    logger.info("Commit, batchSize: " + batchSize);
                    connection.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            long estimatedTime = System.nanoTime() - upsertStartTime;
            logger.info("UpdateRegisterStatus time=" + estimatedTime / 1000000);

            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("UpdateRegisterStatus: error while query ", e);
        }

        response.setCode(0);
        response.setMessage("");

        return response;
    }

    MessageResponse getRegisterUser(String userId, String registerTime, int limit, int offset) {
        long startTime = System.nanoTime();
        MessageResponse response = new MessageResponse();
        Map<String, Object> contactMap = new HashMap<>();
        List<Contact> contacts = new ArrayList<>();
        String sql;
        String message;
        Connection connection = DB.getInstance().getConnection();
        try {
            if (connection == null) {
                message = "GetRegisterUser: connection is null";
                logger.error(message);
                response.setCode(10001);
                response.setMessage(message);
                return response;
            }
            if (userId == null || userId.trim().length() <= 0) {
                message = "GetRegisterUser: no userId";
                logger.error(message);
                response.setCode(10002);
                response.setMessage(message);
                return response;
            } else {
                if (!registerTime.equals("")) {
                    sql = "SELECT * FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"register_time\" > ? AND \"available\" = '1' LIMIT ? OFFSET ?";
                } else {
                    sql = "SELECT * FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"register_time\" is not null AND \"available\" = '1' LIMIT ? OFFSET ?";
                }
            }

            limit = limit > -1 && limit < queryLimit ? limit : queryLimit;
            String rowKeyPrefix = reverseString(userId);
            String queryId = rowKeyPrefix + "|%";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            if (!registerTime.equals("")) {
                preparedStatement.setString(1, queryId);
                preparedStatement.setString(2, registerTime);
                preparedStatement.setInt(3, limit);
                preparedStatement.setInt(4, offset);
            } else {
                preparedStatement.setString(1, queryId);
                preparedStatement.setInt(2, limit);
                preparedStatement.setInt(3, offset);
            }

            ResultSet resultSet = preparedStatement.executeQuery();
            Contact contact;

            while (resultSet.next()) {
                contact = new Contact();
                contact.setPhone(resultSet.getString("phone"));
                contact.setPhoneUid(resultSet.getString("phone_uid"));
                contact.setRegisterTime(resultSet.getString("register_time"));
                contacts.add(contact);
            }

            contactMap.put("list", contacts);

            String totalSql;

            if (!registerTime.equals("")) {
                totalSql = "SELECT COUNT(*) AS total FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"register_time\" > ? AND \"available\" = '1'";
            } else {
                totalSql = "SELECT COUNT(*) AS total FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"register_time\" is not null AND \"available\" = '1'";
            }

            preparedStatement = connection.prepareStatement(totalSql);
            if (!registerTime.equals("")) {
                preparedStatement.setString(1, queryId);
                preparedStatement.setString(2, registerTime);
            } else {
                preparedStatement.setString(1, queryId);
            }

            resultSet = preparedStatement.executeQuery();
            int total = 0;
            while (resultSet.next()) {
                total = resultSet.getInt("total");
            }

            contactMap.put("total", total);

            resultSet.close();
            preparedStatement.close();
            connection.close();

            logger.info("GetRegisterUser: size=" + contacts.size());

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("GetRegisterUser: error while query ", e);
        }

        response.setCode(0);
        response.setMessage("");
        response.setData(contactMap);
        logger.info("GetRegisterUser: userId=" + userId + " query time=" + (System.nanoTime() - startTime) / 1000000);

        return response;
    }

    MessageResponse getFirstDegreeRegister(String userId, int limit, int offset) {
        long startTime = System.nanoTime();
        MessageResponse response = new MessageResponse();
        Map<String, Object> contactMap = new HashMap<>();
        List<String> contacts = new ArrayList<>();
        String sql;
        String message;
        Connection connection = DB.getInstance().getConnection();
        try {
            if (connection == null) {
                message = "GetFirstDegreeRegister: connection is null";
                logger.error(message);
                response.setCode(10001);
                response.setMessage(message);
                return response;
            }
            if (userId == null || userId.trim().length() <= 0) {
                message = "GetFirstDegreeRegister: no userId";
                logger.error(message);
                response.setCode(10002);
                response.setMessage(message);
                return response;
            } else {
                sql = "SELECT * FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"register_time\" is not null AND \"available\" = '1' LIMIT ? OFFSET ?";
            }

            limit = limit > -1 && limit < queryLimit ? limit : queryLimit;
            String rowKeyPrefix = reverseString(userId);
            String queryId = rowKeyPrefix + "|%";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, queryId);
            preparedStatement.setInt(2, limit);
            preparedStatement.setInt(3, offset);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                contacts.add(resultSet.getString("phone"));
            }

            contactMap.put("list", contacts);

            String totalSql;

            totalSql = "SELECT COUNT(*) AS total FROM " + contactTable + " WHERE \"user_id\" LIKE ? AND \"register_time\" is not null AND \"available\" = '1'";

            preparedStatement = connection.prepareStatement(totalSql);
            preparedStatement.setString(1, queryId);

            resultSet = preparedStatement.executeQuery();
            int total = 0;
            while (resultSet.next()) {
                total = resultSet.getInt("total");
            }

            contactMap.put("total", total);

            resultSet.close();
            preparedStatement.close();
            connection.close();

            logger.info("GetFirstDegreeRegister: size=" + contacts.size());

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("GetFirstDegreeRegister: error while query ", e);
        }

        response.setCode(0);
        response.setMessage("");
        response.setData(contactMap);
        logger.info("GetRegisterUser: userId=" + userId + " query time=" + (System.nanoTime() - startTime) / 1000000);

        return response;
    }

    MessageResponse getCompanyJobList(List<String> eidList) {
        long startTime = System.nanoTime();
        MessageResponse response = new MessageResponse();
        String message;
        List<String> jobIdList = new ArrayList<>();
        Connection connection = DB.getInstance().getConnection();
        try {
            if (connection == null) {
                message = "GetCompanyJobList: connection is null";
                logger.error(message);
                response.setCode(10001);
                response.setMessage(message);
                return response;
            }
            if (eidList == null || eidList.size() <= 0) {
                message = "GetCompanyJobList: no eid";
                logger.error(message);
                response.setCode(10002);
                response.setMessage(message);
                return response;
            }

            StringBuilder query = new StringBuilder("SELECT \"job_id\" FROM \"" + jobTable + "\" WHERE \"cid\" IN (");

            for (int i = 0; i < eidList.size(); i++) {
                query.append("?,");
            }
            query.delete(query.length()-1, query.length());
            query.append(")");

            logger.info("GetCompanyJobList, sql=" + query.toString());

            PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
            for (int i = 0; i < eidList.size(); i++) {
                preparedStatement.setString(i+1, eidList.get(i));
            }

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                jobIdList.add(resultSet.getString("job_id"));
            }

            long estimatedTime = (System.nanoTime() - startTime) / 1000000;
            logger.info(String.format("GetCompanyJobList time=%s, jobIdList size=%d", estimatedTime, jobIdList.size()));
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("GetCompanyJobList: error while query ", e);
        }

        response.setCode(0);
        response.setMessage("");
        response.setData(jobIdList);

        return response;
    }

    MessageResponse getSimilarCompany(List<String> eidList) {
        long startTime = System.nanoTime();
        MessageResponse response = new MessageResponse();
        String message;
        List<String> resultList = new ArrayList<>();
        Connection connection = DB.getInstance().getConnection();
        try {
            if (connection == null) {
                message = "GetSimilarCompany: connection is null";
                logger.error(message);
                response.setCode(10001);
                response.setMessage(message);
                return response;
            }

            connection.setAutoCommit(false);
            int batchSize = 0;
            int commitSize = 500;
            int size = eidList.size();
            String query = "select CID_SCORE from \"company_info\".\"similar_company\" where CID_SCORE like ?";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                while (batchSize < size) {
                    String cid = eidList.get(batchSize);
                    String queryId = cid + "_%";
                    statement.setString(1, queryId);
                    ResultSet resultSet = statement.executeQuery();
                    while (resultSet.next()) {
                        resultList.add(resultSet.getString("CID_SCORE"));
                    }

                    batchSize++;
                    if (batchSize % commitSize == 0) {
                        logger.info("Commit, batchSize: " + batchSize);
                        connection.commit();
                    }
                }
                if (size % commitSize != 0) {
                    logger.info("Commit, batchSize: " + batchSize);
                    connection.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            long estimatedTime = System.nanoTime() - startTime;
            logger.info("GetSimilarCompany time=" + estimatedTime / 1000000);

            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("GetSimilarCompany: error while query ", e);
        }

        response.setCode(0);
        response.setMessage("");
        response.setData(resultList);

        return response;
    }

    private String reverseString(String string) {
        return new StringBuilder(string).reverse().toString();
    }
}
