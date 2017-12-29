package com.alibaba.datax.plugin.reader.mongodbreader;
//11.27

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
            public void init() {
            this.originalConfig = super.getPluginJobConf();
                this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
                String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,database);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Long batchSize = null;
        /**
         * 用来控制每个task取值的offset
         */
        private Long skipCount = null;
        /**
         * 每页数据的大小
         */
        private int pageSize = 10000;

        private void log(String str){
            System.out.println(System.currentTimeMillis()/1000+"--"+Thread.currentThread().getName()+":::"+str);
        }
        @Override
        public void startRead(RecordSender recordSender) {
            log("--------start---------");
            if(batchSize == null ||
                             mongoClient == null || database == null ||
                             collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);
            BsonDocument sort = new BsonDocument();
            sort.append(KeyConstant.MONGO_PRIMIARY_ID_META, new BsonInt32(1));
            log("----mongoclient--1:host="+mongoClient.getAddress().getHost()+":port="+mongoClient.getAddress().getPort());
            log("----mongoclient--2:"+mongoClient.getAllAddress());
            //long pageCount = batchSize / pageSize;
            //int modCount = (int)(batchSize % pageSize);
            String maxId = null;
            String minId = null;
            Date maxDate = null;
            Date minDate = null;
            int num = 0;//每次条数
            int count = 0;//总条数
            String queryString[] = null;
            while(true) {


                log("----before:num = " + num + " count = " + count);
                num = 0;
                log("-----oldquery = " + query+",skipCount="+skipCount+",pageSize="+pageSize+",batchSize="+batchSize);
                MongoCursor<Document> dbCursor = null;
                if(!Strings.isNullOrEmpty(query)) {

                    if(count >= 2*batchSize) {
                        log("---------------get data error!!!!!!!!!!!----------------");
                        break;
                    }
                    //query的格式为 增量字段(Date)=2017-1-26T16:00:00Z=2017-1-27T16:00:00Z.获取2017年1月27号的数据
                    queryString = query.split("=");
                    sort = BsonDocument.parse("{"+queryString[0]+":1}");
                    String q = "{"+queryString[0]+":{$gte:ISODate('"+queryString[1]+"'),$lte:ISODate('"+queryString[2]+"')}}";
                    //20171206 add
                    if(minDate == null){
                        q = "{" + queryString[0] + ":{$gt:ISODate('" + queryString[1] + "')," +
                                "$lte:ISODate('" + queryString[2] +"')}}";
                    }
                    else{
                        q = "{" + queryString[0] + ":{$gte:ISODate('" + DateFormatUtils.format(DateUtils.addHours(minDate,-8), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + 'Z'+"')," +
                                "$lte:ISODate('" + queryString[2] +"')}}";
                    }
                    log("****************************minDate*************************"+minDate);

                    if(!Strings.isNullOrEmpty(q)) {

                        dbCursor = col.find(BsonDocument.parse(q)).sort(sort).skip(0).limit(pageSize).iterator();
                    }
                }
                else {
                    if(count >= batchSize) {
                        break;
                    }

                    if(Strings.isNullOrEmpty(maxId)) {
                        //取最小值
                        if(skipCount.intValue() == 0){
                            minId = "";
                        }else {
                            dbCursor = col.find().sort(sort).skip(skipCount.intValue() - 1).limit(1).iterator();
                            while (dbCursor.hasNext()) {
                                Document item = dbCursor.next();
                                try {
                                    minId = item.getString("_id").trim();
                                }catch (Exception e){
                                    minId = item.getObjectId("_id").toString().trim(); //org.bson.types.ObjectId cannot be cast to java.lang.String异常处理
                                }

                            }
                        }
                        dbCursor = col.find().sort(sort).skip(skipCount.intValue()+batchSize.intValue()-1).limit(1).iterator();
                        while (dbCursor.hasNext()) {
                            Document item = dbCursor.next();
                            try {
                                maxId = item.getString("_id").trim();
                            }catch (Exception e) {
                                maxId = item.getObjectId("_id").toString().trim();
                            }
                        }
                    }
                    String q = null;
                    if(!Strings.isNullOrEmpty(minId)) {//&&!Strings.isNullOrEmpty(maxId)
                        if(!Strings.isNullOrEmpty(maxId)) {
                            q = "{_id:{$lte:'" + maxId + "',$gt:'" + minId + "'}}";
                        }else {
                            q = "{_id:{$gt:'" + minId + "'}}";
                        }

                    }else{
                        //第一页
                        q = "{_id:{$lte:'" + maxId + "'}}";
//                        log("-----minId and maxId is null .maxId="+maxId+",minId="+minId);
//                        break;
                    }
                    if(!Strings.isNullOrEmpty(q)) {
                        dbCursor = col.find(BsonDocument.parse(q)).sort(sort).skip(0).limit(pageSize).iterator();
//                            .skip(skipCount.intValue()).limit(pageSize).iterator();
                    }
                    log("-----q = " + q);

                }
                while (dbCursor.hasNext()) {
                    Document item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    Iterator columnItera = mongodbColumnMeta.iterator();

                    while (columnItera.hasNext()) {
                        JSONObject column = (JSONObject)columnItera.next();
                        Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                        if (tempCol == null || tempCol == "" || tempCol.equals("") || tempCol.equals(null)) {
                            record.addColumn(new StringColumn("NULL"));
                            continue;
                        }
                        if (tempCol instanceof Double) {
                            record.addColumn(new DoubleColumn((Double) tempCol));
                        } else if (tempCol instanceof Boolean) {
                            record.addColumn(new BoolColumn((Boolean) tempCol));
                        } else if (tempCol instanceof Date) {
                            record.addColumn(new DateColumn((Date) tempCol));
                        } else if (tempCol instanceof Integer) {
                            record.addColumn(new LongColumn((Integer) tempCol));
                        }else if (tempCol instanceof Long) {
                            record.addColumn(new LongColumn((Long) tempCol));
                        } else {
                            if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                                String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                                if(Strings.isNullOrEmpty(splitter)) {
                                    throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                            MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                                } else {
                                    ArrayList array = (ArrayList)tempCol;
                                    String tempArrayStr = Joiner.on(splitter).join(array);
                                    record.addColumn(new StringColumn(tempArrayStr.trim().replaceAll("\n","。").replaceAll("\r","。")));
                                }
                            } else {
                                record.addColumn(new StringColumn(tempCol.toString().trim().replaceAll("\n","。").replaceAll("\r","。")));
                            }
                        }
                    }
                    recordSender.sendToWriter(record);
                    try {
                        minId = item.getString("_id").trim();
                    }catch (Exception e) {
                        minId = item.getObjectId("_id").toString().trim();
                    }
                    if(queryString!=null) {
                        minDate = item.getDate(queryString[0]);
                    }
                    num++;//每条加一

                }
                count+=num;
                skipCount += pageSize;
                if(num<pageSize){
                    break;
                }
            }
            log("--------end---------");
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
                this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
                this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,database);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }
            
            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.batchSize = readerSliceConfig.getLong(KeyConstant.BATCH_SIZE);
            this.skipCount = readerSliceConfig.getLong(KeyConstant.SKIP_COUNT);
        }

        @Override
        public void destroy() {

        }
    }
}
