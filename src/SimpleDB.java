import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.google.gson.Gson;


public class SimpleDB {
    static AmazonSimpleDB sdb;
    static AmazonS3 s3;
    static String bucket = "text-test-" + UUID.randomUUID();
    static AWSCredentials credentials = null;

    public static void init(PropertiesCredentials p) throws Exception {

        try {
            credentials = p;
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (/Users/xiaojingyou/.aws/credentials), and is in valid format.",
                    e);
        }

        sdb = new AmazonSimpleDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sdb.setRegion(usWest2);

        s3 = new AmazonS3Client(credentials);
        s3.setRegion(usWest2);
    }

    private static void createBucket(String name) throws Exception {
        System.out.println("Creating bucket " + name + "\n");
        s3.createBucket(name);
    }

    private static void deleteAllBucket() throws Exception {
        System.out.println("Listing buckets");
        // cannot delete if bucket contains objects
        for (Bucket bucket : s3.listBuckets()) {
            deleteInOneday(bucket.getName());
            System.out.println(" expiration in one day " + bucket.getName());
        }
    }

    private static void deleteInOneday(String bucketName) throws Exception {
        // archive to glacier
        Transition transToArchive = new Transition()
                .withDays(1)
                .withStorageClass(StorageClass.Glacier);

        BucketLifecycleConfiguration.Rule ruleArchiveAndExpire = new BucketLifecycleConfiguration.Rule()
                .withId("Delete in 1 day rule")
                .withTransition(transToArchive)
                .withExpirationInDays(2)
                .withStatus(BucketLifecycleConfiguration.ENABLED.toString());

        List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
        rules.add(ruleArchiveAndExpire);

        BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
                .withRules(rules);

        s3.setBucketLifecycleConfiguration(bucketName, configuration);
    }

    private static void putObject(String bucketName, String key, File text) throws Exception {
        s3.putObject(new PutObjectRequest(bucketName, key, text));
    }

    private static String downloadObject(String bucketName, String key) throws Exception{
        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        return textInputStream(object.getObjectContent());
    }

    private static String textInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder sb = new StringBuilder();

        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            sb.append(line);
        }
        return sb.toString();
    }

    private static void createDomain(String domain) throws Exception {
        System.out.println("create domain "+domain);
        sdb.createDomain(new CreateDomainRequest(domain));
    }

    private static void deleteDomain(String domain) throws Exception {
        System.out.println("delete domain "+domain);
        sdb.deleteDomain(new DeleteDomainRequest(domain));
    }

    private static void deleteBucket(String name) throws Exception {
        // cannot delete if contains object
        s3.deleteBucket(name);
    }

    private static void listDomains() throws Exception {
        try {
            ListDomainsRequest sdbRequest = new ListDomainsRequest().withMaxNumberOfDomains(10);
            ListDomainsResult sdbResult = sdb.listDomains(sdbRequest);

            int totalItems = 0;
            for (String domainName : sdbResult.getDomainNames()) {
                DomainMetadataRequest metadataRequest = new DomainMetadataRequest().withDomainName(domainName);
                DomainMetadataResult domainMetadata = sdb.domainMetadata(metadataRequest);
                totalItems += domainMetadata.getItemCount();
                System.out.println(domainName + " item count is : " + domainMetadata.getItemCount());
            }

            System.out.println("You have " + sdbResult.getDomainNames().size() + " Amazon SimpleDB domain(s)" +
                    "containing a total of " + totalItems + " items.");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    static HashMap<String, String> map = new HashMap<String, String>();

    private static void createMap() {
        map.put("Jan", "01");
        map.put("Feb", "02");
        map.put("Mar", "03");
        map.put("Apr", "04");
        map.put("May", "05");
        map.put("Jun", "06");
        map.put("Jul", "07");
        map.put("Aug", "08");
        map.put("Sep", "09");
        map.put("Oct", "10");
        map.put("Nov", "11");
        map.put("Dec", "12");
    }

    private static String convertTime(String date) {
        String processed = null;

        if(map.size()==0){
            createMap();
        }

        // hard coded according to tweet format
        String[] s = date.split(" ");
        String year = s[5];
        String month = s[1];
        String day = s[2];
        String time = s[3];
        processed = year+"-"+map.get(month)+"-"+day+" "+time;

        Timestamp timestamp = Timestamp.valueOf(processed);
        return String.valueOf(timestamp.getTime());
    }

    private static void insert(String file, String table) throws Exception {
        System.out.println("insert into domain "+table);
        BufferedReader br = new BufferedReader(new FileReader(file));

        try {
            String line = br.readLine();
            int count = 0;
            List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();
            while (line != null) {
                count++;
                if(count>=25){
                    count = 0;
                    sdb.batchPutAttributes(new BatchPutAttributesRequest(table, sampleData));
                    sampleData = new ArrayList<ReplaceableItem>();
                }
                Gson gson = new Gson();
                System.out.println("                 "+line);
                Tweet tweet = gson.fromJson(line, Tweet.class);

                Tweet.Coordinate coor = tweet.coordinates;
                String coors = coor.toString();
                String[] ss = coors.split(",");
                String c1 = ss[0].substring(1);
                String c2 = ss[1].substring(1, ss[1].length()-1);
                String timestamp = convertTime(tweet.created_at);

                ReplaceableItem item = new ReplaceableItem().withName(tweet.id_str).withAttributes(
                        new ReplaceableAttribute().withName("created_at").withValue(timestamp),
                        new ReplaceableAttribute().withName("text").withValue(tweet.text),
                        new ReplaceableAttribute().withName("coor1").withValue(c1),
                        new ReplaceableAttribute().withName("coor2").withValue(c2));
                sampleData.add(item);
//            	File f = File.createTempFile(tweet.id_str, ".txt");
//            	f.deleteOnExit();
//            	Writer writer = new OutputStreamWriter(new FileOutputStream(f));
//            	writer.write(tweet.text);
//            	writer.close();
//            	putObject(bucket, tweet.id_str, f);

                line = br.readLine();
            }
            sdb.batchPutAttributes(new BatchPutAttributesRequest(table, sampleData));

        } finally {
            br.close();
        }
    }

    private static void select(String table) throws Exception {
        String selectExpression = "select * from `" +table + "`";

        SelectRequest selectRequest = new SelectRequest(selectExpression).withConsistentRead(true);
        com.amazonaws.services.simpledb.model.SelectResult result = sdb.select(selectRequest);

        int count = 0;
        List<Item> items = result.getItems();
        String next = sdb.select(selectRequest).getNextToken();
        System.out.println(next);

        while (true) {
            count += items.size();
            for (Item item : items ) {
                System.out.println("  Item");
                System.out.println("    Name: " + item.getName());

                List<Attribute> attributes = item.getAttributes();
                for (Attribute attribute : attributes) {
                    System.out.println("      Attribute");
                    System.out.println("        Name:  " + attribute.getName());
                    System.out.println("        Value: " + attribute.getValue());
                }
            }

            if (next!=null) {
                selectRequest.setNextToken(next);
                result = sdb.select(selectRequest);
                items = result.getItems();
                next = sdb.select(selectRequest).getNextToken();
            } else {
                break;
            }
        }

        System.out.println("total number of items " + count);
    }

    public static List<SelectResult> selectFromTimeRange(String table, String start, String end) throws Exception{
        List<SelectResult> list = new LinkedList<SelectResult>();

        String selectExpression = "select * from `" + table + "` where created_at > '"+start+"' and created_at < '"+end+"'";
        System.out.println(selectExpression);

        SelectRequest selectRequest = new SelectRequest(selectExpression).withConsistentRead(true);
        com.amazonaws.services.simpledb.model.SelectResult result = sdb.select(selectRequest);

        int count = 0;
        List<Item> items = result.getItems();
        String next = sdb.select(selectRequest).getNextToken();

        while (true) {
            count += items.size();

            for (Item item : items ) {
                Gson gson = new Gson();

                SelectResult sr = new SelectResult(item.getName());

//	            System.out.println("  Item");
//	            System.out.println("    Name: " + item.getName());

                List<Attribute> attributes = item.getAttributes();
                for (Attribute attribute : attributes) {
//	                System.out.println("      Attribute");
//	                System.out.println("        Name:  " + attribute.getName());
//	                System.out.println("        Value: " + attribute.getValue());

                    switch (attribute.getName()) {
                        case "text":
                            sr.setText(attribute.getValue());
                            break;
                        case "coor1":
                            sr.setCoor1(attribute.getValue());
                            break;
                        case "coor2":
                            sr.setCoor2(attribute.getValue());
                            break;
                        case "created_at":
                            sr.setTime(attribute.getValue());
                            break;
                    }
                }

                list.add(sr);
            }

            if (next!=null) {
                selectRequest.setNextToken(next);
                result = sdb.select(selectRequest);
                items = result.getItems();
                next = sdb.select(selectRequest).getNextToken();
            } else {
                break;
            }
        }

        System.out.println("total number of items " + count);

        return list;
    }

    private static void insertAll() throws Exception {
        // domain name
        String movie = "movie";
        String party = "party";
        String food = "food";
        String soccer = "soccer";

        List<String> tables = new ArrayList<String>();
        tables.add(movie);
        tables.add(party);
        tables.add(food);
        tables.add(soccer);

        for(String s: tables){
            createDomain(s);
            insert(s+".txt", s);
        }
    }

    private static void deleteAll() throws Exception {
        // hard coded for now
        String movie = "movie";
        String party = "party";
        String food = "food";
        String soccer = "soccer";
        deleteDomain(movie);
        deleteDomain(party);
        deleteDomain(food);
        deleteDomain(soccer);
    }

    public static void main(String[] args) throws Exception {
        //init();

        // show tables with item count
//    	listDomains();

        // insert to four tables
//    	insertAll();

        // example usage
        System.out.println(selectFromTimeRange("movie", "0", "1414347967000").size());

    }
}