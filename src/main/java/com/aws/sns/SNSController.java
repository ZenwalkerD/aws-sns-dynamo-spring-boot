package com.aws.sns;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.message.DefaultSnsMessageHandler;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;
import com.amazonaws.services.sns.message.SnsSubscriptionConfirmation;
import com.amazonaws.services.sns.message.SnsUnsubscribeConfirmation;

@RestController
public class SNSController {
	private static final Logger logger = LoggerFactory.getLogger(SNSController.class);

	@Autowired
	SnsMessageManager manager;

	@Value("${tableName}")
	String tableName;

	@Autowired
	AmazonDynamoDB client;

	@PostMapping("/endpoint-subscriber")
	public void handler(HttpServletRequest body) throws IOException {

		manager.handleMessage(body.getInputStream(), new DefaultSnsMessageHandler() {

			@Override
			public void handle(SnsNotification message) {
				// logger.info("Message recieved ");

				logger.info("Message Subject - {}", message.getSubject());
				// logger.info("Message - {}",message.getMessage());
				String messageJson = message.getMessage();
				JSONObject obj = new JSONObject(messageJson);
				obj = obj.getJSONArray("Records").getJSONObject(0);
				logger.info("Event source from {},", obj.getString("eventSource"));
				JSONObject o = obj.getJSONObject("s3");
				Iterator<String> map = obj.getJSONObject("s3").keys();

				AtomicReference<String> bucketName = new AtomicReference<String>();
				AtomicReference<String> file = new AtomicReference<String>();

				while (map.hasNext()) {
					String str = map.next();

					// logger.info("str and obj value {}", str);

					if (str.equalsIgnoreCase("bucket")) {
						JSONObject i = o.getJSONObject(str);
						bucketName.set(i.getString("name"));
						// logger.info("Bucket name {}", bucketName);
					} else if (str.equalsIgnoreCase("object")) {
						// logger.info("Object section");
						JSONObject i = o.getJSONObject(str);
						file.set(i.getString("key"));
						// logger.info("Uploaded file {}", file);
					}
				}

				try {
					fetchS3File(bucketName, file);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			private void fetchS3File(AtomicReference<String> bucketName, AtomicReference<String> file)
					throws IOException {
				AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
				S3Object s3Obj = s3Client.getObject(new GetObjectRequest(bucketName.get(), file.get()));
				// logger.info("S3 object fetched {}", s3Obj.getBucketName() );

				BufferedReader reader = new BufferedReader(new InputStreamReader(s3Obj.getObjectContent()));
				StringBuilder builder = new StringBuilder();
				String temp;
				while ((temp = reader.readLine()) != null) {
					builder.append(temp);
				}

				logger.info("***** S3 file content read {}", builder.toString());

				insertIntoTable(builder);
			}

			private void insertIntoTable(StringBuilder builder) {
				JSONObject obj = new JSONObject(builder.toString());
				Map<String, AttributeValue> attributeValues = new HashMap<String, AttributeValue>();
				attributeValues.put("customerid", new AttributeValue().withS(obj.getString("Customer-ID")));
				attributeValues.put("name", new AttributeValue().withS(obj.getString("name")));
				attributeValues.put("date", new AttributeValue().withS(obj.getString("date")));
				attributeValues.put("cost", new AttributeValue().withN(obj.get("cost").toString()));
				attributeValues.put("currency", new AttributeValue().withS(obj.getString("currency")));
				attributeValues.put("companyName", new AttributeValue().withS(obj.getString("From")));

				PutItemRequest req = new PutItemRequest().withTableName(tableName).withItem(attributeValues);

				PutItemResult result = client.putItem(req);

				logger.info("Inserted into table..{}", result.getSdkResponseMetadata());
			}

			@Override
			public void handle(SnsUnsubscribeConfirmation message) {

				logger.info("Received unsubscribe confirmation.");
				logger.info("Token {}", message.getToken());
			}

			@Override
			public void handle(SnsSubscriptionConfirmation message) {
				super.handle(message);
				logger.info("Received subscription confirmation.");
				logger.info("URL {}", message.getSubscribeUrl());
			}
		});
	}
}
