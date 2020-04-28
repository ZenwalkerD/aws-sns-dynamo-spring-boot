package com.aws.sns;

import java.awt.Font;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.aws.sns.AsciiArt.Settings;

@SpringBootConfiguration
@EnableAutoConfiguration
@SpringBootApplication
public class AwsSnsSubscriptionApplication implements CommandLineRunner {

	@Value("${hostIp}")
	String host;

	@Value("${topicName}")
	String topic;

	@Value("${height}")
	int height;

	@Value("${tableName}")
	String tableName;

	@Autowired
	AmazonDynamoDB client;

	private static final Logger logger = LoggerFactory.getLogger("Application.class");

	public static void main(String[] args) {
		SpringApplication.run(AwsSnsSubscriptionApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		logger.info("Host {} and topic {}", this.host, this.topic);
		AmazonSNS sns = AmazonSNSClient.builder().build();
		String topicArn = sns.createTopic(this.topic).getTopicArn();
		if (!this.doesSubscriptionExist(sns, topicArn))
			sns.subscribe(new SubscribeRequest().withEndpoint("http://" + this.host + ":8080/endpoint-subscriber")
					.withProtocol("http").withTopicArn(topicArn));

		createTable();
		AsciiArt art = new AsciiArt();
		Font f = new Font("Dialog", Font.ITALIC, 18);
		Settings s = art.new Settings(f, height, 20);
		art.drawString("ZenwalkerD", "@", s);
	}

	private boolean doesSubscriptionExist(AmazonSNS sns, String topicArn) {
		String nextToken;
		do {
			ListSubscriptionsByTopicResult result = sns
					.listSubscriptionsByTopic(new ListSubscriptionsByTopicRequest().withTopicArn(topicArn));
			nextToken = result.getNextToken();
			if (result.getSubscriptions().stream()
					.anyMatch(s -> s.getEndpoint().equals("http://" + this.host + ":8080/endpoint-subscriber"))) {
				return true;
			}
		} while (nextToken != null);
		return false;
	}

	private void createTable() {

		DynamoDB db = new DynamoDB(client);
		Table table = db.getTable(tableName);
		try {
			table.describe();
			logger.info("Table exists..");
		} catch (ResourceNotFoundException e) {

			logger.info("Creating table...");
			db.createTable(tableName,
					Arrays.asList(new KeySchemaElement("customerid", KeyType.HASH),
							new KeySchemaElement("name", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("customerid", ScalarAttributeType.S),
							new AttributeDefinition("name", ScalarAttributeType.S)),
					new ProvisionedThroughput(1L, 1L));

			try {
				table.waitForActive();
				logger.info("Table created..");
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				logger.info("Create table issue.. {}", e1);
			}
		}

	}

	@Bean
	public AmazonDynamoDB dynamoDBClientInstance() {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

		return client;
	}

	@Bean
	public SnsMessageManager getSnsManagerInstance() {
		return new SnsMessageManager();
	}

}
