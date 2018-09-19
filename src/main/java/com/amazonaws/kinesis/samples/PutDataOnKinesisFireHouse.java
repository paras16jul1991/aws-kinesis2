package com.amazonaws.kinesis.samples;

import java.nio.ByteBuffer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

public class PutDataOnKinesisFireHouse {
	static AWSCredentials credentials = null;

	public static void main(String args[]) {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIA2TRSLU5GIUZ6PIEY",
				"aPCVOWpimpGS9ZcJcE3GdCztcXXLhYvRhvdegE21");
		AmazonKinesisFirehose defaultClient = AmazonKinesisFirehoseClientBuilder.standard()
				.withRegion(Regions.US_EAST_1).withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
		int count = 0;
		while (count++ < 20) {
			String myData = "{\"ticker_symbol\":\"QXZ\", \"sector\":\"PARAS\", \"change\":-0.05, \"price\":" + 12
					+ count + "}";
			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setDeliveryStreamName("test");

			String data = myData + "\n";

			Record record = new Record().withData(ByteBuffer.wrap(data.getBytes()));
			putRecordRequest.setRecord(record);
			PutRecordResult putRecord = defaultClient.putRecord(putRecordRequest);
			System.out.print(putRecord.getRecordId());
		}

	}
}
