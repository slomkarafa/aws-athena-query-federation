/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_FILE_PROPERTY;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_PARTITION_VALUES_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DeltalakeRecordHandlerTest2 extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandlerTest2.class);

    private DeltalakeRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;
    private S3BlockSpillReader spillReader;

    @Rule
    public TestName testName = new TestName();

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        String tenantCol = "tenant";
        String yearCol = "year";
        String monthCol = "month";
        String dayCol = "day";
        String dataBucket = "test-bucket-1";

        // test_withHEader schema
//        schemaForRead = SchemaBuilder.newBuilder()
//            .addStringField(tenantCol)
//            .addIntField(yearCol)
//            .addIntField(monthCol)
//            .addIntField(dayCol)
//            .addField(
//                FieldBuilder.newBuilder("event", Types.MinorType.STRUCT.getType())
//                    .addField(
//                        FieldBuilder.newBuilder("header", Types.MinorType.STRUCT.getType())
//                            .addIntField("version")
//                        .addStringField("eventID")
//                        .addStringField("eventType")
//                        .addStringField("tenantID")
//                        .addStringField("correlationID")
//                        .addStringField("origin")
//                        .addStringField("audience")
//                        .addStringField("timestamp")
//                        .addStringField("expires")
//                            .addField(FieldBuilder.newBuilder("headers", new ArrowType.Map(true))
//                                .addField("entries", Types.MinorType.STRUCT.getType(), false, Arrays.asList(
//                                    FieldBuilder.newBuilder("key", Types.MinorType.VARCHAR.getType(), false).build(),
//                                    FieldBuilder.newBuilder("value", Types.MinorType.VARCHAR.getType()).build()))
//                            .build())
//                        .build()
//                    )
//                    .addField(
//                        FieldBuilder.newBuilder("payload", Types.MinorType.STRUCT.getType())
//                        .addStringField("nonSensitiveInfo")
//                        .addStringField("sensitiveInfo")
//                            .addListField("sensitiveArray", Types.MinorType.VARCHAR.getType())
//                            .addListField("nonSensitiveArray", Types.MinorType.VARCHAR.getType())
//                        .build()
//                    )
//                    .addField(
//                        FieldBuilder.newBuilder("__metadata", new ArrowType.Map(true))
//                        .addField("entries", Types.MinorType.STRUCT.getType(), false, Arrays.asList(
//                            FieldBuilder.newBuilder("key", Types.MinorType.VARCHAR.getType(), false).build(),
//                            FieldBuilder.newBuilder("value", Types.MinorType.VARCHAR.getType()).build()))
//                        .build())
//
//                .build()
//            )
//            .addField(
//                FieldBuilder.newBuilder("metadata", Types.MinorType.STRUCT.getType())
//                    .addIntField("partition")
//                    .addBigIntField("offset")
//                    .addDateMilliField("timestamp")
//                    .build()
//            )
//            .build();
        schemaForRead = SchemaBuilder.newBuilder()
            .addStringField(tenantCol)
            .addStringField(yearCol)
            .addStringField(monthCol)
            .addStringField(dayCol)
            .addField(
                FieldBuilder.newBuilder("metadata", Types.MinorType.STRUCT.getType())
                    .addStringField("name")
                    .build()
            )
            .addField(
                FieldBuilder.newBuilder("testMap", new ArrowType.Map(true))
                .addField("entries", Types.MinorType.STRUCT.getType(), false, Arrays.asList(
                    FieldBuilder.newBuilder("key", Types.MinorType.VARCHAR.getType(), false).build(),
                    FieldBuilder.newBuilder("value", Types.MinorType.VARCHAR.getType()).build()))
                .build())
            .build();

        allocator = new BlockAllocatorImpl();

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", S3_ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.access.key", "NO_NEED");
        conf.set("fs.s3a.secret.key", "NO_NEED");

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, S3_REGION);
        amazonS3 = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        this.handler = new DeltalakeRecordHandler(
            amazonS3,
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            conf,
            dataBucket);

        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        String catalogName = "catalog";
        for (int i = 0; i < 2; i++) {
            Map<String, ValueSet> constraintsMap = new HashMap<>();
            String queryId = "queryId-" + System.currentTimeMillis();

            ReadRecordsRequest request = new ReadRecordsRequest(fakeIdentity(),
                catalogName,
                queryId,
                new TableName("test-database-2", "nested-table"),
                schemaForRead,
                Split.newBuilder(makeSpillLocation(queryId, "1234"), null)
                    .add(SPLIT_PARTITION_VALUES_PROPERTY, "{\"tenant\":\"fooTenant\",\"year\":\"2022\",\"month\":\"5\",\"day\":\"26\"}")
                    .add(SPLIT_FILE_PROPERTY, "tenant=fooTenant/year=2022/month=5/day=26/part-00193-05d483df-8da5-46a8-ad53-30df52e07bdc.c000.snappy.parquet")
                    .build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
            );

            RecordResponse rawResponse = handler.doReadRecords(allocator, request);
            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

            Block expectedBlock = allocator.createBlock(schemaForRead);
//            BlockUtils.setValue(expectedBlock.getFieldVector("tenant"), 0,  "acme");
//            BlockUtils.setValue(expectedBlock.getFieldVector("year"), 0,  "asia");
//            BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 0,  LocalDate.of(2020, 12, 21));
//            BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 0, 100);
//            BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 0, null);
//
//            expectedBlock.setRowCount(1);
//
//            ReadRecordsResponse expectedResponse = new ReadRecordsResponse(catalogName, expectedBlock);

//            assertEquals(1, response.getRecords().getRowCount());
            System.out.println(BlockUtils.rowToString(response.getRecords(), 0));
            System.out.println(BlockUtils.rowToString(response.getRecords(), 1));
//            System.out.println(BlockUtils.rowToString(response.getRecords(), 2));
//            System.out.println(BlockUtils.rowToString(response.getRecords(), 3));
            assertEquals(4, response.getRecords().getRowCount());
//            assertEquals(expectedResponse, response);
        }
    }
}
